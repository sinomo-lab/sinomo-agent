//! Directories a cross-shard directory move into the converting shard still
//! has to place.
//!
//! A cross-shard directory move adopts its root into the destination shard
//! first, together with the destination reservation that shard keeps until
//! the move finishes or aborts, then adopts the subtree below the root page
//! by page. Every moved directory carries the count its source gave it, so a
//! directory the move has adopted counts child directories that are still
//! live rows of the source, under the move's source root. The source reports
//! each of those with the path the move places it at
//! ([`NamespaceCountMovingDirectoryContribution`]).
//!
//! The converting shard counts such a directory toward the parent at that
//! path when it holds that parent: the move's root or a directory the move
//! has adopted. A directory whose destination parent is not adopted either
//! counts toward its parent in the source, which still holds both. A
//! directory the shard has adopted already, whose source row is not retired
//! yet, counts once.
//!
//! Only a move whose destination reservation the shard keeps has placed
//! anything in it: before its root adoption every row of the move is the
//! source's, and after its release none is left under its source root.
use super::*;
use sinomo_dfs::{DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobRecord};

/// One live directory its source shard owns strictly under the source root
/// of one of its Running cross-shard directory moves, with the path the move
/// places it at.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceCountMovingDirectoryContribution {
    pub file_system_id: DfsFileSystemId,
    /// The move's coordinator job.
    pub job_id: String,
    pub child_object_id: DfsObjectId,
    /// The directory's path in its source shard.
    pub child_path: String,
    /// The path the move places it at.
    pub destination_path: String,
}

/// The coordinator job, source root and target root a Running destination
/// reservation records.
fn reservation_roots(job: &DfsNamespaceJobRecord) -> Option<(&str, &str, &str)> {
    match job.pending_frames.as_slice() {
        [
            DfsNamespaceJobFrame::DirectoryMoveDestinationReservation {
                coordinator_job_id,
                source_root,
                target_root,
            },
        ] if job.kind == DfsNamespaceJobKind::DirectoryMoveDestinationReservation
            && job.status == DfsNamespaceJobStatus::Running
            && job.target_path.is_none()
            && job.root_path == *target_root =>
        {
            Some((coordinator_job_id, source_root, target_root))
        }
        _ => None,
    }
}

/// A Running destination reservation the shard holds for a cross-shard
/// directory move into it, which the shard keeps through its conversion:
/// only that move's finish or abort releases it, and both run after the
/// shard activates.
pub(crate) fn is_kept_destination_reservation(job: &DfsNamespaceJobRecord) -> bool {
    reservation_roots(job).is_some()
}

/// Where a moving directory counts in the shard.
pub(super) struct Placement<'a> {
    /// The parent at the path the move places the directory at.
    pub(super) parent_path: &'a str,
    /// Whether that parent is the move's root, which the shard holds with
    /// the reservation.
    pub(super) move_root: bool,
}

/// The source and target roots of each move whose destination reservation
/// one shard's rows keep, by filesystem and coordinator job.
pub(super) struct KeptMoves {
    roots: BTreeMap<(DfsFileSystemId, String), (String, String)>,
}

impl KeptMoves {
    pub(super) fn new(jobs: &[DfsNamespaceJobRecord]) -> Self {
        let roots = jobs
            .iter()
            .filter_map(|job| {
                let (coordinator, source_root, target_root) = reservation_roots(job)?;
                Some((
                    (job.file_system_id.clone(), coordinator.to_owned()),
                    (source_root.to_owned(), target_root.to_owned()),
                ))
            })
            .collect();
        Self { roots }
    }

    /// Where `moving` counts in the shard; `None` when the shard keeps no
    /// reservation of its move, which then has placed nothing in it. A
    /// directory that does not follow its move's roots fails closed.
    pub(super) fn placement<'a>(
        &self,
        moving: &'a NamespaceCountMovingDirectoryContribution,
    ) -> Result<Option<Placement<'a>>> {
        let Some((source_root, target_root)) = self
            .roots
            .get(&(moving.file_system_id.clone(), moving.job_id.clone()))
        else {
            return Ok(None);
        };
        let follows = canonical_path(&moving.child_path)
            && canonical_path(&moving.destination_path)
            && moving
                .child_path
                .strip_prefix(source_root.as_str())
                .and_then(|suffix| suffix.strip_prefix('/'))
                .zip(
                    moving
                        .destination_path
                        .strip_prefix(target_root.as_str())
                        .and_then(|suffix| suffix.strip_prefix('/')),
                )
                .is_some_and(|(source, destination)| source == destination);
        let parent_path = parent(&moving.destination_path).filter(|_| follows);
        let Some(parent_path) = parent_path else {
            return Err(invalid(
                "moving directory does not follow the roots of its kept directory move",
            ));
        };
        Ok(Some(Placement {
            parent_path,
            move_root: parent_path == target_root,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::tests::{fixture, limits, object, reseal};
    use super::*;

    fn reservation(status: DfsNamespaceJobStatus) -> DfsNamespaceJobRecord {
        DfsNamespaceJobRecord {
            job_id: "namespace_job_dir-move-destination-reservation_counted".into(),
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            origin: None,
            kind: DfsNamespaceJobKind::DirectoryMoveDestinationReservation,
            status,
            root_path: "/services/tree".into(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::DirectoryMoveDestinationReservation {
                coordinator_job_id: "counted-move".into(),
                source_root: "/users/tree".into(),
                target_root: "/services/tree".into(),
            }],
            processed_object_count: 0,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            updated_at: time::OffsetDateTime::UNIX_EPOCH,
            completed_at: None,
            failure_reason: None,
        }
    }

    fn moving(
        name: &str,
        child: &str,
        destination: &str,
    ) -> NamespaceCountMovingDirectoryContribution {
        NamespaceCountMovingDirectoryContribution {
            file_system_id: sinomo_dfs::sifs_file_system_id(),
            job_id: "counted-move".into(),
            child_object_id: DfsObjectId::from_legacy_named(name).unwrap(),
            child_path: child.into(),
            destination_path: destination.into(),
        }
    }

    /// Services holds a moved root that carries one adopted and one moving
    /// child directory, and the reservation of the move.
    fn moved_root(
        reservation: DfsNamespaceJobRecord,
    ) -> (
        sinomo_testdir::TestDir,
        Vec<DfsMetadataShardConfig>,
        NamespaceCountMaintenanceSource,
    ) {
        let (root, catalog, mut sources) = fixture();
        let mut services = sources.remove(1);
        assert_eq!(services.captured.source.physical_shard.0, "services");
        let captured = Arc::make_mut(&mut services.captured);
        let mut state =
            sinomo_dfs::DfsMetadataCandidateState::new(captured.projected_rows.clone()).unwrap();
        let mut moved = object(
            "count-moved-root",
            "/services/tree",
            DfsObjectKind::Directory,
        );
        moved.namespace_link_count = Some(4);
        for object in [
            moved,
            object(
                "count-adopted-child",
                "/services/tree/adopted",
                DfsObjectKind::Directory,
            ),
        ] {
            state
                .apply(sinomo_dfs::DfsPreparedMetadataEffect::PutObject { object })
                .unwrap();
        }
        state
            .apply(sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceJob { job: reservation })
            .unwrap();
        captured.projected_rows = state.snapshot();
        reseal(&mut services);
        (root, catalog, services)
    }

    fn contributions(
        entries: Vec<NamespaceCountMovingDirectoryContribution>,
    ) -> Vec<ShardCountContributions> {
        ["root", "users"]
            .into_iter()
            .map(|source| ShardCountContributions {
                source_shard: DfsMetadataShardId::new(source),
                target_shard: DfsMetadataShardId::new("services"),
                aliases: Vec::new(),
                child_directories: Vec::new(),
                moving_directories: if source == "users" {
                    entries.clone()
                } else {
                    Vec::new()
                },
            })
            .collect()
    }

    fn counts(
        catalog: &[DfsMetadataShardConfig],
        services: &NamespaceCountMaintenanceSource,
        entries: Vec<NamespaceCountMovingDirectoryContribution>,
    ) -> Result<BTreeMap<String, u64>> {
        let plan = plan_shard_namespace_count_maintenance(
            catalog,
            services,
            &contributions(entries),
            limits(),
        )?;
        Ok(plan
            .updates
            .into_iter()
            .map(|update| (update.original.path, update.namespace_link_count))
            .collect())
    }

    /// The moved root counts its adopted child, its moving child once even
    /// when the source still reports the adopted one, and not a moving
    /// grandchild whose parent is not adopted. The kept reservation is no
    /// unsettled work.
    #[test]
    fn a_kept_move_counts_the_directories_it_still_has_to_place() {
        let (_root, catalog, services) = moved_root(reservation(DfsNamespaceJobStatus::Running));
        let placing = vec![
            moving(
                "count-moving-child",
                "/users/tree/moving",
                "/services/tree/moving",
            ),
            moving(
                "count-adopted-child",
                "/users/tree/adopted",
                "/services/tree/adopted",
            ),
            moving(
                "count-moving-grandchild",
                "/users/tree/moving/deep",
                "/services/tree/moving/deep",
            ),
        ];
        let counted = counts(&catalog, &services, placing.clone()).unwrap();
        assert!(
            !counted.contains_key("/services/tree"),
            "the carried count 4 is verified"
        );
        assert_eq!(counted["/services/tree/adopted"], 2);
        assert_eq!(counted["/services"], 4, "its own two directories");
        let mut image = services.captured.projected_rows.clone();
        let plan = plan_shard_namespace_count_maintenance(
            &catalog,
            &services,
            &contributions(placing.clone()),
            limits(),
        )
        .unwrap();
        image.objects = plan.counted_objects(&services, limits()).unwrap();
        validate_shard_image_counts(
            &catalog,
            &DfsMetadataShardId::new("services"),
            &services.captured.source.file_system_ids,
            &image,
            &contributions(placing.clone()),
            limits(),
        )
        .unwrap();

        // Without the moving child the carried count differs.
        let error = counts(&catalog, &services, placing[1..].to_vec())
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("existing initialized count differs"),
            "{error}"
        );
        // A directory that does not follow the move's roots fails closed.
        let astray = vec![moving(
            "count-moving-child",
            "/users/tree/moving",
            "/services/other/moving",
        )];
        let error = counts(&catalog, &services, astray).err().unwrap();
        assert!(
            error.to_string().contains("does not follow the roots"),
            "{error}"
        );
        // A move the shard keeps no reservation of has placed nothing here.
        let mut other = placing[0].clone();
        other.job_id = "another-move".into();
        let error = counts(&catalog, &services, vec![other]).err().unwrap();
        assert!(
            error
                .to_string()
                .contains("existing initialized count differs"),
            "{error}"
        );
    }

    /// Only a Running reservation of the exact marker shape is kept: a
    /// released one is an ordinary Completed row.
    #[test]
    fn only_a_running_reservation_is_kept() {
        assert!(is_kept_destination_reservation(&reservation(
            DfsNamespaceJobStatus::Running
        )));
        let released = reservation(DfsNamespaceJobStatus::Completed);
        assert!(!is_kept_destination_reservation(&released));
        let mut misshapen = reservation(DfsNamespaceJobStatus::Running);
        misshapen.root_path = "/services/elsewhere".into();
        assert!(!is_kept_destination_reservation(&misshapen));
    }
}
