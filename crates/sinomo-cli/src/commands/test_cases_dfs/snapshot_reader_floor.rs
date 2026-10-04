//! Bounded physical reader-floor evidence without opening metadata tables.

use super::*;
use sinomo_runtime::dfs::DfsMetadataSnapshotReaderFloorEvidence;
use sinomo_runtime::storage::migration::{MigrationJob, MigrationPhase};

const MAX_OBSERVED_METADATA_SHARDS: usize = 128;

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_metadata_snapshot_reader_floor(ctx) {
        id: "cluster.dfs.metadata-snapshot-reader-floor",
        summary: "Read checked floor-six physical snapshot headers and validate observed local migration jobs",
        tags: ["cluster-dfs", "dfs-storage-migration"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_snapshot_reader_floor(ctx) }
    }
}

#[derive(serde::Serialize)]
struct SnapshotObservation {
    shard_root: PathBuf,
    header: Option<DfsMetadataSnapshotReaderFloorEvidence>,
}

fn validate_observed_reader_floors(
    observed: &[SnapshotObservation],
    jobs: &[MigrationJob],
    require_floor6: bool,
) -> Result<(usize, usize), SinomoTestError> {
    let floor6_count = observed
        .iter()
        .filter(|entry| {
            entry
                .header
                .as_ref()
                .is_some_and(|header| header.format_version == 6)
        })
        .count();
    let mut completed_floor6_jobs = 0;
    for job in jobs {
        if job.identity.target_version != 6 {
            continue;
        }
        if job.identity.owner != "sinomo-dfs" || job.identity.migration != "metadata-snapshot-v6" {
            return Err(SinomoTestError::failed(
                "reader-floor job has an inconsistent migration identity",
            ));
        }
        if job.verified_records > job.copied_records {
            return Err(SinomoTestError::failed(
                "reader-floor migration has inconsistent record counts",
            ));
        }
        if job.phase == MigrationPhase::Superseded {
            continue;
        }
        if job.phase != MigrationPhase::Complete || job.copied_records != job.verified_records {
            return Err(SinomoTestError::failed(
                "reader-floor migration is not completely verified",
            ));
        }
        let scope = Path::new(&job.identity.scope);
        if !observed.iter().any(|entry| {
            entry.shard_root == scope
                && entry
                    .header
                    .as_ref()
                    .is_some_and(|header| header.format_version == 6)
        }) {
            return Err(SinomoTestError::failed(format!(
                "completed namespaced metadata migration has no installed checked floor-six snapshot at {}",
                scope.display(),
            )));
        }
        completed_floor6_jobs += 1;
    }
    // An authenticated snapshot transfer validates the prefix and publishes
    // the floor-six header directly. It requires no redundant local migration.
    // The orchestrating upgrade verifier separately proves a completed local
    // bounded floor-six migration somewhere in the cluster.
    if require_floor6 && floor6_count == 0 {
        return Err(SinomoTestError::failed(
            "no checked physical floor-six snapshot was observed",
        ));
    }
    Ok((floor6_count, completed_floor6_jobs))
}

fn run_snapshot_reader_floor(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let require_floor6 = optional_bool_parameter(ctx, "require-floor6", true)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let paths = sinomo_runtime::daemon::DaemonPaths::new(&data_dir);
    let daemon = sinomo_runtime::daemon::read_daemon_status(&paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let operator = crate::app_api::DfsOperatorClient::open_daemon_with_timeout(
        daemon.address,
        expect_timeout(ctx)?,
    )
    .map_err(SinomoTestError::failed)?;
    // A completed floor is sticky. Observe completion before inspecting its
    // physical header so a concurrent successful installation cannot make an
    // earlier floor-three observation look like a completed-floor mismatch.
    let status = operator.status().map_err(SinomoTestError::failed)?;
    let root = dfs::default_dfs_metadata_shards_root(&data_dir);
    let mut shards = fs::read_dir(&root)
        .map_err(|error| {
            SinomoTestError::failed(format!("cannot list metadata shard roots: {error}"))
        })?
        .take(MAX_OBSERVED_METADATA_SHARDS + 1)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    if shards.len() > MAX_OBSERVED_METADATA_SHARDS {
        return Err(SinomoTestError::failed(
            "metadata shard-root observation exceeded its bound; reader-floor coverage is incomplete",
        ));
    }
    shards.sort_by_key(|entry| entry.file_name());
    let mut observed = Vec::new();
    for shard in shards {
        let kind = shard
            .file_type()
            .map_err(|error| SinomoTestError::failed(error.to_string()))?;
        if kind.is_symlink() {
            return Err(SinomoTestError::failed(
                "metadata shard-root observation refuses a symlink",
            ));
        }
        if !kind.is_dir() {
            continue;
        }
        let shard_root = shard.path();
        // The core owner reads the bounded checksummed header from a single
        // opened generation and verifies its inner version, proof and digest.
        // It neither opens the metadata store nor acquires its writer lease.
        let header = dfs::inspect_metadata_snapshot_reader_floor(&shard_root)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?;
        observed.push(SnapshotObservation { shard_root, header });
    }
    let (floor6_count, completed_floor6_jobs) =
        validate_observed_reader_floors(&observed, &status.storage_migrations, require_floor6)?;
    ctx.record("snapshots", &observed);
    ctx.record("storage_migrations", &status.storage_migrations);
    ctx.record("checked_floor6_snapshot_count", floor6_count);
    ctx.record("completed_floor6_job_count", completed_floor6_jobs);
    Ok(SinomoTestOutcome::passed(
        "bounded checked physical snapshot headers establish floor six and all observed local migration jobs are valid",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::dfs::{DfsMetadataBackend, DfsMetadataShardId, LocalSobjMetadataBackend};

    #[test]
    fn validated_snapshot_install_can_prove_floor_six_without_a_local_migration_job() {
        let source_root = sinomo_testdir::TestDir::new("floor-six-proof-source");
        let destination_root = sinomo_testdir::TestDir::new("floor-six-proof-destination");
        let shard = DfsMetadataShardId::new("services");
        let mut source = LocalSobjMetadataBackend::new(shard.clone(), source_root.path()).unwrap();
        source
            .put_file_system(&dfs::DfsFileSystem {
                metadata_format: dfs::DfsRevisionMetadataFormat::NamespacedV6,
                file_system_id: dfs::sifs_file_system_id(),
                display_name: "physical floor fixture".into(),
                owner_node_id: NodeId::generate(),
                subtree_policies: vec![],
                retention_policies: BTreeMap::from([(
                    "module.test.reader-floor".into(),
                    dfs::DfsRetentionPolicy {
                        rules: vec![dfs::DfsRetentionRule {
                            path: "/services".into(),
                            minimum: dfs::DfsRetentionMinimum {
                                keep_last: 1,
                                keep_for_seconds: 0,
                            },
                        }],
                    },
                )]),
                module_metadata: Default::default(),
            })
            .unwrap();
        let snapshot = source.export_table_snapshot();
        let sequence = source.journal_last_sequence();
        let mut destination =
            LocalSobjMetadataBackend::new(shard.clone(), destination_root.path()).unwrap();
        destination
            .replace_with_replicated_snapshot(shard, sequence, snapshot)
            .unwrap();
        let jobs = destination.metadata_storage_migrations().unwrap();
        assert!(
            jobs.is_empty(),
            "a validated snapshot install performs no redundant local migration"
        );
        let header = dfs::inspect_metadata_snapshot_reader_floor(destination_root.path()).unwrap();
        assert_eq!(header.as_ref().unwrap().format_version, 6);
        let mut observed = vec![SnapshotObservation {
            shard_root: destination_root.path().to_owned(),
            header,
        }];
        assert_eq!(
            validate_observed_reader_floors(&observed, &jobs, true).unwrap(),
            (1, 0)
        );
        observed[0].header.as_mut().unwrap().format_version = 3;
        assert!(
            validate_observed_reader_floors(&observed, &jobs, true).is_err(),
            "a missing physical floor is never waived for a snapshot install"
        );
    }
}
