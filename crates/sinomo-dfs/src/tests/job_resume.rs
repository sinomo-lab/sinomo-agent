//! Namespace jobs run only at their shard's primary, and each job's outcome
//! in a maintenance pass is its own.

use super::*;
use crate::test_runtime::{DfsTestRuntime, Partition};

/// `node`'s copy of `shard`, opened for inspection.
fn copy_of(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    shard: &str,
) -> crate::DfsMetadataTableSnapshot {
    let shard = crate::DfsMetadataShardId::new(shard);
    let root = crate::local_dfs_metadata_shard_path(runtime.data_dir(node), &shard);
    crate::LocalSobjMetadataBackend::open_for_inspection(shard, root)
        .unwrap()
        .export_table_snapshot()
        .unwrap()
}

/// The row of `job_id` in `node`'s copy of `shard`.
fn job_in(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    shard: &str,
    job_id: &str,
) -> Option<DfsNamespaceJobRecord> {
    copy_of(runtime, node, shard)
        .tables
        .namespace_jobs
        .into_iter()
        .find(|job| job.job_id == job_id)
}

/// Replicate `primary`'s journal of the shard that owns `path` to `member`
/// through the ordinary durability barrier.
fn catch_up(service: &mut NetworkedDfsService<DfsNodeTransport>, path: &str, member: &NodeId) {
    let target = service
        .capture_metadata_quorum_sync_target_for_namespace_scope(
            &sifs_file_system_id(),
            path,
            "job-resume-catch-up",
        )
        .unwrap();
    service
        .sync_metadata_shard_to_quorum_intersecting(
            &target,
            std::slice::from_ref(member),
            Duration::from_secs(30),
        )
        .expect("the member catches up");
}

/// A directory with two files under it.
fn tree(service: &mut NetworkedDfsService<DfsNodeTransport>, writer: &NodeId, path: &str) {
    service.posix_mkdir_all(path).unwrap();
    for name in ["a", "b"] {
        service
            .posix_write_file(
                &format!("{path}/{name}"),
                name.as_bytes(),
                DfsWriteOptions::replicated_eventual(vec![writer.clone()]),
            )
            .unwrap();
    }
}

/// A replica's copy holds a Running clear job and a Running rename-directory
/// job of a shard it replicates. Its maintenance pass resumes neither, a
/// direct run of either is refused as the wrong owner before it reads or
/// writes anything, and the replica's copy is unchanged: the jobs stay
/// Running there, for their primary or a successor.
#[test]
fn a_replica_never_runs_or_fails_a_clear_or_rename_directory_job() {
    let mut cluster = sifs_two_primary_cluster("job-resume-replica");
    let users = cluster.users_primary_node.clone();
    let replica = cluster.replica_node.clone();
    tree(&mut cluster.replica, &replica, "/users/clear-me");
    tree(&mut cluster.replica, &replica, "/users/rename-from");
    let runtime = &cluster._runtime;
    // The users primary records both jobs Running with no step run. A
    // maintenance checkout does not wake the primary's maintenance, so
    // they stay Running until the primary is cut off.
    let (clear, rename) = runtime
        .with_main_maintenance_service(&users, |service| {
            let clear = service
                .posix_clear_dir_recursive_bounded("/users/clear-me", 0)
                .unwrap();
            let rename = service
                .posix_rename_directory_bounded("/users/rename-from", "/users/rename-to", 0)
                .unwrap();
            catch_up(service, "/users", &replica);
            (clear, rename)
        })
        .unwrap();
    runtime.partition_node(
        &users,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    assert_eq!(clear.kind, DfsNamespaceJobKind::ClearDirectoryRecursive);
    assert_eq!(rename.kind, DfsNamespaceJobKind::RenameDirectory);
    for job in [&clear, &rename] {
        assert_eq!(
            job_in(runtime, &replica, "users", &job.job_id).map(|job| job.status),
            Some(DfsNamespaceJobStatus::Running),
            "the replica holds {} Running",
            job.job_id
        );
    }
    let before = copy_of(runtime, &replica, "users");

    let (report, direct) = runtime
        .with_main_maintenance_service(&replica, |service| {
            let report = service.posix_resume_namespace_jobs_bounded(8, 512).unwrap();
            let direct = [&clear, &rename]
                .map(|job| service.posix_resume_namespace_job_bounded(&job.job_id, 512));
            (report, direct)
        })
        .unwrap();
    assert_eq!(
        (report.scanned_job_count, report.resumed_job_count),
        (0, 0),
        "a replica pages no job of a shard it is not primary of: {report:?}"
    );
    assert!(report.failures.is_empty(), "{report:?}");
    for outcome in direct {
        assert!(
            matches!(
                &outcome,
                Err(DfsPosixError::Dfs(DfsError::StaleOwnerEpoch { primary_node_id, .. }))
                    if *primary_node_id == users
            ),
            "a replica refuses to run the job: {outcome:?}"
        );
    }
    let after = copy_of(runtime, &replica, "users");
    assert_eq!(
        after.prefix_proof, before.prefix_proof,
        "the replica wrote nothing"
    );
    assert_eq!(after.tables.namespace_jobs, before.tables.namespace_jobs);

    runtime.partition_node(&users, Partition::default());
    cluster.close();
}

/// A pass over three Running jobs: the first fails definitely, the second
/// fails too but its failure cannot be recorded, and the third can run. The
/// first is Failed, the second stays Running with its line in the report,
/// and the third completes in the same pass.
#[test]
fn one_failing_job_does_not_stop_the_pass() {
    let mut cluster = sifs_two_primary_cluster("job-resume-failing");
    let users = cluster.users_primary_node.clone();
    let replica = cluster.replica_node.clone();
    tree(&mut cluster.replica, &replica, "/users/clear-me");
    cluster.replica.posix_mkdir_all("/users/failing").unwrap();
    cluster
        .replica
        .posix_mkdir_all("/users/unrecorded")
        .unwrap();
    let runtime = &cluster._runtime;
    // A clear job whose next frame is not a clear frame ends with a definite
    // error. Their ids sort before the clear job's, so the pass meets them
    // first.
    let malformed = |job_id: &str, path: &str| {
        let now = time::OffsetDateTime::now_utc();
        DfsNamespaceJobRecord {
            job_id: job_id.into(),
            file_system_id: sifs_file_system_id(),
            origin: None,
            kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
            status: DfsNamespaceJobStatus::Running,
            root_path: path.into(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::RenameDirectoryComplete {
                source_object_id: DfsObjectId::from_legacy_named(job_id).unwrap(),
                source_path: path.into(),
                target_path: format!("{path}-elsewhere"),
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        }
    };
    let failing = malformed("namespace_job_0_failing", "/users/failing");
    let unrecorded = malformed("namespace_job_1_unrecorded", "/users/unrecorded");
    // The unrecorded job's failure is refused before it is prepared.
    let refused = unrecorded.job_id.clone();
    let _refusal = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&users),
        Arc::new(move |point, _, _| {
            let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare {
                payload,
            } = point
            else {
                return Ok(());
            };
            if payload.metadata_effects.iter().any(|effect| {
                matches!(
                    effect,
                    crate::DfsPreparedMetadataEffect::PutNamespaceJob { job }
                        if job.job_id == refused && job.status == DfsNamespaceJobStatus::Failed
                )
            }) {
                return Err(DfsError::Metadata {
                    message: "the failure is not recorded".into(),
                });
            }
            Ok(())
        }),
    );
    let (report, clear) = runtime
        .with_main_maintenance_service(&users, |service| {
            service.persist_namespace_job(&failing).unwrap();
            service.persist_namespace_job(&unrecorded).unwrap();
            let clear = service
                .posix_clear_dir_recursive_bounded("/users/clear-me", 0)
                .unwrap();
            let report = service.posix_resume_namespace_jobs_bounded(8, 512).unwrap();
            (report, clear)
        })
        .unwrap();
    assert!(failing.job_id < unrecorded.job_id && unrecorded.job_id < clear.job_id);
    assert_eq!(report.resumed_job_count, 3, "{report:?}");
    assert_eq!(report.failed_job_count, 1, "{report:?}");
    assert_eq!(report.completed_job_count, 1, "{report:?}");
    assert_eq!(report.remaining_running_job_count, 1, "{report:?}");
    let lines = report
        .failures
        .iter()
        .map(|failure| failure.job_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(lines, [failing.job_id.as_str(), unrecorded.job_id.as_str()]);
    assert!(
        report.failures[1]
            .reason
            .contains("recording the failure did not complete"),
        "{report:?}"
    );
    let status = |job: &DfsNamespaceJobRecord| {
        job_in(runtime, &users, "users", &job.job_id).map(|job| job.status)
    };
    assert_eq!(status(&failing), Some(DfsNamespaceJobStatus::Failed));
    assert_eq!(status(&unrecorded), Some(DfsNamespaceJobStatus::Running));
    assert_eq!(status(&clear), Some(DfsNamespaceJobStatus::Completed));
    cluster.close();
}

/// While another driver's step holds a job's claim, the jobs pass does not
/// step the job and is not refused as busy: it leaves the job to the claim's
/// release. The release alone runs the job again, which completes.
#[test]
fn a_job_held_by_another_drivers_step_runs_once_that_step_releases_it() {
    let mut cluster = sifs_two_primary_cluster("job-resume-claimed");
    let users = cluster.users_primary_node.clone();
    let replica = cluster.replica_node.clone();
    tree(&mut cluster.replica, &replica, "/users/clear-me");
    let runtime = &cluster._runtime;
    let clear = runtime
        .with_main_maintenance_service(&users, |service| {
            service
                .posix_clear_dir_recursive_bounded("/users/clear-me", 0)
                .unwrap()
        })
        .unwrap();
    let status = || job_in(runtime, &users, "users", &clear.job_id).map(|job| job.status);
    // Another driver's step holds the job.
    let claim = crate::maintenance::job_claims::try_claim(&users, &clear.job_id)
        .expect("nothing else steps the job");
    let pass = runtime
        .maintenance_tick(&users, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .unwrap();
    assert!(pass.outcome.is_ok(), "{:?}", pass.outcome);
    assert_eq!(status(), Some(DfsNamespaceJobStatus::Running));
    drop(claim);
    let deadline = Instant::now() + Duration::from_secs(60);
    while status() != Some(DfsNamespaceJobStatus::Completed) {
        assert!(
            Instant::now() < deadline,
            "the release runs the job again: {:?}",
            status()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    cluster.close();
}

/// A driver cut short after preparing a step's directory touch, as a jobs
/// pass that yields to foreground work is, leaves that touch prepared with
/// its commit's outcome unknown, and the step runs again. The next touch of
/// the directory finishes the prepared one and touches the row it leaves; it
/// is not refused for having read the row before the prepared touch.
#[test]
fn a_touch_after_a_cut_short_touch_of_its_directory_finishes_that_touch_first() {
    let mut cluster = sifs_two_primary_cluster("job-resume-cut-short-touch");
    let users = cluster.users_primary_node.clone();
    let path = "/users/touched";
    cluster.replica.posix_mkdir_all(path).unwrap();
    let runtime = &cluster._runtime;
    let shard_root = crate::local_dfs_metadata_shard_path(
        runtime.data_dir(&users),
        &crate::DfsMetadataShardId::new("users"),
    );
    let row = || {
        copy_of(runtime, &users, "users")
            .tables
            .objects
            .into_iter()
            .find(|object| object.path == path && object.kind == DfsObjectKind::Directory)
            .unwrap()
    };
    let before = row();
    // The first touch's commit is held once its body is prepared.
    type Held = Option<(
        String,
        crate::metadata_backend::prepared_operation::PreparedCommitHold,
    )>;
    let held: Arc<Mutex<Held>> = Arc::default();
    let armed = Arc::new(AtomicBool::new(true));
    let hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&users),
        Arc::new({
            let held = Arc::clone(&held);
            let armed = Arc::clone(&armed);
            let object_id = before.object_id.clone();
            move |point, _, _| {
                let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare {
                    payload,
                } = point
                else {
                    return Ok(());
                };
                if payload.metadata_effects.iter().any(|effect| {
                    matches!(effect, crate::DfsPreparedMetadataEffect::PutObject { object }
                        if object.object_id == object_id)
                }) && armed.swap(false, Ordering::SeqCst)
                {
                    *held.lock().unwrap() = Some((
                        payload.operation_id.clone(),
                        crate::metadata_backend::prepared_operation::hold_prepared_commit(
                            std::slice::from_ref(&shard_root),
                            &payload.operation_id,
                        ),
                    ));
                }
                Ok(())
            }
        }),
    );
    let cut_short = runtime
        .with_main_maintenance_service(&users, |service| {
            assert!(
                service.posix_touch_directory_modified(path).is_err(),
                "the held touch does not complete"
            );
            let (cut_short, hold) = held.lock().unwrap().take().expect("the touch was prepared");
            let record = |service: &NetworkedDfsService<DfsNodeTransport>| {
                service
                    .metadata_backend()
                    .unwrap()
                    .prepared_operation_record(&cut_short)
                    .unwrap()
                    .expect("the touch's body is durable")
            };
            assert_eq!(record(service).status, DfsPreparedOperationStatus::Prepared);
            drop(hold);
            service
                .posix_touch_directory_modified(path)
                .expect("the next touch finishes the prepared one first");
            assert_eq!(
                record(service).status,
                DfsPreparedOperationStatus::Committed
            );
            record(service)
        })
        .unwrap();
    drop(hook);
    let touched = cut_short
        .payload
        .metadata_effects
        .iter()
        .find_map(|effect| match effect {
            crate::DfsPreparedMetadataEffect::PutObject { object } => Some(object.clone()),
            _ => None,
        })
        .unwrap();
    let after = row();
    assert_ne!(after.version, before.version);
    assert_ne!(after.version, touched.version);
    assert!(after.modified_at_unix_nanos > touched.modified_at_unix_nanos);
    cluster.close();
}
