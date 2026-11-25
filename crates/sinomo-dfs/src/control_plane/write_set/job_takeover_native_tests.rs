//! Namespace jobs whose shard's primary is lost, on the native test runtime:
//! the root shard is on the write-set protocol with three members, its
//! primary R records a clear job and a rename-directory job Running with no
//! step run, both members hold them, and R is lost before it runs them. The
//! member that takes the root shard over finishes both, with no maintenance
//! pass run for it.

use super::participant_takeover_native_tests::{
    b_takes_over_root, catch_up_root, heal_root, parent_on_a_switched_root, snapshot,
};
use super::takeover_driver_native_tests::keep_with_paths;
use super::takeover_loss_native_tests::wait_until;
use super::*;
use crate::test_support::DfsTestRuntime;
use crate::{DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceJobStatus, DfsObjectKind};

/// A directory with two files under it, written through `node`.
fn tree(runtime: &DfsTestRuntime, node: &NodeId, path: &str) {
    let mut client = runtime.client(node).unwrap();
    client.posix_mkdir_all(path).unwrap();
    for name in ["a", "b"] {
        client
            .posix_write_file(
                &format!("{path}/{name}"),
                name.as_bytes(),
                crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
    }
}

/// The visible kinds of the objects at `paths` in `node`'s copy of the
/// root shard.
fn visible(runtime: &DfsTestRuntime, node: &NodeId, paths: &[&str]) -> Vec<Option<DfsObjectKind>> {
    let copy = snapshot(runtime, node, "root");
    paths
        .iter()
        .map(|path| {
            copy.tables
                .objects
                .iter()
                .find(|object| object.path == *path && object.kind != DfsObjectKind::Tombstone)
                .map(|object| object.kind.clone())
        })
        .collect()
}

/// Whether `job` is Running in `node`'s copy of the root shard; `None`
/// without its row.
fn running(runtime: &DfsTestRuntime, node: &NodeId, job: &DfsNamespaceJobRecord) -> Option<bool> {
    snapshot(runtime, node, "root")
        .tables
        .namespace_jobs
        .iter()
        .find(|row| row.job_id == job.job_id)
        .map(|row| row.status == DfsNamespaceJobStatus::Running)
}

/// Whether `job` completed in `node`'s copy of the root shard.
fn completed(runtime: &DfsTestRuntime, node: &NodeId, job: &DfsNamespaceJobRecord) -> bool {
    let copy = snapshot(runtime, node, "root");
    copy.tables
        .namespace_jobs
        .iter()
        .any(|row| row.job_id == job.job_id && row.status == DfsNamespaceJobStatus::Completed)
        || copy
            .tables
            .namespace_job_receipts
            .iter()
            .any(|receipt| receipt.job_id == job.job_id)
}

/// The cluster with R's two jobs held Running by both members, R not yet
/// lost. Returns the runtime, the root shard, U, B and the two jobs.
fn jobs_of_a_root_about_to_be_lost(
    name: &str,
) -> (
    DfsTestRuntime,
    DfsMetadataShardConfig,
    NodeId,
    NodeId,
    [DfsNamespaceJobRecord; 2],
) {
    let (runtime, root, u, b) = parent_on_a_switched_root(name);
    let r = root.primary_node_id.clone();
    keep_with_paths(&runtime, &root, &[&b]);
    tree(&runtime, &b, "/clear-me");
    tree(&runtime, &b, "/rename-from");
    // A maintenance checkout does not wake R's maintenance, so both jobs
    // stay Running at R until it is lost.
    let jobs = runtime
        .with_main_maintenance_service(&r, |service| {
            let clear = service
                .posix_clear_dir_recursive_bounded("/clear-me", 0)
                .unwrap();
            let rename = service
                .posix_rename_directory_bounded("/rename-from", "/rename-to", 0)
                .unwrap();
            [clear, rename]
        })
        .unwrap();
    assert_eq!(jobs[0].kind, DfsNamespaceJobKind::ClearDirectoryRecursive);
    assert_eq!(jobs[1].kind, DfsNamespaceJobKind::RenameDirectory);
    for member in [&u, &b] {
        catch_up_root(&runtime, &root, member);
    }
    for job in &jobs {
        assert_eq!(
            running(&runtime, &b, job),
            Some(true),
            "B holds {} Running before the takeover",
            job.job_id
        );
    }
    (runtime, root, u, b, jobs)
}

/// Both jobs' effects, in `node`'s copy of the root shard.
fn assert_both_finished(runtime: &DfsTestRuntime, node: &NodeId) {
    assert_eq!(
        visible(
            runtime,
            node,
            &[
                "/clear-me",
                "/clear-me/a",
                "/rename-from",
                "/rename-to",
                "/rename-to/a",
                "/rename-to/b"
            ]
        ),
        [
            Some(DfsObjectKind::Directory),
            None,
            None,
            Some(DfsObjectKind::Directory),
            Some(DfsObjectKind::File),
            Some(DfsObjectKind::File),
        ]
    );
}

/// B takes the root shard over and finishes both of R's jobs.
#[test]
fn native_successor_finishes_clear_and_rename_directory_jobs_after_a_takeover() {
    let (runtime, root, u, b, jobs) = jobs_of_a_root_about_to_be_lost("native-jobs-successor");
    b_takes_over_root(&runtime, &root, &u, &b);
    wait_until("B finishes both jobs", &runtime, &[&b], || {
        jobs.iter().all(|job| completed(&runtime, &b, job))
    });
    assert_both_finished(&runtime, &b);
    heal_root(&runtime, &root, &u, &b);
    runtime.close().unwrap();
}

/// [`native_successor_finishes_clear_and_rename_directory_jobs_after_a_takeover`]
/// with B's activation requesting no reconciliation: the record that names
/// B primary at the new epoch alone requests the jobs turn that finishes
/// both jobs, well before any pass a timer runs.
#[test]
fn native_gained_primary_wake_alone_resumes_the_jobs_of_a_lost_primary() {
    use crate::maintenance::phase_runs::{JOBS_TURN_LAUNCH, count};
    let (runtime, root, u, b, jobs) = jobs_of_a_root_about_to_be_lost("native-jobs-gained-primary");
    let b_root = runtime.data_dir(&b).root();
    let jobs_turns_before = count(&b_root, JOBS_TURN_LAUNCH);
    runtime.skip_takeover_activation_reconciliation(&b, true);
    b_takes_over_root(&runtime, &root, &u, &b);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while !jobs.iter().all(|job| completed(&runtime, &b, job)) {
        assert!(
            std::time::Instant::now() < deadline,
            "the record naming B primary resumes both jobs: {:?}",
            jobs.iter()
                .map(|job| running(&runtime, &b, job))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        count(&b_root, JOBS_TURN_LAUNCH) > jobs_turns_before,
        "the record requests a jobs turn at B"
    );
    assert_both_finished(&runtime, &b);
    runtime.skip_takeover_activation_reconciliation(&b, false);
    heal_root(&runtime, &root, &u, &b);
    runtime.close().unwrap();
}
