//! A target fence's commit, repeated once the fence is Completed.
use super::*;

/// A target fence that is already Completed commits again as a no-op, even
/// after its target changed: the fence was committed, by an earlier run of
/// the step or by its shard's conversion, and a later write to the target
/// must not fail its coordinator's commit.
#[test]
fn a_completed_target_fence_commits_again_as_a_no_op() {
    let mut cluster = sifs_two_primary_cluster("native-completed-fence-commit");
    let canonical = "/users/completed-fence-canonical";
    let alias_path = "/services/completed-fence-alias";
    let options = || DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()]);
    cluster
        .replica
        .posix_write_file(canonical, b"committed generation", options())
        .unwrap();
    let committed =
        node_store_object_at(&cluster._runtime, &cluster.users_primary_node, canonical).unwrap();
    let expected = DfsNamespaceObjectPrecondition {
        object_id: committed.object_id.clone(),
        version: DfsVersion::generate(),
    };
    let now = time::OffsetDateTime::now_utc();
    let fence = DfsNamespaceJobRecord {
        job_id: "completed-target-fence".into(),
        file_system_id: committed.file_system_id.clone(),
        origin: None,
        kind: DfsNamespaceJobKind::HardlinkTargetFenceAcrossShardPrimary,
        status: DfsNamespaceJobStatus::Completed,
        root_path: canonical.into(),
        target_path: None,
        pending_frames: vec![DfsNamespaceJobFrame::HardlinkTargetFenceReservation {
            source_path: alias_path.into(),
            expected_target: expected.clone(),
            committed_target: committed.clone(),
        }],
        processed_object_count: 1,
        created_at: now,
        updated_at: now,
        completed_at: Some(now),
        failure_reason: None,
    };
    {
        let mut owner = cluster
            ._runtime
            .metadata_primary_service(canonical)
            .unwrap();
        owner.persist_namespace_job(&fence).unwrap();
    }
    cluster
        .replica
        .posix_write_file(canonical, b"a later generation", options())
        .unwrap();
    let changed =
        node_store_object_at(&cluster._runtime, &cluster.users_primary_node, canonical).unwrap();
    assert_ne!(changed.version, committed.version);
    let mut owner = cluster
        ._runtime
        .metadata_primary_service(canonical)
        .unwrap();
    owner
        .commit_hardlink_target_fence_local(
            &fence.job_id,
            alias_path,
            canonical,
            &expected,
            &committed,
        )
        .unwrap();
    assert_eq!(owner.namespace_job(&fence.job_id).unwrap(), Some(fence));
    drop(owner);
    assert_eq!(
        node_store_object_at(&cluster._runtime, &cluster.users_primary_node, canonical),
        Some(changed)
    );
    cluster.close();
}
