//! A cross-shard directory move whose finish frame runs after the coordinator
//! node's storage generation advanced, outside any shard conversion.
//!
//! A shard birth writes a new shard configuration and a new storage record, so
//! every node advances its storage generation and rebuilds its message lanes
//! around a reconfigured resident backend. The move fences its source parent
//! while it runs, so its finish frame is the parent's only writer: it reads the
//! parent through the rebuilt lane and touches it from the version it reads,
//! and later writes on the coordinator read that touch. The same holds when
//! maintenance resumes the move while the caller's run waits at its finish
//! frame, so the frame's touch runs twice.

use super::parent_counts::foreign_parent_cluster;
use super::*;

const SOURCE: &str = "/users/finish-after-birth";
const TARGET: &str = "/services/finish-after-birth";
const PARENT: &str = "/users";

fn write_options(cluster: &SifsTwoPrimaryCluster) -> DfsWriteOptions {
    DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()])
}

fn storage_generation(cluster: &SifsTwoPrimaryCluster, node: &NodeId) -> u64 {
    crate::runtime_host::daemon_dfs_metadata_authority_generation(
        &sinomo_core::network::ClusterPaths::new(cluster._runtime.data_dir(node)),
        &cluster._runtime.runtime_handle(),
    )
    .expect("the node's metadata authority runtime")
    .storage_generation()
}

/// Run an ordinary native shard birth and return the coordinator's storage
/// generation before and after it.
fn birth(cluster: &SifsTwoPrimaryCluster, shard_id: &str, prefix: &str) -> (u64, u64) {
    let coordinator = &cluster.users_primary_node;
    let before = storage_generation(cluster, coordinator);
    cluster
        ._runtime
        .bootstrap_empty_metadata_prefix(shard_id, prefix)
        .expect("an ordinary native shard birth");
    cluster
        ._runtime
        .wait_published_configuration_current(coordinator)
        .unwrap();
    (before, storage_generation(cluster, coordinator))
}

/// The parent row in the coordinator's own users store.
fn stored_parent(cluster: &SifsTwoPrimaryCluster) -> DfsObjectMetadata {
    hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
        .unwrap()
        .tables
        .objects
        .into_iter()
        .find(|object| object.path == PARENT && object.kind == DfsObjectKind::Directory)
        .expect("the users shard holds its root directory")
}

fn move_job(cluster: &SifsTwoPrimaryCluster) -> DfsNamespaceJobRecord {
    hardlink_read_set_snapshot(&cluster._runtime, &cluster.users_primary_node, "users")
        .unwrap()
        .tables
        .namespace_jobs
        .into_iter()
        .find(|job| {
            job.kind == DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries
                && job.root_path == SOURCE
        })
        .expect("the move's durable coordinator job")
}

fn source_tree(cluster: &mut SifsTwoPrimaryCluster) {
    cluster.replica.posix_mkdir(SOURCE).unwrap();
    cluster
        .replica
        .posix_mkdir(&format!("{SOURCE}/sub"))
        .unwrap();
    let options = write_options(cluster);
    cluster
        .replica
        .posix_write_file(&format!("{SOURCE}/file"), b"moved bytes", options)
        .unwrap();
}

/// Start the move and hold it at its finish frame's release of the
/// destination reservation, the frame's last step before it touches the
/// source parent. `while_held` runs there; the move then continues.
fn hold_at_finish<R>(
    cluster: &mut SifsTwoPrimaryCluster,
    while_held: impl FnOnce(&mut SifsTwoPrimaryCluster) -> R,
) -> (R, Result<DfsStat, DfsPosixError>) {
    let coordinator = cluster.users_primary_node.clone();
    let mut pause = cluster
        ._runtime
        .pause_remote_namespace_phase(&coordinator)
        .unwrap();
    let mut mover = cluster._runtime.client(&cluster.replica_node).unwrap();
    let (held, outcome) = std::thread::scope(|scope| {
        let moving = scope.spawn(move || mover.posix_rename(SOURCE, TARGET));
        // Release each remote phase until the release is captured. A capture
        // consumes its pause, so a new one is installed before each release.
        let held = (|| -> std::result::Result<(), String> {
            for _ in 0..16 {
                let command = pause.wait_captured(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)?;
                if matches!(
                    &command.routed_message.message,
                    DfsNetworkMessage::NamespaceRequest(request)
                        if matches!(
                            request.operation,
                            DfsNamespaceOperation::ReleaseDirectoryMoveDestination { .. }
                        )
                ) {
                    return Ok(());
                }
                let next = cluster
                    ._runtime
                    .pause_remote_namespace_phase(&coordinator)?;
                pause.release();
                pause = next;
            }
            Err("the move's finish frame was not reached".into())
        })();
        let observed = held.map(|()| while_held(cluster));
        pause.release();
        (observed, moving.join().unwrap())
    });
    (held.unwrap(), outcome)
}

/// After the move: it is complete, its tree is at the target, and later
/// creates in the parent on the coordinator commit from the parent's current
/// row, across another birth's storage-generation advance.
fn assert_moved_and_parent_current(cluster: &mut SifsTwoPrimaryCluster, held_parent: &DfsVersion) {
    let job = move_job(cluster);
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed, "{job:?}");
    let parent = stored_parent(cluster);
    assert!(
        parent.version > *held_parent,
        "the finish frame touches the parent: {held_parent:?} then {:?}",
        parent.version
    );
    assert_eq!(
        cluster
            .replica
            .posix_read_file_distributed(&format!("{TARGET}/file"))
            .unwrap(),
        b"moved bytes"
    );
    assert!(cluster.replica.posix_lstat_distributed(SOURCE).is_err());
    cluster
        .replica
        .posix_mkdir(&format!("{PARENT}/after-move-a"))
        .expect("a create in the parent after the move commits");
    let (before, after) = birth(cluster, "born-after-move", "/born-after-move");
    assert!(
        after > before,
        "the second birth advances the storage generation"
    );
    cluster
        .replica
        .posix_mkdir(&format!("{PARENT}/after-move-b"))
        .expect("a create in the parent after the second birth commits");
    assert!(stored_parent(cluster).version > parent.version);
}

fn is_parent_busy(result: &Result<DfsStat, DfsPosixError>) -> bool {
    matches!(result, Err(DfsPosixError::Dfs(DfsError::Busy { path })) if path == PARENT)
}

#[test]
fn native_cross_shard_move_finishes_after_a_shard_birth_at_its_finish_frame() {
    let mut cluster = foreign_parent_cluster("native-move-finish-after-birth");
    source_tree(&mut cluster);
    let ((generations, concurrent_write, held_parent), outcome) =
        hold_at_finish(&mut cluster, |cluster| {
            let generations = birth(cluster, "born-at-finish", "/users/born-at-finish");
            // The first command on the coordinator after the birth runs on a
            // lane rebuilt for the new storage generation; a create in the
            // source parent is refused while the move runs.
            let options = write_options(cluster);
            let concurrent_write = cluster.replica.posix_write_file(
                &format!("{PARENT}/written-at-finish"),
                b"written while the move waits",
                options,
            );
            (
                generations,
                concurrent_write,
                stored_parent(cluster).version,
            )
        });
    assert!(
        generations.1 > generations.0,
        "the birth advances the coordinator's storage generation: {generations:?}"
    );
    assert!(
        is_parent_busy(&concurrent_write),
        "the move fences its source parent: {concurrent_write:?}"
    );
    let stat = outcome.expect("the move finishes after the birth");
    assert_eq!(stat.path, TARGET);
    assert_moved_and_parent_current(&mut cluster, &held_parent);
    cluster.close();
}

/// While a move runs in a directory, every kind of create in that directory
/// is refused by the primary that serves it, whatever lookup its requester
/// made: a routed write, an exclusive create, an owner path write and a
/// create issued on the primary itself. Each is allowed once the move ends.
#[test]
fn native_creates_in_a_moving_entrys_parent_are_refused_at_their_primary() {
    let mut cluster = foreign_parent_cluster("native-move-fences-creates");
    source_tree(&mut cluster);
    let coordinator = cluster.users_primary_node.clone();
    let path = |name: &str| format!("{PARENT}/{name}");
    let (refused, outcome) = hold_at_finish(&mut cluster, |cluster| {
        let options = write_options(cluster);
        let routed = cluster
            .replica
            .posix_write_file(&path("routed-create"), b"routed", options.clone())
            .map(drop);
        let exclusive = cluster
            .replica
            .posix_symlink("/services", &path("exclusive-create"), options.clone())
            .map(drop);
        let owner = cluster
            .replica
            .try_route_browser_write(&path("owner-create"), b"owner", None)
            .map(drop);
        let mut primary = cluster._runtime.client(&coordinator).unwrap();
        let local = primary
            .posix_write_file(&path("primary-create"), b"local", options)
            .map(drop);
        drop(primary);
        [
            ("routed write", routed),
            ("exclusive create", exclusive),
            ("owner write", owner),
            ("create on the primary", local),
        ]
    });
    for (kind, result) in &refused {
        assert!(
            matches!(result, Err(DfsPosixError::Dfs(DfsError::Busy { path })) if path == PARENT),
            "a {kind} in the moving entry's parent is refused as busy: {result:?}"
        );
    }
    outcome.expect("the move finishes");
    let options = write_options(&cluster);
    cluster
        .replica
        .posix_write_file(&path("routed-create"), b"routed", options.clone())
        .expect("a routed create is allowed after the move");
    cluster
        .replica
        .posix_symlink("/services", &path("exclusive-create"), options.clone())
        .expect("an exclusive create is allowed after the move");
    cluster
        .replica
        .try_route_browser_write(&path("owner-create"), b"owner", None)
        .expect("an owner write is allowed after the move");
    let mut primary = cluster._runtime.client(&coordinator).unwrap();
    primary
        .posix_write_file(&path("primary-create"), b"local", options)
        .expect("a create on the primary is allowed after the move");
    drop(primary);
    cluster.close();
}

#[test]
fn native_cross_shard_move_resumed_by_maintenance_at_its_finish_frame_converges() {
    let mut cluster = foreign_parent_cluster("native-move-finish-resumed-after-birth");
    source_tree(&mut cluster);
    let coordinator = cluster.users_primary_node.clone();
    let ((generations, tick, held_parent), outcome) = hold_at_finish(&mut cluster, |cluster| {
        let held_parent = stored_parent(cluster).version;
        let generations = birth(cluster, "born-at-finish", "/users/born-at-finish");
        // Maintenance on the coordinator resumes the Running move while the
        // caller's run waits: its own finish frame releases the reservation
        // and touches the parent through the lane rebuilt after the birth.
        let tick = cluster
            ._runtime
            .maintenance_tick(&coordinator, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .map(|observation| observation.outcome.map(drop));
        (generations, tick, held_parent)
    });
    assert!(
        generations.1 > generations.0,
        "the birth advances the coordinator's storage generation: {generations:?}"
    );
    assert!(
        matches!(tick, Ok(Ok(()))),
        "the coordinator's maintenance pass runs: {tick:?}"
    );
    // The caller's run continues from its held release and touches the
    // parent again from the row the maintenance touch committed.
    let stat = outcome.expect("the caller's move returns once its finish frame runs");
    assert_eq!(stat.path, TARGET);
    assert_moved_and_parent_current(&mut cluster, &held_parent);
    cluster.close();
}
