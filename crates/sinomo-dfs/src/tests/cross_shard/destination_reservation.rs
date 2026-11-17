//! A cross-shard directory move reserves its destination subtree on the
//! destination shard until it finishes or aborts.

use super::parent_counts::foreign_parent_cluster;
use super::*;

const SOURCE: &str = "/users/reserved-move";
const TARGET: &str = "/services/reserved-move";

fn write_options(cluster: &SifsTwoPrimaryCluster) -> DfsWriteOptions {
    DfsWriteOptions::replicated_eventual(vec![cluster.replica_node.clone()])
}

fn is_busy<T: std::fmt::Debug>(result: &Result<T, DfsPosixError>) -> bool {
    matches!(result, Err(DfsPosixError::Dfs(DfsError::Busy { .. })))
}

struct HeldMove<R> {
    observed: R,
    outcome: Result<DfsStat, DfsPosixError>,
    pending: String,
}

/// Start the move of a two-file directory and hold it at its second child
/// adoption: the root and the first child are adopted, the second is not.
/// `while_held` gets the adopted and the pending child paths.
fn hold_move_before_second_child<R>(
    cluster: &mut SifsTwoPrimaryCluster,
    while_held: impl FnOnce(&mut SifsTwoPrimaryCluster, &str, &str) -> R,
) -> HeldMove<R> {
    let mut mover = cluster._runtime.client(&cluster.replica_node).unwrap();
    let coordinator = cluster.users_primary_node.clone();
    let mut pause = cluster
        ._runtime
        .pause_remote_namespace_phase(&coordinator)
        .unwrap();
    std::thread::scope(|scope| {
        let moving = scope.spawn(move || mover.posix_rename(SOURCE, TARGET));
        let mut adopted_children = Vec::new();
        // Release each actual remote phase until the second child adoption is
        // captured; a new pause is installed before the previous one releases.
        let held = (|| -> std::result::Result<(String, String), String> {
            for _ in 0..8 {
                let command = pause.wait_captured(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)?;
                let DfsNetworkMessage::NamespaceRequest(request) = command.routed_message.message
                else {
                    return Err("captured another remote phase kind".into());
                };
                let next = cluster
                    ._runtime
                    .pause_remote_namespace_phase(&coordinator)?;
                if let DfsNamespaceOperation::AdoptObjectAtPath { object, .. } = &request.operation
                    && object.path != TARGET
                {
                    adopted_children.push(object.path.clone());
                    if adopted_children.len() == 2 {
                        drop(next);
                        return Ok((adopted_children[0].clone(), adopted_children[1].clone()));
                    }
                }
                pause.release();
                pause = next;
            }
            Err(format!(
                "second child adoption was not reached: {adopted_children:?}"
            ))
        })();
        let observed = held
            .as_ref()
            .map(|(adopted, pending)| while_held(cluster, adopted, pending));
        pause.release();
        let outcome = moving.join().unwrap();
        let observed = observed.map_err(Clone::clone).unwrap();
        HeldMove {
            observed,
            outcome,
            pending: held.unwrap().1,
        }
    })
}

fn reservation_rows(cluster: &SifsTwoPrimaryCluster, path: &str) -> Vec<DfsNamespaceJobRecord> {
    cluster
        ._runtime
        .node_metadata(&cluster.services_primary_node)
        .unwrap()
        .directory_move_destination_reservations(
            &DfsMetadataShardId::new("services"),
            &sifs_file_system_id(),
            path,
        )
        .unwrap()
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

#[derive(Debug)]
struct HeldObservations {
    create_at_pending_child: Result<DfsStat, DfsPosixError>,
    create_new_file: Result<DfsStat, DfsPosixError>,
    write_adopted_child: Result<DfsStat, DfsPosixError>,
    mkdir_on_destination_primary: Result<DfsStat, DfsPosixError>,
    read_adopted_child: Result<Vec<u8>, DfsPosixError>,
    list_target: Result<Vec<DfsDirEntry>, DfsPosixError>,
    reservations: usize,
}

fn observe_held_move(
    cluster: &mut SifsTwoPrimaryCluster,
    adopted: &str,
    pending: &str,
) -> HeldObservations {
    let options = write_options(cluster);
    HeldObservations {
        create_at_pending_child: cluster.replica.posix_write_file(
            pending,
            b"concurrent create",
            options.clone(),
        ),
        create_new_file: cluster.replica.posix_write_file(
            &format!("{TARGET}/created-during-move"),
            b"concurrent create",
            options.clone(),
        ),
        write_adopted_child: cluster.replica.posix_write_file(
            adopted,
            b"concurrent overwrite",
            options,
        ),
        mkdir_on_destination_primary: cluster
            .services_primary
            .posix_mkdir(&format!("{TARGET}/made-during-move")),
        read_adopted_child: cluster.replica.posix_read_file_distributed(adopted),
        list_target: cluster.replica.posix_list_dir_distributed(TARGET),
        reservations: reservation_rows(cluster, TARGET).len(),
    }
}

fn source_tree(cluster: &mut SifsTwoPrimaryCluster) -> BTreeMap<String, DfsStat> {
    cluster.replica.posix_mkdir(SOURCE).unwrap();
    let options = write_options(cluster);
    let mut children = BTreeMap::new();
    for name in ["first", "second"] {
        let path = format!("{SOURCE}/{name}");
        cluster
            .replica
            .posix_write_file(&path, format!("moved {name}").as_bytes(), options.clone())
            .unwrap();
        children.insert(
            name.to_owned(),
            cluster.replica.posix_lstat_distributed(&path).unwrap(),
        );
    }
    children
}

#[test]
fn cross_shard_directory_move_reserves_its_destination_until_it_finishes() {
    // Each shard has only its primary: the services primary, which adopts
    // the move, does not replicate the users shard that coordinates it.
    let mut cluster = foreign_parent_cluster("native-move-destination-reservation");
    let children = source_tree(&mut cluster);
    let HeldMove {
        observed: held,
        outcome,
        pending: pending_path,
    } = hold_move_before_second_child(&mut cluster, observe_held_move);
    outcome.expect("the held move completes once released");
    let pending_name = pending_path
        .strip_prefix(&format!("{TARGET}/"))
        .expect("pending child of the target")
        .to_owned();
    let pending_now = cluster
        .replica
        .posix_lstat_distributed(&pending_path)
        .unwrap();
    assert!(
        is_busy(&held.create_at_pending_child),
        "a create at a path the move will adopt must wait while it runs; it returned \
         {:?} and the move's adoption then left object {} with {:?} there",
        held.create_at_pending_child,
        pending_now.object_id,
        cluster.replica.posix_read_file_distributed(&pending_path),
    );
    assert!(is_busy(&held.create_new_file), "{:?}", held.create_new_file);
    assert!(
        is_busy(&held.write_adopted_child),
        "{:?}",
        held.write_adopted_child
    );
    assert!(
        is_busy(&held.mkdir_on_destination_primary),
        "{:?}",
        held.mkdir_on_destination_primary
    );
    assert_eq!(held.reservations, 1);
    let adopted_name = children
        .keys()
        .find(|name| **name != pending_name)
        .unwrap()
        .clone();
    assert_eq!(
        held.read_adopted_child.unwrap(),
        format!("moved {adopted_name}").into_bytes(),
        "reads under the reserved destination are not blocked"
    );
    assert!(
        held.list_target
            .unwrap()
            .iter()
            .any(|entry| entry.name == adopted_name)
    );

    // Nothing was replaced: both children are the moved objects.
    for (name, before) in &children {
        let path = format!("{TARGET}/{name}");
        let after = cluster.replica.posix_lstat_distributed(&path).unwrap();
        assert_eq!(after.object_id, before.object_id, "{path}");
        assert_eq!(
            cluster.replica.posix_read_file_distributed(&path).unwrap(),
            format!("moved {name}").into_bytes()
        );
    }
    assert!(reservation_rows(&cluster, TARGET).is_empty());
    let job = move_job(&cluster);
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    let released = cluster
        ._runtime
        .node_metadata(&cluster.services_primary_node)
        .unwrap()
        .get_namespace_job(
            &job.directory_move_destination_reservation_job()
                .unwrap()
                .job_id,
        )
        .unwrap();
    assert!(
        released
            .as_ref()
            .is_none_or(|row| row.status == DfsNamespaceJobStatus::Completed),
        "the finish frame releases the reservation: {released:?}"
    );
    let options = write_options(&cluster);
    cluster
        .replica
        .posix_write_file(
            &format!("{TARGET}/created-during-move"),
            b"after the move",
            options,
        )
        .expect("the same create succeeds after the move");
    cluster
        .services_primary
        .posix_mkdir(&format!("{TARGET}/made-during-move"))
        .expect("the same mkdir succeeds after the move");
    cluster.close();
}

#[test]
fn aborted_cross_shard_directory_move_releases_its_destination_reservation() {
    let mut cluster = foreign_parent_cluster("native-move-destination-reservation-abort");
    let _children = source_tree(&mut cluster);
    // A subdirectory is scanned after every child has been adopted; its scan
    // fails, which aborts the move after its root holds the destination.
    let subdirectory = format!("{SOURCE}/third");
    cluster.replica.posix_mkdir(&subdirectory).unwrap();
    crate::posix::fail_directory_move_scan_of(subdirectory);
    let HeldMove {
        observed: held,
        outcome,
        ..
    } = hold_move_before_second_child(&mut cluster, |cluster, _adopted, _pending| {
        let options = write_options(cluster);
        (
            cluster.replica.posix_write_file(
                &format!("{TARGET}/created-during-move"),
                b"concurrent create",
                options,
            ),
            reservation_rows(cluster, TARGET).len(),
        )
    });
    let (create_while_running, reservations_while_running) = held;
    assert!(
        is_busy(&create_while_running),
        "a create must wait while the move runs: {create_while_running:?}"
    );
    assert_eq!(reservations_while_running, 1);
    assert!(
        outcome
            .as_ref()
            .is_err_and(|error| error.to_string().contains("injected directory-move scan")),
        "the failed scan aborts the move: {outcome:?}"
    );
    let job = move_job(&cluster);
    assert_eq!(job.status, DfsNamespaceJobStatus::Failed, "{job:?}");
    assert!(
        reservation_rows(&cluster, TARGET).is_empty(),
        "the abort released the destination before failing the move"
    );
    let released = cluster
        ._runtime
        .node_metadata(&cluster.services_primary_node)
        .unwrap()
        .get_namespace_job(
            &job.directory_move_destination_reservation_job()
                .unwrap()
                .job_id,
        )
        .unwrap();
    assert!(
        released
            .as_ref()
            .is_none_or(|row| row.status == DfsNamespaceJobStatus::Completed),
        "{released:?}"
    );
    let options = write_options(&cluster);
    cluster
        .replica
        .posix_write_file(
            &format!("{TARGET}/created-during-move"),
            b"after the abort",
            options,
        )
        .expect("a create succeeds once the move aborted");
    cluster.close();
}
