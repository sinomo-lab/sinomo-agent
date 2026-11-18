//! A cross-shard directory move parked on its destination shard while that
//! shard converts.
//!
//! The move from users to services adopts its root, and with it the
//! destination reservation, into services while services serves. Services
//! then closes for conversion before the move scans the root's first page.
//! That page carries an alias whose target is in services, so the move parks
//! on services. It also carries a directory that holds another: the moved
//! root's carried count includes the first, which is still a users row.
//! Services converts with the move parked, carries the reservation into its
//! image and activates, which ends the move's park; once services serves,
//! the rename's retry resumes the move, which adopts the rest and releases
//! the reservation.
use super::*;
use sinomo_dfs::test_support::DfsDirectoryMoveScanRefusal;
use sinomo_dfs::{
    DfsCountContributionPage, DfsMetadataQueryResult, DfsMetadataSelection, DfsNamespaceJobKind,
    DfsNamespaceJobStatus,
};

const FROM: &str = "/users/tree";
const TO: &str = "/services/tree";
const CANONICAL: &str = "/services/canonical";

fn query(
    cluster: &Cluster,
    shard: &DfsMetadataShardConfig,
    selection: DfsMetadataSelection,
) -> Option<DfsMetadataQueryResult> {
    cluster
        .native
        .client(&cluster.nodes[0])
        .unwrap()
        .query_metadata_authority(shard, selection)
        .ok()
}

/// `source`'s complete contribution page toward `target`, once it serves.
fn contributions(
    cluster: &Cluster,
    source: &DfsMetadataShardConfig,
    target: &str,
) -> Option<DfsCountContributionPage> {
    match query(
        cluster,
        source,
        DfsMetadataSelection::CountContributionsV1 {
            target_shard: target.into(),
            expected_prefix: None,
            after: None,
            question: Default::default(),
        },
    )? {
        DfsMetadataQueryResult::CountContributionsV1 { page } if page.complete => Some(page),
        _ => None,
    }
}

/// The Running job of `kind` that `shard` reports to `target`.
fn running_job(
    cluster: &Cluster,
    shard: &DfsMetadataShardConfig,
    target: &str,
    kind: DfsNamespaceJobKind,
) -> sinomo_dfs::DfsRunningJobContribution {
    let page = contributions(cluster, shard, target).expect("the shard serves its contributions");
    page.running_jobs
        .into_iter()
        .find(|job| job.kind == kind)
        .expect("the shard reports the Running job")
}

/// Whether `shard` records `job_id` as finished, as a Completed row or its
/// terminal receipt.
fn finished(cluster: &Cluster, shard: &DfsMetadataShardConfig, job_id: &str) -> bool {
    match query(
        cluster,
        shard,
        DfsMetadataSelection::NamespaceJob {
            job_id: job_id.into(),
        },
    ) {
        Some(DfsMetadataQueryResult::NamespaceJob { job, receipt }) => {
            job.is_some_and(|job| job.status == DfsNamespaceJobStatus::Completed)
                || receipt.is_some_and(|receipt| receipt.status == DfsNamespaceJobStatus::Completed)
        }
        _ => false,
    }
}

fn link_count(cluster: &Cluster, path: &str) -> u64 {
    cluster
        .native
        .client(&cluster.nodes[0])
        .unwrap()
        .posix_stat_distributed(path)
        .unwrap()
        .link_count
}

/// Services converts while a move into it is parked on it with its root
/// adopted: the conversion keeps the move's reservation and counts the
/// directories the move has not placed yet, services activates, and the
/// move finishes with every count right.
#[test]
fn native_a_move_parked_on_its_converting_destination_finishes_after_it_activates() {
    let cluster = start(
        "shard-conversion-parked-move",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    {
        let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
        client
            .posix_write_file(CANONICAL, b"canonical bytes", options(&cluster))
            .unwrap();
        client.posix_mkdir_all(&format!("{FROM}/sub/deep")).unwrap();
        client
            .posix_write_file(&format!("{FROM}/file"), b"moved bytes", options(&cluster))
            .unwrap();
        client
            .posix_hardlink(CANONICAL, &format!("{FROM}/alias"))
            .unwrap();
    }
    assert_eq!(link_count(&cluster, FROM), 3);
    assert_eq!(link_count(&cluster, CANONICAL), 2);
    let services_count = link_count(&cluster, "/services");
    let users_count = link_count(&cluster, "/users");
    let users = shard_config(&owners[0], &fs, "users");
    let services = shard_config(&owners[0], &fs, "services");

    // The move adopts its root, with the reservation, into services and is
    // held before it scans the root's first page.
    let refusal = DfsDirectoryMoveScanRefusal::install(FROM);
    let held = cluster
        .native
        .client(&cluster.nodes[0])
        .unwrap()
        .posix_rename(FROM, TO);
    assert!(
        matches!(
            &held,
            Err(sinomo_dfs::DfsPosixError::Dfs(
                sinomo_dfs::DfsError::Busy { .. }
            ))
        ),
        "{held:?}"
    );
    let job = running_job(
        &cluster,
        &users,
        "services",
        DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
    );
    assert_eq!(job.root_path, FROM);
    let reservation = running_job(
        &cluster,
        &services,
        "users",
        DfsNamespaceJobKind::DirectoryMoveDestinationReservation,
    );
    assert_eq!(reservation.root_path, TO);
    assert_eq!(link_count(&cluster, TO), 3, "the root carries its count");

    // Services closes; the move's next pass scans the page and parks on it.
    close(&owners[0], &fs, "services");
    for node in &cluster.nodes {
        cluster
            .native
            .wait_published_configuration_current(node)
            .unwrap();
    }
    drop(refusal);
    let _ = cluster.native.maintenance_tick(
        &cluster.nodes[2],
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let parked = loop {
        let page = contributions(&cluster, &users, "services").expect("users serves");
        let reported = page
            .running_jobs
            .iter()
            .find(|reported| reported.job_id == job.job_id)
            .cloned();
        if reported.is_some_and(|reported| {
            reported.waiting_for_storage_of == vec![sinomo_dfs::DfsMetadataShardId::new("services")]
        }) {
            break page;
        }
        assert!(
            Instant::now() < deadline,
            "the move did not park on services: {page:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut moving = parked
        .moving_directories
        .iter()
        .map(|moving| (moving.child_path.as_str(), moving.destination_path.as_str()))
        .collect::<Vec<_>>();
    moving.sort();
    let (sub, deep) = (format!("{FROM}/sub"), format!("{FROM}/sub/deep"));
    let (to_sub, to_deep) = (format!("{TO}/sub"), format!("{TO}/sub/deep"));
    assert_eq!(
        moving,
        vec![
            (sub.as_str(), to_sub.as_str()),
            (deep.as_str(), to_deep.as_str())
        ]
    );

    // Services converts with the move parked on it and activates. The scan
    // is refused again from here, so the move runs its next step only once
    // every member serves the converted image.
    let refusal = DfsDirectoryMoveScanRefusal::install(FROM);
    assert_eq!(
        drive(&owners[1], &fs, "services"),
        ConversionProgress::Ready
    );
    await_activated(&cluster, &owners, "services", &configured(&services));
    // The activation ends the move's park on services.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let page = contributions(&cluster, &users, "services").expect("users serves");
        let waiting = page
            .running_jobs
            .iter()
            .find(|reported| reported.job_id == job.job_id)
            .map(|reported| reported.waiting_for_storage_of.clone());
        if waiting == Some(Vec::new()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the activation did not end the move's park: {page:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    for reader in 0..cluster.nodes.len() {
        await_serving(&cluster, reader, &services, TO);
    }
    drop(refusal);

    // The rename's retry resumes the move, which adopts the rest; its finish
    // frame releases the carried reservation, and a dropped one would leave
    // no released row behind.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let resumed = cluster
            .native
            .client(&cluster.nodes[0])
            .unwrap()
            .posix_rename(FROM, TO);
        match resumed {
            Ok(stat) => {
                assert_eq!(stat.path, TO);
                break;
            }
            Err(error) => assert!(
                Instant::now() < deadline,
                "the move did not finish after services activated: {error:?}"
            ),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(finished(&cluster, &users, &job.job_id));
    assert!(
        finished(&cluster, &services, &reservation.job_id),
        "the carried reservation is released"
    );
    assert!(storage(&owners[0], &fs, "services").is_some_and(|record| record.is_active()));
    assert_eq!(link_count(&cluster, TO), 3);
    assert_eq!(link_count(&cluster, &to_sub), 3);
    assert_eq!(link_count(&cluster, &to_deep), 2);
    assert_eq!(link_count(&cluster, CANONICAL), 2);
    assert_eq!(link_count(&cluster, "/services"), services_count + 1);
    assert_eq!(link_count(&cluster, "/users"), users_count - 1);
    {
        let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
        assert_eq!(
            client
                .posix_read_file_distributed(&format!("{TO}/alias"))
                .unwrap(),
            b"canonical bytes"
        );
        assert_eq!(
            client
                .posix_read_file_distributed(&format!("{TO}/file"))
                .unwrap(),
            b"moved bytes"
        );
        assert!(client.posix_lstat_distributed(FROM).is_err());
    }
    write_beside_closed(&cluster, 0, &format!("{TO}/after-move"));
    drop(owners);
    cluster.native.close().unwrap();
}

/// The adoption a captured command dispatches, other than the move's root.
fn child_adoption(command: &sinomo_dfs::DfsMessageCommand, root: &str) -> Option<String> {
    let sinomo_dfs::DfsNetworkMessage::NamespaceRequest(request) = &command.routed_message.message
    else {
        return None;
    };
    match &request.operation {
        sinomo_dfs::DfsNamespaceOperation::AdoptObjectAtPath { object, .. }
            if object.path != root =>
        {
            Some(object.path.clone())
        }
        _ => None,
    }
}

/// A move whose next step adopts a child into services while services
/// converts parks on services instead of holding its conversion: the
/// adoption services refuses, and every later run of that step, wait there.
/// Services converts and activates, and the activation's run of the move
/// adopts the rest once services serves.
#[test]
fn native_a_move_whose_next_adoption_addresses_its_closed_destination_converges() {
    const FROM: &str = "/users/adopting";
    const TO: &str = "/services/adopting";
    let cluster = start(
        "shard-conversion-adopting-move",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    let owners = owners(&cluster);
    let fs = cluster.fs.clone();
    let coordinator = cluster.nodes[2].clone();
    {
        let mut client = cluster.native.client(&cluster.nodes[0]).unwrap();
        client.posix_mkdir_all(&format!("{FROM}/dir/deep")).unwrap();
        client
            .posix_write_file(&format!("{FROM}/file"), b"adopted bytes", options(&cluster))
            .unwrap();
    }
    let services_count = link_count(&cluster, "/services");
    let users_count = link_count(&cluster, "/users");
    let users = shard_config(&owners[0], &fs, "users");
    let services = shard_config(&owners[0], &fs, "services");

    // The move adopts its root and is held at its first child adoption,
    // before it is sent; services closes meanwhile.
    let mut pause = cluster
        .native
        .pause_remote_namespace_phase(&coordinator)
        .unwrap();
    let mut mover = cluster.native.client(&cluster.nodes[0]).unwrap();
    let (held, reservation) = std::thread::scope(|scope| {
        let moving = scope.spawn(move || mover.posix_rename(FROM, TO));
        let mut held = None;
        for _ in 0..16 {
            let command = pause
                .wait_captured(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
                .unwrap();
            if let Some(child) = child_adoption(&command, TO) {
                held = Some(child);
                break;
            }
            let next = cluster
                .native
                .pause_remote_namespace_phase(&coordinator)
                .unwrap();
            pause.release();
            pause = next;
        }
        let held = held.expect("the move reaches a child adoption");
        let reservation = running_job(
            &cluster,
            &services,
            "users",
            DfsNamespaceJobKind::DirectoryMoveDestinationReservation,
        );
        close(&owners[0], &fs, "services");
        for node in &cluster.nodes {
            cluster
                .native
                .wait_published_configuration_current(node)
                .unwrap();
        }
        pause.release();
        let refused = moving.join().unwrap();
        assert!(
            refused.is_err(),
            "the held adoption reaches closed services: {refused:?}"
        );
        (held, reservation)
    });
    drop(pause);
    let job = running_job(
        &cluster,
        &users,
        "services",
        DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
    );
    assert_eq!(job.root_path, FROM);
    assert_eq!(reservation.root_path, TO);
    // The test sends more RPCs than one observation window holds, so each
    // stage checkpoints the window before it can overflow.
    cluster.native.checkpoint_rpc_observation_window().unwrap();
    // The adoption waits on services: a retry of the rename runs it again
    // and parks it there.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let retried = cluster
            .native
            .client(&cluster.nodes[0])
            .unwrap()
            .posix_rename(FROM, TO);
        assert!(
            matches!(
                &retried,
                Err(sinomo_dfs::DfsPosixError::Dfs(
                    sinomo_dfs::DfsError::Busy { .. }
                ))
            ),
            "{retried:?}"
        );
        let page = contributions(&cluster, &users, "services").expect("users serves");
        let parked = page.running_jobs.iter().any(|reported| {
            reported.job_id == job.job_id
                && reported.waiting_for_storage_of
                    == vec![sinomo_dfs::DfsMetadataShardId::new("services")]
        });
        if parked {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the adoption did not park on services: {page:?}"
        );
        cluster.native.checkpoint_rpc_observation_window().unwrap();
        std::thread::sleep(Duration::from_millis(50));
    }

    cluster.native.checkpoint_rpc_observation_window().unwrap();
    // Services converts with the move parked on it. The activation's run of
    // the move is held at its adoption until every member serves.
    let pause = cluster
        .native
        .pause_remote_namespace_phase(&coordinator)
        .unwrap();
    assert_eq!(
        drive(&owners[1], &fs, "services"),
        ConversionProgress::Ready
    );
    await_activated(&cluster, &owners, "services", &configured(&services));
    let woken = pause
        .wait_captured(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
        .expect("the activation runs the parked move");
    assert_eq!(child_adoption(&woken, TO).as_deref(), Some(held.as_str()));
    for reader in 0..cluster.nodes.len() {
        await_serving(&cluster, reader, &services, TO);
    }
    cluster.native.checkpoint_rpc_observation_window().unwrap();
    pause.release();
    drop(pause);
    let deadline = Instant::now() + Duration::from_secs(120);
    while !finished(&cluster, &users, &job.job_id) {
        assert!(
            Instant::now() < deadline,
            "the move did not finish after services activated: {:?}",
            query(
                &cluster,
                &users,
                DfsMetadataSelection::NamespaceJob {
                    job_id: job.job_id.clone()
                }
            )
        );
        cluster.native.checkpoint_rpc_observation_window().unwrap();
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(finished(&cluster, &services, &reservation.job_id));
    assert_eq!(link_count(&cluster, TO), 3);
    assert_eq!(link_count(&cluster, &format!("{TO}/dir")), 3);
    assert_eq!(link_count(&cluster, &format!("{TO}/dir/deep")), 2);
    assert_eq!(link_count(&cluster, "/services"), services_count + 1);
    assert_eq!(link_count(&cluster, "/users"), users_count - 1);
    assert_eq!(
        cluster
            .native
            .client(&cluster.nodes[0])
            .unwrap()
            .posix_read_file_distributed(&format!("{TO}/file"))
            .unwrap(),
        b"adopted bytes"
    );
    write_beside_closed(&cluster, 0, &format!("{TO}/after-move"));
    drop(owners);
    cluster.native.close().unwrap();
}
