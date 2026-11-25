//! A directory move's job reads other primaries while maintenance resumes it,
//! on a cluster whose root, services and users shards each have one member.
//! The move carries an alias whose target is in users; the move waits while
//! users converts, and the users record's activation resumes its job in
//! maintenance, where the job asks every shard's primary for the alias's
//! target. Those reads are handed to the host: no turn holds admission across
//! them.

use super::namespace_counts::{
    close_shard_for_conversion, directory_move_job, disjoint_shard_cluster, replace_shard_storage,
};
use super::*;
use crate::read_handoff::DfsReadOwner;
use crate::test_runtime::{FaultAction, FaultRule, Partition, RpcPhase};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(60);

/// Well within the 30 s a read held under admission used to wait before it
/// failed: moves that end within it did not end by such a bound.
const CROSSING_BOUND: Duration = Duration::from_secs(15);

/// Far longer than a maintenance turn holds its node's admission for, far
/// shorter than a read held under it used to wait.
const ADMISSION_FREE_BOUND: Duration = Duration::from_secs(5);

fn wait_until(what: &str, bound: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + bound;
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Hold every namespace call from `source` to `target` until released.
fn hold_namespace_calls(
    runtime: &crate::test_runtime::DfsTestRuntime,
    source: &NodeId,
    target: &NodeId,
    gate: &Arc<tokio::sync::Semaphore>,
) {
    runtime.install_fault(FaultRule {
        source: Some(source.clone()),
        target: Some(target.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(gate)),
    });
}

fn release_namespace_calls(
    runtime: &crate::test_runtime::DfsTestRuntime,
    gate: &Arc<tokio::sync::Semaphore>,
) {
    runtime.remove_faults_matching(|rule| {
        matches!(&rule.action, FaultAction::PauseBeforeDispatch(held) if Arc::ptr_eq(held, gate))
    });
    gate.add_permits(64);
}

/// How many namespace calls `source` has sent `target` in this window.
fn namespace_calls(
    runtime: &crate::test_runtime::DfsTestRuntime,
    source: &NodeId,
    target: &NodeId,
) -> usize {
    runtime
        .rpc_observations()
        .iter()
        .filter(|call| {
            call.source == *source
                && call.target == *target
                && call.phase == RpcPhase::BeforeDispatch
                && call.route == crate::DFS_DATA_NAMESPACE_ROUTE
        })
        .count()
}

/// A job's status and its frames' kinds, for a failure's report.
fn frames(job: &DfsNamespaceJobRecord) -> (DfsNamespaceJobStatus, Vec<String>) {
    (
        job.status.clone(),
        job.pending_frames
            .iter()
            .map(|frame| {
                format!("{frame:?}")
                    .split([' ', '{', '('])
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect(),
    )
}

/// A node's full maintenance launches and completions.
fn turns(root: &std::path::Path) -> (u64, u64) {
    use crate::maintenance::phase_runs::{FULL_TICK_COMPLETION, FULL_TICK_LAUNCH, count};
    (
        count(root, FULL_TICK_LAUNCH),
        count(root, FULL_TICK_COMPLETION),
    )
}

fn job_owner(job: &DfsNamespaceJobRecord) -> DfsReadOwner {
    DfsReadOwner::Job(job.job_id.clone())
}

/// `from` holds an alias of the users file `canonical`, made through `client`.
fn tree_with_alias(
    client: &mut NetworkedDfsService<DfsNodeTransport>,
    writer: &NodeId,
    from: &str,
    canonical: &str,
) {
    client.posix_mkdir_all(from).unwrap();
    client
        .posix_write_file(
            canonical,
            b"handed-off read target bytes",
            DfsWriteOptions::replicated_eventual(vec![writer.clone()]),
        )
        .unwrap();
    client
        .posix_hardlink(canonical, &format!("{from}/alias"))
        .unwrap();
}

/// The move of `from` to `to` through `client` waits while users converts.
fn parked_move(client: &mut NetworkedDfsService<DfsNodeTransport>, from: &str, to: &str) {
    let parked = client.posix_rename(from, to);
    assert!(
        matches!(&parked, Err(DfsPosixError::Dfs(DfsError::Busy { .. }))),
        "the move waits on its carried alias's target shard: {parked:?}"
    );
}

fn moved(runtime: &crate::test_runtime::DfsTestRuntime, coordinator: &NodeId, from: &str) -> bool {
    directory_move_job(runtime, coordinator, from).status == DfsNamespaceJobStatus::Completed
}

/// While the move's job read on users' primary is held, the coordinator's
/// maintenance goes on: its turns end, its admission stays free, and another
/// job that needs several turns completes. The read is the job's own, keyed
/// by its job id although the job has an origin request: no request
/// continuation carries it. The job completes once its answer arrives.
#[test]
fn a_held_job_read_leaves_its_nodes_other_maintenance_running() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-handoff-held-tree-read");
    let root_node = cluster.replica_node.clone();
    let users_node = cluster.users_primary_node.clone();
    let writer = cluster.replica_node.clone();
    let (from, to) = ("/held-read-tree", "/services/held-read-tree");
    tree_with_alias(
        &mut cluster.replica,
        &writer,
        from,
        "/users/held-read-target",
    );
    cluster
        .replica
        .posix_mkdir_all("/held-read-other/old/nested")
        .unwrap();
    cluster
        .replica
        .posix_write_file(
            "/held-read-other/old/nested/file.txt",
            b"other job",
            DfsWriteOptions::replicated_eventual(vec![writer.clone()]),
        )
        .unwrap();

    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    parked_move(&mut cluster.replica, from, to);
    let job = directory_move_job(&cluster._runtime, &root_node, from);
    assert!(job.origin.is_some(), "the move's job has an origin request");

    cluster
        ._runtime
        .checkpoint_rpc_observation_window()
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    hold_namespace_calls(&cluster._runtime, &root_node, &users_node, &gate);
    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    let root = cluster._runtime.data_dir(&root_node).root();
    wait_until(
        "the job's read on users' primary is out",
        EVENT_BOUND,
        || {
            crate::read_handoff::waiting(&root, &job_owner(&job))
                && namespace_calls(&cluster._runtime, &root_node, &users_node) > 0
        },
    );

    // A one-step rename job needs a turn per remaining step.
    let other = cluster
        .replica
        .posix_rename_directory_bounded_routed("/held-read-other/old", "/held-read-other/new", 1)
        .unwrap();
    assert_eq!(other.status, DfsNamespaceJobStatus::Running);
    let job_status = |job_id: &str| {
        cluster
            ._runtime
            .node_metadata(&root_node)
            .unwrap()
            .list_namespace_jobs()
            .unwrap()
            .into_iter()
            .find(|job| job.job_id == job_id)
            .unwrap()
            .status
    };
    let deadline = Instant::now() + EVENT_BOUND;
    while job_status(&other.job_id) != DfsNamespaceJobStatus::Completed {
        assert!(Instant::now() < deadline, "the other job completes");
        let tick = cluster
            ._runtime
            .maintenance_tick(&root_node, Duration::from_secs(5))
            .unwrap();
        assert!(
            tick.outcome.is_ok(),
            "each turn ends while the read is held: {:?}",
            tick.outcome
        );
        assert!(
            !cluster
                ._runtime
                .main_global_writer_holds(&root_node)
                .unwrap()
        );
    }
    assert!(
        crate::read_handoff::waiting(&root, &job_owner(&job)),
        "the held job's read is still out"
    );
    assert_eq!(job_status(&job.job_id), DfsNamespaceJobStatus::Running);
    assert_eq!(
        cluster
            .replica
            .posix_read_file_distributed("/held-read-other/new/nested/file.txt")
            .unwrap(),
        b"other job"
    );

    release_namespace_calls(&cluster._runtime, &gate);
    wait_until(
        "the held move completes on its answers",
        EVENT_BOUND,
        || moved(&cluster._runtime, &root_node, from),
    );
    assert!(
        cluster
            .replica
            .posix_namespace_lstat_distributed(&format!("{to}/alias"))
            .is_ok()
    );
    drop(kv);
    cluster.close();
}

/// Two moves wait while users converts, one coordinated on the root primary R
/// and one on the services primary S, neither into a path at or above the
/// other's. Each carries an alias of a users file,
/// which makes it wait, and an alias of a file on the other coordinator's
/// shard. On the users record's activation, both moves resume in maintenance
/// and each asks the other's primary for its second alias's target, or adopts
/// a moved record there; namespace calls between R and S are held until a
/// call each way and a move's handed-off read are out. Neither node holds its
/// admission across its wait, so each answers the other, and both moves
/// complete well within the bound a read held under admission used to wait
/// for.
#[test]
fn crossing_jobs_each_read_the_others_primary_and_both_complete() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-handoff-crossing-jobs");
    let root_node = cluster.replica_node.clone();
    let services_node = cluster.services_primary_node.clone();
    let writer = cluster.replica_node.clone();
    // Neither move's destination is at or above the other's moving path, so
    // neither refuses the other as busy.
    let (from_r, to_r) = (
        "/crossing-source-r/tree",
        "/services/crossing-target-s/tree",
    );
    let (from_s, to_s) = (
        "/services/crossing-source-s/tree",
        "/crossing-target-r/tree",
    );
    for directory in ["/services/crossing-target-s", "/crossing-target-r"] {
        cluster.replica.posix_mkdir_all(directory).unwrap();
    }
    tree_with_alias(
        &mut cluster.replica,
        &writer,
        from_r,
        "/users/crossing-root-target",
    );
    tree_with_alias(
        &mut cluster.replica,
        &writer,
        from_s,
        "/users/crossing-services-target",
    );
    for (tree, peer) in [
        (from_r, "/services/crossing-peer-of-r"),
        (from_s, "/crossing-peer-of-s"),
    ] {
        cluster
            .replica
            .posix_write_file(
                peer,
                b"peer target bytes",
                DfsWriteOptions::replicated_eventual(vec![writer.clone()]),
            )
            .unwrap();
        cluster
            .replica
            .posix_hardlink(peer, &format!("{tree}/peer-alias"))
            .unwrap();
    }

    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    parked_move(&mut cluster.replica, from_r, to_r);
    parked_move(&mut cluster.services_primary, from_s, to_s);
    let job_r = directory_move_job(&cluster._runtime, &root_node, from_r);
    let job_s = directory_move_job(&cluster._runtime, &services_node, from_s);

    cluster
        ._runtime
        .checkpoint_rpc_observation_window()
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    hold_namespace_calls(&cluster._runtime, &root_node, &services_node, &gate);
    hold_namespace_calls(&cluster._runtime, &services_node, &root_node, &gate);
    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    let root_r = cluster._runtime.data_dir(&root_node).root();
    let root_s = cluster._runtime.data_dir(&services_node).root();
    // Each node's work waits on the other: a call each way is held, and the
    // handed-off read of a move (its job's or its request's) is out.
    let read_out = |root: &std::path::Path, job: &DfsNamespaceJobRecord| {
        crate::read_handoff::waiting(root, &job_owner(job))
            || job.origin.as_ref().is_some_and(|origin| {
                crate::read_handoff::waiting(
                    root,
                    &DfsReadOwner::request(&origin.requester_node_id, &origin.request_id),
                )
            })
    };
    let crossing = || {
        (
            read_out(&root_r, &job_r),
            read_out(&root_s, &job_s),
            namespace_calls(&cluster._runtime, &root_node, &services_node),
            namespace_calls(&cluster._runtime, &services_node, &root_node),
        )
    };
    let deadline = Instant::now() + EVENT_BOUND;
    loop {
        let (r_read, s_read, r_to_s, s_to_r) = crossing();
        if (r_read || s_read) && r_to_s > 0 && s_to_r > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "each node waits on the other with a read out: R's read out {r_read}, S's \
             {s_read}; R sent S {r_to_s} calls and S sent R {s_to_r}; R's job {:?}, S's job \
             {:?}; due on R {:?}, on S {:?}; turns on R {:?}, on S {:?}; R sent U {}, S sent U {}",
            frames(&directory_move_job(&cluster._runtime, &root_node, from_r)),
            frames(&directory_move_job(
                &cluster._runtime,
                &services_node,
                from_s
            )),
            crate::read_handoff::due_owners(&root_r),
            crate::read_handoff::due_owners(&root_s),
            turns(&root_r),
            turns(&root_s),
            namespace_calls(&cluster._runtime, &root_node, &cluster.users_primary_node),
            namespace_calls(
                &cluster._runtime,
                &services_node,
                &cluster.users_primary_node
            ),
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    wait_until(
        "neither node holds its admission across its wait",
        ADMISSION_FREE_BOUND,
        || {
            !cluster
                ._runtime
                .main_global_writer_holds(&root_node)
                .unwrap()
                && !cluster
                    ._runtime
                    .main_global_writer_holds(&services_node)
                    .unwrap()
        },
    );

    release_namespace_calls(&cluster._runtime, &gate);
    let deadline = Instant::now() + CROSSING_BOUND;
    while !(moved(&cluster._runtime, &root_node, from_r)
        && moved(&cluster._runtime, &services_node, from_s))
    {
        if Instant::now() >= deadline {
            let owners = |root: &std::path::Path, job: &DfsNamespaceJobRecord| {
                let request = job.origin.as_ref().map(|origin| {
                    DfsReadOwner::request(&origin.requester_node_id, &origin.request_id)
                });
                format!(
                    "job waits {} parked {}, request waits {:?} parked {:?}",
                    crate::read_handoff::waiting(root, &job_owner(job)),
                    crate::read_handoff::parked(root, &job_owner(job)),
                    request
                        .as_ref()
                        .map(|owner| crate::read_handoff::waiting(root, owner)),
                    request
                        .as_ref()
                        .map(|owner| crate::read_handoff::parked(root, owner)),
                )
            };
            let pending = |node: &NodeId| {
                cluster
                    ._runtime
                    .node_metadata(node)
                    .unwrap()
                    .list_pending_namespace_requests_page(None, 16)
                    .unwrap()
                    .requests
                    .into_iter()
                    .map(|request| (request.recovery_attempts, request.last_recovery_error))
                    .collect::<Vec<_>>()
            };
            panic!(
                "both moves complete: R's {:?} ({}), S's {:?} ({}); turns on R {:?}, on S \
                 {:?}; pending on R {:?}, on S {:?}",
                frames(&directory_move_job(&cluster._runtime, &root_node, from_r)),
                owners(&root_r, &job_r),
                frames(&directory_move_job(
                    &cluster._runtime,
                    &services_node,
                    from_s
                )),
                owners(&root_s, &job_s),
                turns(&root_r),
                turns(&root_s),
                pending(&root_node),
                pending(&services_node),
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    for path in [
        format!("{to_r}/alias"),
        format!("{to_r}/peer-alias"),
        format!("{to_s}/alias"),
        format!("{to_s}/peer-alias"),
    ] {
        assert!(
            cluster
                .replica
                .posix_namespace_lstat_distributed(&path)
                .is_ok(),
            "{path} arrived"
        );
    }
    drop(kv);
    cluster.close();
}

/// The move's job reads users' primary while it cannot be reached: the read
/// ends on the network's no-path verdict and parks the job. Nothing else
/// happens in the cluster, and the job stays quiet: no turn runs it and no
/// read is sent again. When the path to users' primary comes back, the job
/// runs again and the move completes.
#[test]
fn a_job_whose_read_found_no_path_stays_quiet_until_the_path_comes_back() {
    let (mut cluster, nodes) = disjoint_shard_cluster("native-handoff-parked-tree-read");
    let root_node = cluster.replica_node.clone();
    let users_node = cluster.users_primary_node.clone();
    let writer = cluster.replica_node.clone();
    let (from, to) = ("/parked-read-tree", "/services/parked-read-tree");
    tree_with_alias(
        &mut cluster.replica,
        &writer,
        from,
        "/users/parked-read-target",
    );

    let mut kv = cluster._runtime.configuration_publisher(&root_node);
    let (active, converting) =
        close_shard_for_conversion(&cluster._runtime, kv.as_mut(), &nodes, "users");
    parked_move(&mut cluster.replica, from, to);
    let job = directory_move_job(&cluster._runtime, &root_node, from);

    cluster
        ._runtime
        .checkpoint_rpc_observation_window()
        .unwrap();
    cluster._runtime.partition_node(
        &users_node,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    cluster._runtime.set_peer_verdict(
        &root_node,
        &users_node,
        sinomo_core::network::PeerPathVerdict::NoPath,
        false,
    );
    replace_shard_storage(
        &cluster._runtime,
        kv.as_mut(),
        &nodes,
        &converting,
        active.value.clone(),
    );
    let root = cluster._runtime.data_dir(&root_node).root();
    wait_until(
        "the job's read found no path and parked it",
        EVENT_BOUND,
        || crate::read_handoff::parked(&root, &job_owner(&job)),
    );

    // Quiet: once the turns that parked it have ended, no turn runs, so the
    // parked job sends nothing more to users.
    let launches = || {
        use crate::maintenance::phase_runs::{FULL_TICK_COMPLETION, FULL_TICK_LAUNCH, count};
        (
            count(&root, FULL_TICK_LAUNCH),
            count(&root, FULL_TICK_COMPLETION),
        )
    };
    let mut settled = launches();
    let mut stable = 0;
    while stable < 10 {
        std::thread::sleep(Duration::from_millis(50));
        let current = launches();
        stable = if current == settled && current.0 == current.1 {
            stable + 1
        } else {
            0
        };
        settled = current;
    }
    std::thread::sleep(Duration::from_secs(3));
    assert!(crate::read_handoff::parked(&root, &job_owner(&job)));
    assert_eq!(
        launches(),
        settled,
        "no turn runs while users' primary has no path"
    );
    assert_eq!(
        directory_move_job(&cluster._runtime, &root_node, from).status,
        DfsNamespaceJobStatus::Running
    );

    cluster
        ._runtime
        .partition_node(&users_node, Partition::default());
    cluster._runtime.set_peer_verdict(
        &root_node,
        &users_node,
        sinomo_core::network::PeerPathVerdict::Path,
        false,
    );
    wait_until("the path back resumes the move", EVENT_BOUND, || {
        moved(&cluster._runtime, &root_node, from)
    });
    assert!(
        cluster
            .replica
            .posix_namespace_lstat_distributed(&format!("{to}/alias"))
            .is_ok()
    );
    drop(kv);
    cluster.close();
}
