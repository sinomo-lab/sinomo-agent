//! Work that holds admission hands its reads on other nodes' primaries to its
//! host instead of waiting for them, on the native test runtime: a served
//! request on one shard's lane, and a namespace job maintenance resumes.

use super::*;
use crate::test_runtime::{FaultAction, FaultRule, RpcPhase};
use crate::test_support::DfsTestRuntime;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(60);

/// Longer than the 30 s a read made under admission used to be bounded by.
const HELD_PAST_THE_OLD_BOUND: Duration = Duration::from_secs(31);

/// Far longer than other work holds a lane for, far shorter than the hold.
const LANE_HELD_BY_OTHER_WORK: Duration = Duration::from_secs(5);

/// Hold every namespace call from `source` to `target` until the returned
/// gate is released.
fn hold_namespace_calls(
    runtime: &DfsTestRuntime,
    source: &NodeId,
    target: &NodeId,
) -> Arc<tokio::sync::Semaphore> {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(source.clone()),
        target: Some(target.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });
    gate
}

/// Let every held call go and hold no more.
fn release_namespace_calls(runtime: &DfsTestRuntime, gate: &Arc<tokio::sync::Semaphore>) {
    runtime.remove_faults_matching(|rule| {
        matches!(&rule.action, FaultAction::PauseBeforeDispatch(held) if Arc::ptr_eq(held, gate))
    });
    gate.add_permits(32);
}

/// Whether a namespace call from `source` to `target` is held.
fn namespace_call_held(runtime: &DfsTestRuntime, source: &NodeId, target: &NodeId) -> bool {
    runtime.rpc_observations().iter().any(|call| {
        call.source == *source
            && call.target == *target
            && call.phase == RpcPhase::BeforeDispatch
            && call.route == crate::DFS_DATA_NAMESPACE_ROUTE
    })
}

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + EVENT_BOUND;
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// One write of `path` from `node`'s client, on its own thread.
fn write_from(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    path: &'static str,
) -> mpsc::Receiver<Result<(), String>> {
    let mut client = runtime.client(node).unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let written = client
            .posix_write_file(
                path,
                path.as_bytes(),
                crate::DfsWriteOptions::replicated_eventual(Vec::new()),
            )
            .map(drop)
            .map_err(|error| error.to_string());
        let _ = sender.send(written);
    });
    receiver
}

/// A move of `/users` to `/services/moved-users` coordinated on U, which
/// alone holds users and services, while R is the root shard's primary: the
/// move counts `/` through a parent-count participant phase on R's root
/// lane, and that phase reads the move's job from U. U's answers to R are
/// held past the old 30 s bound. R's root lane is free the whole time, a
/// write to the root shard completes on it meanwhile, and the move completes
/// once the answers go.
#[test]
fn native_single_lane_phase_held_past_the_old_read_bound_releases_its_lane_and_completes() {
    let name = "native-handoff-single-lane";
    let (runtime, service, root) =
        crate::test_support::current_native_metadata_service(name, 3, "/").unwrap();
    drop(service);
    assert_eq!(root.shard_id, "root");
    let r = root.primary_node_id.clone();
    let others = DfsTestRuntime::ordered_node_ids(name, 3)
        .into_iter()
        .filter(|node| *node != r)
        .collect::<Vec<_>>();
    let (u, b) = (others[0].clone(), others[1].clone());
    for shard in runtime.metadata_shards().unwrap() {
        if shard.shard_id != "root" {
            runtime
                .reassign_metadata_shard(
                    &shard.file_system_id,
                    &shard.shard_id,
                    u.clone(),
                    Vec::new(),
                )
                .unwrap();
        }
    }
    let file_system_id = root.file_system_id.clone();
    // The move reserves `/`; a write elsewhere on the root shard leaves it be.
    runtime
        .client(&b)
        .unwrap()
        .posix_mkdir_all("/elsewhere")
        .unwrap();

    runtime.checkpoint_rpc_observation_window().unwrap();
    let gate = hold_namespace_calls(&runtime, &r, &u);
    let mut caller = runtime.client(&b).unwrap();
    let coordinator = u.clone();
    let (moved_tx, moved) = mpsc::channel();
    std::thread::spawn(move || {
        let result = caller
            .route_namespace_operation_with_request_id(
                sinomo_core::Id::generate().to_string(),
                coordinator,
                crate::DfsNamespaceOperation::Rename {
                    from: "/users".into(),
                    to: "/services/moved-users".into(),
                },
            )
            .map(drop)
            .map_err(|error| error.to_string());
        let _ = moved_tx.send(result);
    });
    wait_until("the participant phase on R reads U", || {
        namespace_call_held(&runtime, &r, &u)
    });
    let held_at = Instant::now();
    wait_until("R's root lane is free while its read is held", || {
        !runtime.main_lane_held(&r, &file_system_id, "root").unwrap()
    });
    // The lane serves other work while the read is held.
    let during = write_from(
        &runtime,
        &b,
        "/elsewhere/written-while-the-read-is-held.txt",
    )
    .recv_timeout(EVENT_BOUND)
    .expect("the root write ends");
    assert!(
        during.is_ok(),
        "a root write completes on the free lane: {during:?}"
    );
    // Other work, such as a maintenance turn's global writer, takes the lane
    // for moments; a phase waiting on the lane would hold it the whole time.
    let mut last_free = Instant::now();
    while held_at.elapsed() < HELD_PAST_THE_OLD_BOUND {
        if !runtime.main_lane_held(&r, &file_system_id, "root").unwrap() {
            last_free = Instant::now();
        }
        assert!(
            last_free.elapsed() < LANE_HELD_BY_OTHER_WORK,
            "R's root lane is not held across the held read"
        );
        assert!(
            matches!(moved.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "the move waits for the held answer"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    release_namespace_calls(&runtime, &gate);
    let result = moved.recv_timeout(EVENT_BOUND).expect("the move ends");
    assert!(result.is_ok(), "the move completes: {result:?}");
    let mut reader = runtime.client(&b).unwrap();
    assert!(
        reader
            .posix_stat_distributed("/services/moved-users")
            .is_ok(),
        "the moved directory is at its destination"
    );
    assert!(
        matches!(
            reader.posix_stat_distributed("/users"),
            Err(crate::DfsPosixError::NotFound { .. })
        ),
        "the moved directory left its source"
    );
    drop(reader);
    runtime.close().unwrap();
}

/// A namespace job's step that a foreground request cuts short runs again as
/// soon as that request releases its admission, with nothing else happening
/// in the cluster: the job stays Running rather than failing, and the
/// release, not a later wake, resumes it.
#[test]
fn native_job_step_cut_short_by_foreground_work_completes_when_admission_frees() {
    let (runtime, service, users) = crate::test_support::current_native_metadata_service(
        "native-handoff-cut-short-step",
        3,
        "/users",
    )
    .unwrap();
    drop(service);
    let a = users.primary_node_id.clone();
    let mut client = runtime.client(&a).unwrap();
    for child in ["one", "two", "three"] {
        let directory = format!("/users/cut-short/old/{child}");
        client.posix_mkdir_all(&directory).unwrap();
        client
            .posix_write_file(
                &format!("{directory}/file.txt"),
                child.as_bytes(),
                crate::DfsWriteOptions::replicated_eventual(Vec::new()),
            )
            .unwrap();
    }
    // The served request runs one step; maintenance runs the rest.
    let job = client
        .posix_rename_directory_bounded_routed("/users/cut-short/old", "/users/cut-short/new", 1)
        .unwrap();
    assert_eq!(job.status, crate::DfsNamespaceJobStatus::Running);

    // Stop maintenance's first step of the job until foreground work has
    // asked it to give way, then let the step go on.
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&a)).unwrap();
    let (stopped_tx, stopped) = mpsc::sync_channel(1);
    let stop = std::sync::Mutex::new(Some(stopped_tx));
    let job_id = job.job_id.clone();
    let step_scheduler = Arc::clone(&scheduler);
    let _hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
        runtime.data_dir(&a),
        Arc::new(move |point, _, _| {
            let crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare {
                payload,
            } = point
            else {
                return Ok(());
            };
            let steps_on = payload.metadata_effects.iter().any(|effect| {
                matches!(effect, crate::DfsPreparedMetadataEffect::PutNamespaceJob { job }
                    if job.job_id == job_id && job.processed_object_count >= 2)
            });
            if steps_on && let Some(stopped) = stop.lock().unwrap().take() {
                let requests = step_scheduler.foreground_requests();
                stopped.send(()).unwrap();
                let deadline = Instant::now() + EVENT_BOUND;
                while step_scheduler.foreground_requests() == requests {
                    assert!(
                        Instant::now() < deadline,
                        "foreground work asks for admission"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            Ok(())
        }),
    );
    // Foreground work on the node while the step is stopped.
    let mut foreground_client = runtime.client(&a).unwrap();
    let (foreground_tx, foreground) = mpsc::channel();
    std::thread::spawn(move || {
        if stopped.recv_timeout(EVENT_BOUND).is_err() {
            return;
        }
        let written = foreground_client
            .posix_write_file(
                "/users/cut-short/foreground.txt",
                b"foreground",
                crate::DfsWriteOptions::replicated_eventual(Vec::new()),
            )
            .map(drop)
            .map_err(|error| error.to_string());
        let _ = foreground_tx.send(written);
    });
    let tick = runtime.maintenance_tick(&a, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    let foreground = foreground
        .recv_timeout(EVENT_BOUND)
        .expect("maintenance ran the job's next step and the foreground write ended");
    assert!(
        foreground.is_ok(),
        "the foreground write completes: {foreground:?}"
    );
    let tick = tick.unwrap();
    assert!(
        !matches!(
            tick.outcome,
            Ok(crate::DfsMaintenanceTickOutcome::Completed { .. })
        ),
        "the foreground write cut the turn short: {:?}",
        tick.outcome
    );
    assert_ne!(
        job_status(&runtime, &a, &job.job_id),
        crate::DfsNamespaceJobStatus::Failed,
        "a cut-short step does not fail its job"
    );
    // No turn is asked for: the release of the foreground write's admission
    // resumes the job, well before any retry.
    let resumed = Instant::now();
    wait_until("the job completes once admission frees", || {
        job_status(&runtime, &a, &job.job_id) == crate::DfsNamespaceJobStatus::Completed
    });
    assert!(
        resumed.elapsed() < LANE_HELD_BY_OTHER_WORK,
        "the job resumed on the admission's release: {:?}",
        resumed.elapsed()
    );
    assert_eq!(
        client
            .posix_read_file_distributed("/users/cut-short/new/two/file.txt")
            .unwrap(),
        b"two"
    );
    drop(client);
    runtime.close().unwrap();
}

fn job_status(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    job_id: &str,
) -> crate::DfsNamespaceJobStatus {
    use crate::DfsMetadataBackend as _;
    runtime
        .node_metadata(node)
        .unwrap()
        .list_namespace_jobs()
        .unwrap()
        .into_iter()
        .find(|job| job.job_id == job_id)
        .expect("the job is persisted")
        .status
}
