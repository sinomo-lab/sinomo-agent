//! Two primaries in each other's write sets on the native test runtime: each
//! serves a routed content write whose commit needs the other's answer.

use super::commit_native_tests::{cut, members, release, submit};
use super::*;
use crate::test_support::{DfsTestRuntime, current_native_metadata_service};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Far above the 5 s object-RPC idle timeout, the only bound a production
/// send could meet. While every member stays reachable, nothing else ends a
/// served commit's wait for an acknowledgement.
const CROSSING_WRITE_BOUND: Duration = Duration::from_secs(30);

fn switch_to_all_members(runtime: &DfsTestRuntime, config: &DfsMetadataShardConfig) {
    let all = members(config).into_iter().collect();
    let switch = build_metadata_write_set_switch(&cut(runtime, config), &all).unwrap();
    submit(runtime, config, &switch);
}

/// One routed content write from `from` to `path`, on its own thread.
fn routed_write(
    runtime: &DfsTestRuntime,
    from: &NodeId,
    path: &'static str,
) -> mpsc::Receiver<Result<(), String>> {
    let data_dir = runtime.data_dir(from).clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let written = match crate::runtime_host::open_dfs_runtime_service(
            &data_dir,
            Some(crate::test_runtime::executor().handle()),
        ) {
            Ok(mut client) => client
                .posix_write_file(
                    path,
                    path.as_bytes(),
                    crate::DfsWriteOptions::replicated_eventual(Vec::new()),
                )
                .map(drop)
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        let _ = sender.send(written);
    });
    receiver
}

fn write_settled(runtime: &DfsTestRuntime, from: &NodeId, path: &'static str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match routed_write(runtime, from, path).recv().unwrap() {
            Ok(()) => return,
            Err(error) => {
                assert!(Instant::now() < deadline, "{path} was not written: {error}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

fn journal_send_paused(runtime: &DfsTestRuntime, source: &NodeId, target: &NodeId) -> bool {
    runtime.rpc_observations().iter().any(|call| {
        call.source == *source
            && call.target == *target
            && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            && [
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
            ]
            .contains(&call.route.as_str())
    })
}

/// A is the services primary and B the users primary; each is a member of
/// the other's write set. A routed write to each shard reaches its primary,
/// and each primary's prepare is held on its way to the other until both are
/// in flight. Each primary then serves its write while the other's answer to
/// it arrives. Both writes commit within a bound far above anything that
/// could end the wait on its own while both nodes stay reachable.
#[test]
fn native_crossing_primaries_each_answer_the_other_while_serving_a_routed_write() {
    let (runtime, service, users) =
        current_native_metadata_service("native-crossing-routed-writes", 3, "/users").unwrap();
    drop(service);
    let services = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    let a = users.primary_node_id.clone();
    assert_eq!(services.primary_node_id, a);
    let b = users.replica_node_ids[0].clone();
    let c = users.replica_node_ids[1].clone();
    let users = runtime
        .reassign_metadata_shard(
            &users.file_system_id,
            "users",
            b.clone(),
            vec![a.clone(), c.clone()],
        )
        .unwrap();
    assert_eq!(users.primary_node_id, b);
    for node in members(&users) {
        runtime
            .advertise_metadata_write_set_capability(&node)
            .unwrap();
    }
    switch_to_all_members(&runtime, &services);
    switch_to_all_members(&runtime, &users);
    // Each primary has served a routed write and answered the other's once.
    write_settled(&runtime, &c, "/services/crossing-before.txt");
    write_settled(&runtime, &c, "/users/crossing-before.txt");

    runtime.checkpoint_rpc_observation_window().unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for (source, target) in [(&a, &b), (&b, &a)] {
        for route in [
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        ] {
            runtime.install_fault(crate::test_runtime::FaultRule {
                source: Some(source.clone()),
                target: Some(target.clone()),
                route: Some(route.into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
            });
        }
    }
    let to_services = routed_write(&runtime, &c, "/services/crossing.txt");
    let to_users = routed_write(&runtime, &c, "/users/crossing.txt");
    // Neither write can commit while its prepare to the other is held, so
    // once both primaries hold their write's admission both are in flight.
    let serving = |primary: &NodeId, shard: &DfsMetadataShardConfig| {
        runtime
            .main_lane_held(primary, &shard.file_system_id, &shard.shard_id)
            .unwrap()
    };
    let started = Instant::now();
    while !(serving(&a, &services)
        && serving(&b, &users)
        && journal_send_paused(&runtime, &a, &b)
        && journal_send_paused(&runtime, &b, &a))
    {
        assert!(
            started.elapsed() < CROSSING_WRITE_BOUND,
            "both primaries serve their write and send a prepare to the other"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    runtime.clear_faults();
    release(&gate);

    let released = Instant::now();
    let mut outcomes = [None, None];
    while outcomes.iter().any(Option::is_none) && released.elapsed() < CROSSING_WRITE_BOUND {
        for (outcome, receiver) in outcomes.iter_mut().zip([&to_services, &to_users]) {
            if outcome.is_none()
                && let Ok(result) = receiver.try_recv()
            {
                *outcome = Some((result, released.elapsed()));
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        outcomes.iter().all(Option::is_some),
        "both routed writes end within {CROSSING_WRITE_BOUND:?} while both primaries stay \
         reachable: services {:?}, users {:?}; A's global writer holds: {:?}, B's: {:?}",
        outcomes[0],
        outcomes[1],
        runtime.main_global_writer_holds(&a),
        runtime.main_global_writer_holds(&b),
    );
    for (shard, outcome) in ["services", "users"].into_iter().zip(&outcomes) {
        let (result, _) = outcome.as_ref().unwrap();
        assert!(result.is_ok(), "the {shard} write commits: {result:?}");
    }
    runtime.close().unwrap();
}
