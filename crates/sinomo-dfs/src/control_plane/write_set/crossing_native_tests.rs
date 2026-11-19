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

/// Well within the 45 s default deadline of a rename's adopt step: a rename
/// that ends inside it did not end by timing out and recovering.
const CROSSING_RENAME_BOUND: Duration = Duration::from_secs(10);

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

/// A rename from `from` to `to` issued on `node`, on its own thread. A
/// `Busy` refusal, a path held by another node's running move, is retried as
/// its caller would retry it, within [`CROSSING_WRITE_BOUND`].
fn routed_rename(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    from: &'static str,
    to: &'static str,
) -> mpsc::Receiver<Result<(), String>> {
    let data_dir = runtime.data_dir(node).clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let deadline = Instant::now() + CROSSING_WRITE_BOUND;
        let renamed = loop {
            let attempt = match crate::runtime_host::open_dfs_runtime_service(
                &data_dir,
                Some(crate::test_runtime::executor().handle()),
            ) {
                Ok(mut client) => client.posix_rename(from, to).map(drop),
                Err(error) => break Err(error.to_string()),
            };
            match attempt {
                Err(crate::DfsPosixError::Dfs(crate::DfsError::Busy { .. }))
                    if Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(50));
                }
                attempt => break attempt.map_err(|error| error.to_string()),
            }
        };
        let _ = sender.send(renamed);
    });
    receiver
}

/// Hold every journal batch between `a` and `b`, both ways, until released.
fn hold_journal_batches(
    runtime: &DfsTestRuntime,
    a: &NodeId,
    b: &NodeId,
) -> Arc<tokio::sync::Semaphore> {
    hold_routes(
        runtime,
        a,
        b,
        &[
            crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
            crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
        ],
    )
}

/// Hold every call on `routes` between `a` and `b`, both ways, until released.
fn hold_routes(
    runtime: &DfsTestRuntime,
    a: &NodeId,
    b: &NodeId,
    routes: &[&str],
) -> Arc<tokio::sync::Semaphore> {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    for (source, target) in [(a, b), (b, a)] {
        for route in routes {
            runtime.install_fault(crate::test_runtime::FaultRule {
                source: Some(source.clone()),
                target: Some(target.clone()),
                route: Some((*route).into()),
                call_id: None,
                origin: None,
                remaining_hits: 32,
                action: crate::test_runtime::FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
            });
        }
    }
    gate
}

/// A is primary of the root and services shards and B of users; each is a
/// member of the other's shards. A rename from services to root reaches A,
/// which may touch several shards, so A admits it on the lanes of every shard
/// it is primary of, root and services, instead of its global writer. A
/// routed write to users reaches B on its single lane. Each commit needs the
/// other node's answer, on a lane the other's request does not hold: both
/// complete while both are in flight.
#[test]
fn native_several_lane_rename_runs_beside_a_single_lane_write_and_its_answers() {
    let (runtime, service, users) =
        current_native_metadata_service("native-several-lane-rename", 3, "/users").unwrap();
    drop(service);
    let shards = runtime.metadata_shards().unwrap();
    let shard = |id: &str| {
        shards
            .iter()
            .find(|shard| shard.shard_id == id)
            .unwrap()
            .clone()
    };
    let (root, services) = (shard("root"), shard("services"));
    let a = users.primary_node_id.clone();
    assert_eq!(root.primary_node_id, a);
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
    write_settled(&runtime, &c, "/services/several-lane-source.txt");
    write_settled(&runtime, &c, "/users/several-lane-before.txt");

    runtime.checkpoint_rpc_observation_window().unwrap();
    let gate = hold_journal_batches(&runtime, &a, &b);
    let rename = routed_rename(
        &runtime,
        &c,
        "/services/several-lane-source.txt",
        "/several-lane-target.txt",
    );
    let write = routed_write(&runtime, &c, "/users/several-lane.txt");
    let held = |node: &NodeId, shard: &DfsMetadataShardConfig| {
        runtime
            .main_lane_held(node, &shard.file_system_id, &shard.shard_id)
            .unwrap()
    };
    let started = Instant::now();
    while !(held(&a, &root)
        && held(&a, &services)
        && held(&b, &users)
        && journal_send_paused(&runtime, &a, &b)
        && journal_send_paused(&runtime, &b, &a))
    {
        assert!(
            started.elapsed() < CROSSING_WRITE_BOUND,
            "A holds the rename's lanes, B the write's, and each sends a prepare"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !runtime.main_global_writer_holds(&a).unwrap()
            && !runtime.main_global_writer_holds(&b).unwrap(),
        "neither request holds its node's global writer"
    );
    assert!(
        !held(&a, &users),
        "the rename never takes a shard A only replicates"
    );
    runtime.clear_faults();
    release(&gate);

    let released = Instant::now();
    let mut outcomes = [None, None];
    while outcomes.iter().any(Option::is_none) && released.elapsed() < CROSSING_WRITE_BOUND {
        for (outcome, receiver) in outcomes.iter_mut().zip([&rename, &write]) {
            if outcome.is_none()
                && let Ok(result) = receiver.try_recv()
            {
                *outcome = Some(result);
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        outcomes.iter().all(Option::is_some),
        "the rename and the write end within {CROSSING_WRITE_BOUND:?} while both nodes \
         stay reachable: rename {:?}, write {:?}",
        outcomes[0],
        outcomes[1],
    );
    for (request, outcome) in ["rename", "write"].into_iter().zip(&outcomes) {
        let result = outcome.as_ref().unwrap();
        assert!(result.is_ok(), "the {request} commits: {result:?}");
    }
    let mut reader = runtime.client(&c).unwrap();
    assert_eq!(
        reader
            .posix_read_file_distributed("/several-lane-target.txt")
            .unwrap(),
        b"/services/several-lane-source.txt"
    );
    drop(reader);
    runtime.close().unwrap();
}

/// One rename from `from` to `to` issued on `node`, on its own thread, with
/// its typed outcome.
fn routed_rename_once(
    runtime: &DfsTestRuntime,
    node: &NodeId,
    from: &'static str,
    to: &'static str,
) -> mpsc::Receiver<Result<(), crate::DfsPosixError>> {
    let data_dir = runtime.data_dir(node).clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let renamed = crate::runtime_host::open_dfs_runtime_service(
            &data_dir,
            Some(crate::test_runtime::executor().handle()),
        )
        .map_err(|error| {
            crate::DfsPosixError::Dfs(crate::DfsError::Metadata {
                message: error.to_string(),
            })
        })
        .and_then(|mut client| client.posix_rename(from, to).map(drop));
        let _ = sender.send(renamed);
    });
    receiver
}

fn namespace_send_paused(runtime: &DfsTestRuntime, source: &NodeId, target: &NodeId) -> bool {
    runtime.rpc_observations().iter().any(|call| {
        call.source == *source
            && call.target == *target
            && call.phase == crate::test_runtime::RpcPhase::BeforeDispatch
            && call.route == crate::DFS_DATA_NAMESPACE_ROUTE
    })
}

/// A is primary of the root and services shards and B of users; each is a
/// member of the other's shards. A rename from services into users reaches A
/// and a rename from users into services reaches B. Each reads its
/// destination at the other node's primary and adopts the moved object there,
/// and each node's namespace requests to the other are held until both
/// renames have sent one, so each rename waits on the other node while both
/// are served. Both renames complete well within their adopt deadline, so
/// neither ends by timing out and recovering.
#[test]
fn native_crossing_primaries_each_rename_into_the_other() {
    let (runtime, service, users) =
        current_native_metadata_service("native-crossing-renames", 3, "/users").unwrap();
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
    write_settled(&runtime, &c, "/services/crossing-rename-a.txt");
    write_settled(&runtime, &c, "/users/crossing-rename-b.txt");

    runtime.checkpoint_rpc_observation_window().unwrap();
    let gate = hold_routes(&runtime, &a, &b, &[crate::DFS_DATA_NAMESPACE_ROUTE]);
    let into_users = routed_rename(
        &runtime,
        &c,
        "/services/crossing-rename-a.txt",
        "/users/crossing-rename-a.txt",
    );
    let into_services = routed_rename(
        &runtime,
        &c,
        "/users/crossing-rename-b.txt",
        "/services/crossing-rename-b.txt",
    );
    // Where one primary's held request keeps the other from sending its own,
    // the renames already wait on each other; the bound ends this wait.
    let started = Instant::now();
    let both_sent = loop {
        if namespace_send_paused(&runtime, &a, &b) && namespace_send_paused(&runtime, &b, &a) {
            break true;
        }
        if started.elapsed() >= CROSSING_RENAME_BOUND {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    runtime.clear_faults();
    release(&gate);

    let released = Instant::now();
    let mut outcomes = [None, None];
    while outcomes.iter().any(Option::is_none) && released.elapsed() < CROSSING_WRITE_BOUND {
        for (outcome, receiver) in outcomes.iter_mut().zip([&into_users, &into_services]) {
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
        "both renames end within {CROSSING_WRITE_BOUND:?} while both primaries stay \
         reachable: into users {:?}, into services {:?}; A's global writer holds: {:?}, B's: {:?}",
        outcomes[0],
        outcomes[1],
        runtime.main_global_writer_holds(&a),
        runtime.main_global_writer_holds(&b),
    );
    assert!(
        both_sent,
        "each rename sent a namespace request to the other primary before both were released"
    );
    for (rename, outcome) in ["into users", "into services"].into_iter().zip(&outcomes) {
        let (result, elapsed) = outcome.as_ref().unwrap();
        assert!(result.is_ok(), "the rename {rename} commits: {result:?}");
        assert!(
            *elapsed < CROSSING_RENAME_BOUND,
            "the rename {rename} ends within {CROSSING_RENAME_BOUND:?}, not by its adopt \
             deadline: {elapsed:?}"
        );
    }
    let mut reader = runtime.client(&c).unwrap();
    for (path, bytes) in [
        (
            "/users/crossing-rename-a.txt",
            "/services/crossing-rename-a.txt",
        ),
        (
            "/services/crossing-rename-b.txt",
            "/users/crossing-rename-b.txt",
        ),
    ] {
        assert_eq!(
            reader.posix_read_file_distributed(path).unwrap(),
            bytes.as_bytes(),
            "{path} holds the moved object"
        );
    }
    drop(reader);
    runtime.close().unwrap();
}

/// A as in [`native_crossing_primaries_each_rename_into_the_other`], but the
/// renames collide: A moves a file from services onto the users path that B
/// is moving into services. Each rename either commits or ends with a typed
/// refusal well within the adopt deadline, never by waiting on the other. A
/// refused move's durable job is then finished by maintenance, and the
/// namespace ends in a state some order of the two renames explains.
#[test]
fn native_crossing_renames_that_collide_on_a_path_end_without_waiting() {
    let (runtime, service, users) =
        current_native_metadata_service("native-colliding-renames", 3, "/users").unwrap();
    drop(service);
    let a = users.primary_node_id.clone();
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
    let (moved_in, collided, moved_out) = (
        "/services/collide-in.txt",
        "/users/collide.txt",
        "/services/collide-out.txt",
    );
    write_settled(&runtime, &c, moved_in);
    write_settled(&runtime, &c, collided);

    runtime.checkpoint_rpc_observation_window().unwrap();
    let gate = hold_routes(&runtime, &a, &b, &[crate::DFS_DATA_NAMESPACE_ROUTE]);
    let onto = routed_rename_once(&runtime, &c, moved_in, collided);
    let away = routed_rename_once(&runtime, &c, collided, moved_out);
    let started = Instant::now();
    while !(namespace_send_paused(&runtime, &a, &b) && namespace_send_paused(&runtime, &b, &a)) {
        assert!(
            started.elapsed() < CROSSING_WRITE_BOUND,
            "each rename sends a namespace request to the other primary"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    runtime.clear_faults();
    release(&gate);

    let released = Instant::now();
    let mut outcomes = [None, None];
    while outcomes.iter().any(Option::is_none) && released.elapsed() < CROSSING_WRITE_BOUND {
        for (outcome, receiver) in outcomes.iter_mut().zip([&onto, &away]) {
            if outcome.is_none()
                && let Ok(result) = receiver.try_recv()
            {
                *outcome = Some((result, released.elapsed()));
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    for (rename, outcome) in ["onto the moving path", "away from it"]
        .into_iter()
        .zip(&outcomes)
    {
        let (result, elapsed) = outcome
            .as_ref()
            .unwrap_or_else(|| panic!("the rename {rename} ended: {outcomes:?}"));
        assert!(
            *elapsed < CROSSING_RENAME_BOUND,
            "the rename {rename} ends within {CROSSING_RENAME_BOUND:?}: {elapsed:?} {result:?}"
        );
        assert!(
            matches!(
                result,
                Ok(())
                    | Err(crate::DfsPosixError::Dfs(crate::DfsError::Busy { .. }))
                    | Err(crate::DfsPosixError::Conflict { .. })
                    | Err(crate::DfsPosixError::NotFound { .. })
            ),
            "the rename {rename} commits or is refused with a typed error: {result:?}"
        );
    }
    // A refused rename's job stays durable; the coordinators' maintenance
    // finishes or rejects it, after which no path is held.
    let settled = Instant::now();
    let state = loop {
        let mut reader = runtime.client(&c).unwrap();
        let mut held = false;
        let mut read = |path: &str| match reader.posix_read_file_distributed(path) {
            Ok(bytes) => Some(String::from_utf8(bytes).unwrap()),
            Err(crate::DfsPosixError::NotFound { .. }) => None,
            Err(crate::DfsPosixError::Dfs(crate::DfsError::Busy { .. })) => {
                held = true;
                None
            }
            Err(error) => panic!("{path} reads: {error:?}"),
        };
        let state = (read(moved_in), read(collided), read(moved_out));
        drop(reader);
        if !held {
            break state;
        }
        assert!(
            settled.elapsed() < CROSSING_WRITE_BOUND,
            "maintenance finishes the refused move: {outcomes:?}"
        );
        for node in [&a, &b] {
            let _ = runtime.maintenance_tick(node, Duration::from_secs(5));
        }
    };
    let (onto_ok, away_ok) = (
        outcomes[0].as_ref().unwrap().0.is_ok(),
        outcomes[1].as_ref().unwrap().0.is_ok(),
    );
    // A move that committed took effect; a refused one may still have taken
    // effect through its job.
    let onto_took_effect = state.0.is_none();
    assert!(
        !onto_ok || onto_took_effect,
        "a committed move took effect: {state:?}"
    );
    let expected = match (onto_took_effect, away_ok) {
        // The move away ran first; the move onto the freed path followed.
        (true, true) => (None, Some(moved_in), Some(collided)),
        (false, true) => (Some(moved_in), None, Some(collided)),
        // The move onto the path replaced it; the move away found it gone.
        (true, false) => (None, Some(moved_in), None),
        (false, false) => (Some(moved_in), Some(collided), None),
    };
    let expected = (
        expected.0.map(str::to_string),
        expected.1.map(str::to_string),
        expected.2.map(str::to_string),
    );
    assert_eq!(state, expected, "outcomes {outcomes:?}");
    runtime.close().unwrap();
}

/// A nested shard's root is A's node files directory, owned by A, and B is
/// primary of the root shard; A is primary of services. B moves A's node
/// directory, which holds that nested root, into services while A moves a
/// file from services into the root shard, and each node's namespace
/// requests to the other are held until both have sent one. B runs the move
/// on its own shard's lane and holds no copy of the nested shard, so it reads
/// the moving directory's nested child, and the nested subtree, from A. Both
/// moves commit well within their adopt deadline and the nested subtree
/// arrives under the destination.
#[test]
fn native_directory_move_over_a_nested_shard_runs_beside_a_crossing_rename() {
    let (runtime, service, users) =
        current_native_metadata_service("native-nested-dir-move", 3, "/users").unwrap();
    drop(service);
    let a = users.primary_node_id.clone();
    let b = users.replica_node_ids[0].clone();
    let c = users.replica_node_ids[1].clone();
    let nested_root = crate::sifs_node_files_path(&a);
    let nested = runtime
        .bootstrap_empty_metadata_prefix("crossing-nested", &nested_root)
        .unwrap();
    assert_eq!(nested.primary_node_id, a);
    assert!(nested.replica_node_ids.is_empty());
    let root = runtime
        .reassign_metadata_shard(
            &users.file_system_id,
            "root",
            b.clone(),
            vec![a.clone(), c.clone()],
        )
        .unwrap();
    assert_eq!(root.primary_node_id, b);
    let services = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    assert_eq!(services.primary_node_id, a);
    let mut writer = runtime.client(&a).unwrap();
    let child = format!("{nested_root}/child.bin");
    writer
        .posix_write_file(
            &child,
            b"nested child bytes",
            crate::DfsWriteOptions {
                policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                replica_candidates: vec![a.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .unwrap();
    drop(writer);
    write_settled(&runtime, &c, "/services/nested-crossing-a.txt");

    let from: &'static str = crate::sifs_node_path(&a).leak();
    let to = "/services/nested-moved";
    runtime.checkpoint_rpc_observation_window().unwrap();
    let gate = hold_routes(&runtime, &a, &b, &[crate::DFS_DATA_NAMESPACE_ROUTE]);
    let directory_move = routed_rename(&runtime, &c, from, to);
    let crossing = routed_rename(
        &runtime,
        &c,
        "/services/nested-crossing-a.txt",
        "/nested-crossing-a.txt",
    );
    let started = Instant::now();
    let both_sent = loop {
        if namespace_send_paused(&runtime, &a, &b) && namespace_send_paused(&runtime, &b, &a) {
            break true;
        }
        if started.elapsed() >= CROSSING_RENAME_BOUND {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    runtime.clear_faults();
    release(&gate);

    let released = Instant::now();
    let mut outcomes = [None, None];
    while outcomes.iter().any(Option::is_none) && released.elapsed() < CROSSING_WRITE_BOUND {
        for (outcome, receiver) in outcomes.iter_mut().zip([&directory_move, &crossing]) {
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
        "both moves end within {CROSSING_WRITE_BOUND:?} while every node stays reachable: \
         directory move {:?}, crossing rename {:?}",
        outcomes[0],
        outcomes[1],
    );
    assert!(
        both_sent,
        "each move sent a namespace request to the other primary before both were released: \
         A to B {}, B to A {}; directory move {:?}, crossing rename {:?}",
        namespace_send_paused(&runtime, &a, &b),
        namespace_send_paused(&runtime, &b, &a),
        outcomes[0],
        outcomes[1],
    );
    for (rename, outcome) in ["directory move", "crossing rename"]
        .into_iter()
        .zip(&outcomes)
    {
        let (result, elapsed) = outcome.as_ref().unwrap();
        assert!(result.is_ok(), "the {rename} commits: {result:?}");
        assert!(
            *elapsed < CROSSING_RENAME_BOUND,
            "the {rename} ends within {CROSSING_RENAME_BOUND:?}, not by its adopt deadline: \
             {elapsed:?}"
        );
    }
    let mut reader = runtime.client(&c).unwrap();
    assert_eq!(
        reader
            .posix_read_file_distributed(&format!("{to}/files/child.bin"))
            .unwrap(),
        b"nested child bytes"
    );
    assert_eq!(
        reader
            .posix_read_file_distributed("/nested-crossing-a.txt")
            .unwrap(),
        b"/services/nested-crossing-a.txt"
    );
    assert!(
        matches!(
            reader.posix_stat_distributed(&child),
            Err(crate::DfsPosixError::NotFound { .. })
        ),
        "the nested child left its source"
    );
    drop(reader);
    runtime.close().unwrap();
}
