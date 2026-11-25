//! A content write retried under its operation id after earlier attempts of
//! it, or of another write to the same object, were cancelled while their
//! prepares could not commit, on the native test runtime. The retry is the
//! same write: it commits once.

use super::commit_native_tests::{hold_replies, release, switched};
use super::*;
use crate::test_runtime::executor;
use crate::test_support::DfsTestRuntime;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// The bound on every wait a test makes for an event.
const EVENT_BOUND: Duration = Duration::from_secs(90);

/// One copy, on the primary: a commit waits only for its write set.
fn on_the_primary(config: &DfsMetadataShardConfig) -> crate::DfsWriteOptions {
    crate::DfsWriteOptions {
        policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
        replica_candidates: vec![config.primary_node_id.clone()],
        wait_for_commit: true,
        foreground_replica_repair: false,
    }
}

/// One content write from `from`'s client under `operation`, bounded only by
/// `budget` when one is given.
fn write(
    runtime: &DfsTestRuntime,
    options: crate::DfsWriteOptions,
    from: &NodeId,
    path: &'static str,
    operation: &'static str,
    budget: Option<Duration>,
) -> Result<crate::DfsStat, crate::DfsPosixError> {
    let data_dir = runtime.data_dir(from).clone();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut client =
            crate::runtime_host::open_dfs_runtime_service(&data_dir, Some(executor().handle()))
                .expect("the requester opens its client");
        if let Some(budget) = budget {
            client.set_transport_operation_deadline(Some(Instant::now() + budget));
        }
        let _ = sender.send(client.posix_write_file_with_operation_id(
            path,
            operation.as_bytes(),
            options,
            operation,
        ));
    });
    receiver
        .recv_timeout(EVENT_BOUND)
        .unwrap_or_else(|_| panic!("{operation} is answered"))
}

/// The file at `path` as `reader`'s client reads it: its stat and bytes.
fn read(runtime: &DfsTestRuntime, reader: &NodeId, path: &str) -> (crate::DfsStat, Vec<u8>) {
    let mut client = runtime.client(reader).unwrap();
    (
        client.posix_stat_distributed(path).unwrap(),
        client.posix_read_file_distributed(path).unwrap(),
    )
}

/// A write's attempts end at their requester's budget while a member's
/// answers are held, so the prepare an attempt left cannot commit; a later
/// attempt meets that prepare still unresolved. Once the member answers,
/// the same operation id commits the write once.
#[test]
fn native_cancelled_attempts_of_one_write_commit_it_once() {
    let (runtime, config) = switched("native-identity-cancelled", None);
    let held = config.replica_node_ids[0].clone();
    let requester = config.replica_node_ids[1].clone();
    let options = on_the_primary(&config);
    let path = "/services/identity-cancelled.txt";
    let operation = "identity-cancelled";
    write(
        &runtime,
        options.clone(),
        &requester,
        "/services/identity-before.txt",
        "identity-before",
        None,
    )
    .expect("a write commits while every member answers");
    let gate = hold_replies(&runtime, &config, &held);
    for attempt in 0..2 {
        let cancelled = write(
            &runtime,
            options.clone(),
            &requester,
            path,
            operation,
            Some(Duration::from_secs(3)),
        );
        assert!(
            cancelled.is_err(),
            "attempt {attempt} ends at its budget: {cancelled:?}"
        );
    }
    release(&gate);
    runtime.clear_faults();
    let committed = write(&runtime, options.clone(), &requester, path, operation, None)
        .expect("the same operation id commits the write");
    let again = write(&runtime, options, &requester, path, operation, None)
        .expect("the same operation id is answered from its commit");
    assert_eq!(again.version, committed.version);
    let (stat, bytes) = read(&runtime, &requester, path);
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(&requester, &stat.object_id, operation)
    );
    assert_eq!(bytes, operation.as_bytes());
    runtime.close().unwrap();
}

/// Two services of the primary write the same file under the same operation
/// id. The second commits the write between the first's read of the write's
/// row and the first's publication of its opening, which the native tables
/// then refuse to reopen: the first is answered from that commit, and the
/// write commits once.
#[test]
fn native_same_id_write_meeting_its_commit_is_answered_from_it() {
    let (runtime, config) = switched("native-identity-raced", None);
    let primary = config.primary_node_id.clone();
    let mut options = on_the_primary(&config);
    // An opening of its own before the commit.
    options.foreground_replica_repair = true;
    let path = "/services/identity-raced.txt";
    let operation = "identity-raced";
    write(
        &runtime,
        options.clone(),
        &primary,
        path,
        "identity-raced-base",
        None,
    )
    .expect("the file exists");
    let open = |runtime: &DfsTestRuntime| {
        crate::runtime_host::open_dfs_runtime_service_on_shard(
            runtime.data_dir(&primary),
            Some(executor().handle()),
            &config,
        )
        .unwrap()
    };
    let mut first = open(&runtime);
    let object_id = first.posix_stat_distributed(path).unwrap().object_id;
    let intent_id = crate::intent::stable_write_intent_id(&primary, &object_id, operation);
    let second = std::sync::Arc::new(std::sync::Mutex::new(Some(open(&runtime))));
    let raced = std::sync::Arc::new(std::sync::Mutex::new(None));
    crate::service_commit::opening_races::before_opening_of(&intent_id, {
        let second = std::sync::Arc::clone(&second);
        let raced = std::sync::Arc::clone(&raced);
        let options = options.clone();
        move || {
            let mut second = second.lock().unwrap().take().unwrap();
            *raced.lock().unwrap() = Some(second.posix_write_file_with_operation_id(
                path,
                operation.as_bytes(),
                options,
                operation,
            ));
        }
    });
    let answered = first
        .posix_write_file_with_operation_id(path, operation.as_bytes(), options, operation)
        .expect("the same operation id is answered from its commit");
    let committed = raced
        .lock()
        .unwrap()
        .take()
        .expect("the other path ran")
        .expect("the other path commits the write");
    assert_eq!(answered.version, committed.version);
    drop(first);
    let (stat, bytes) = read(&runtime, &primary, path);
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(&primary, &object_id, operation)
    );
    assert_eq!(bytes, operation.as_bytes());
    runtime.close().unwrap();
}

/// A write to an object is cancelled while its prepare cannot commit, and a
/// second write to the same object, stacked behind that prepare, is
/// cancelled as well. Once the member answers, each write's same operation
/// id commits it once, in the order the callers retry them.
#[test]
fn native_write_stacked_behind_a_cancelled_write_commits_once() {
    let (runtime, config) = switched("native-identity-stacked", None);
    let held = config.replica_node_ids[0].clone();
    let requester = config.replica_node_ids[1].clone();
    let options = on_the_primary(&config);
    let path = "/services/identity-stacked.txt";
    write(
        &runtime,
        options.clone(),
        &requester,
        path,
        "identity-stacked-base",
        None,
    )
    .expect("the object exists");
    let gate = hold_replies(&runtime, &config, &held);
    for operation in ["identity-stacked-a", "identity-stacked-b"] {
        let cancelled = write(
            &runtime,
            options.clone(),
            &requester,
            path,
            operation,
            Some(Duration::from_secs(3)),
        );
        assert!(
            cancelled.is_err(),
            "{operation} ends at its budget: {cancelled:?}"
        );
    }
    release(&gate);
    runtime.clear_faults();
    let a = write(
        &runtime,
        options.clone(),
        &requester,
        path,
        "identity-stacked-a",
        None,
    )
    .expect("the first write commits under its operation id");
    let b = write(
        &runtime,
        options.clone(),
        &requester,
        path,
        "identity-stacked-b",
        None,
    )
    .expect("the stacked write commits under its operation id");
    assert_ne!(a.version, b.version);
    let (stat, bytes) = read(&runtime, &requester, path);
    assert_eq!(
        stat.version,
        crate::intent::stable_write_target_version(
            &requester,
            &stat.object_id,
            "identity-stacked-b"
        )
    );
    assert_eq!(bytes, b"identity-stacked-b");
    let again = write(
        &runtime,
        options,
        &requester,
        path,
        "identity-stacked-b",
        None,
    )
    .expect("the stacked write's id is answered from its commit");
    assert_eq!(again.version, b.version);
    runtime.close().unwrap();
}
