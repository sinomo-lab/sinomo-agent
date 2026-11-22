//! A hosted browser read is a served request: its client's budget bounds its
//! work, and its client's departure ends that work.
use super::write_options_tests::native_browser_runtime;
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule};
use sinomo_core::network::ObjectRpcCall;
use sinomo_core::node::NodeId;

const PATH: &str = "/documents/served-read.txt";
/// Well inside the 30 s bound a metadata query keeps while its service holds
/// admission, so the departure, not that bound, is what ends the read.
const PROMPT: Duration = Duration::from_secs(10);

fn stat_request() -> ObjectRpcRequest {
    ObjectRpcRequest::encode_body(
        DAEMON_DFS_BROWSER_STAT_ROUTE,
        DAEMON_RPC_CATEGORY,
        &crate::DaemonDfsBrowserStatRequest {
            path: PATH.into(),
            max_staleness_millis: 0,
        },
    )
    .unwrap()
}

/// Call the caller's own hosted browser route; the client gives up at
/// `client_timeout`.
fn call(
    native: &DfsTestRuntime,
    caller: &NodeId,
    request: ObjectRpcRequest,
    client_timeout: Duration,
) -> Result<crate::DaemonDfsBrowserStatOutcomeResponse, String> {
    let future = native.rpc_result_future(
        caller,
        caller,
        ObjectRpcCall::value(request),
        client_timeout,
    );
    crate::test_runtime::executor().block_on(async move {
        future
            .await?
            .into_materialized_value_response(16 * 1024 * 1024)
            .await
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())
    })
}

#[test]
fn a_browser_read_ends_at_its_clients_budget_and_when_its_client_leaves() {
    let (native, nodes) = native_browser_runtime("browser-served-read", 2);
    let primary = native
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap()
        .primary_node_id;
    let caller = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut writer = native.client(&primary).unwrap();
    writer.posix_mkdir_all("/documents").unwrap();
    writer
        .posix_write_file(
            PATH,
            b"served read",
            crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    drop(writer);
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::new(native.data_dir(&caller).clone()),
        &native.runtime_handle(),
    )
    .unwrap();
    assert!(
        call(&native, &caller, stat_request(), PROMPT)
            .unwrap()
            .into_result()
            .is_ok(),
        "the read is answered while its primary answers"
    );

    // The primary's answers are held for as long as the test says.
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    native.install_fault(FaultRule {
        source: Some(caller.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_NAMESPACE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });

    // A client's budget bounds the read's work.
    let budget = Duration::from_secs(1);
    let started = Instant::now();
    let budgeted = call(
        &native,
        &caller,
        crate::routes::with_dfs_request_budget(stat_request(), budget),
        PROMPT * 3,
    );
    let elapsed = started.elapsed();
    assert!(
        !matches!(&budgeted, Ok(outcome) if outcome.clone().into_result().is_ok()),
        "a read whose primary is held does not succeed: {budgeted:?}"
    );
    assert!(
        elapsed >= budget && elapsed < budget + PROMPT,
        "the read ended at its client's budget: {elapsed:?}"
    );

    // Without a budget the read waits; its client's departure ends it.
    let left = call(&native, &caller, stat_request(), Duration::from_secs(1));
    assert!(left.is_err(), "the client gave up: {left:?}");
    let departed = Instant::now();
    let (freed_tx, freed_rx) = std::sync::mpsc::channel();
    let lock_backend = Arc::clone(&backend);
    std::thread::spawn(move || {
        let _service = lock_backend.service.lock().unwrap();
        let _ = freed_tx.send(Instant::now());
    });
    let freed = freed_rx
        .recv_timeout(PROMPT)
        .expect("the read's work ended when its client left");
    assert!(freed.duration_since(departed) < PROMPT);

    native.remove_faults_matching(|rule| {
        matches!(&rule.action, FaultAction::PauseBeforeDispatch(held)
            if Arc::ptr_eq(held, &gate))
    });
    gate.add_permits(32);
    drop(backend);
    native.close().unwrap();
}
