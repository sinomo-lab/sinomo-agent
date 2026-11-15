//! A write that committed but whose post-commit observation fails.

use super::*;
use crate::test_runtime::{FaultAction, FaultRule, Partition, RpcPhase};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The write commits at the shard primary, and then the writer is cut off
/// from it before it reads the committed result back. The write reports a
/// possibly applied delivery, never one that sent nothing, so a caller does
/// not retry it under a new operation id; the change is committed.
#[test]
fn native_committed_write_never_reports_definitely_not_sent() {
    let name = "native-committed-write-observation";
    let (runtime, service, config) =
        crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
    drop(service);
    let primary = config.primary_node_id.clone();
    let writer = config.replica_node_ids[0].clone();
    let path = "/services/committed-observation.txt";
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(writer.clone()),
        target: Some(primary.clone()),
        route: Some(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
    });
    let mut client = runtime.client(&writer).unwrap();
    let candidates = vec![primary.clone()];
    let write = std::thread::spawn(move || {
        client.posix_write_file(
            path,
            b"committed observation",
            crate::DfsWriteOptions::replicated_eventual(candidates),
        )
    });
    // The primary has committed and its reply is held.
    let deadline = Instant::now() + Duration::from_secs(90);
    while !runtime.rpc_observations().iter().any(|event| {
        event.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
            && event.source == writer
            && event.target == primary
            && event.phase == RpcPhase::AfterDispatch
    }) {
        assert!(
            Instant::now() < deadline,
            "the write never reached the primary"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    runtime.partition_node(
        &writer,
        Partition {
            kv: false,
            dfs: true,
        },
    );
    gate.add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
    let written = write.join().unwrap();
    eprintln!("committed write reported {written:?}");
    match &written {
        Ok(_) => {}
        Err(DfsPosixError::Dfs(DfsError::NetworkDelivery { delivery_state, .. })) => {
            assert_eq!(
                *delivery_state,
                sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                "{written:?}"
            );
        }
        Err(other) => panic!("a committed write fails only as possibly applied: {other:?}"),
    }
    runtime.partition_node(&writer, Partition::default());
    let committed = runtime
        .metadata_primary_service("/services")
        .unwrap()
        .lookup_sifs_path(path)
        .unwrap()
        .expect("the write committed at the primary");
    assert_eq!(
        committed.content_size_bytes,
        Some(b"committed observation".len() as u64)
    );
    runtime.clear_faults();
    runtime.close().unwrap();
}
