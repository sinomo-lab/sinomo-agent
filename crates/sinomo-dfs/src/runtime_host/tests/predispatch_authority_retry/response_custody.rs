//! Original private requesters observe completion from normal resident ingress.
use super::*;
use crate::service_write::{DfsDistributedWriteOptions, DfsWriteRepair};
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};

const PAYLOAD: &[u8] = b"private completion belongs to the original native requester";

fn run_private_requester_custody(terminal_before_drain: bool) {
    let label = if terminal_before_drain {
        "native-private-terminal-first"
    } else {
        "native-private-pending-first"
    };
    let nodes = DfsTestRuntime::ordered_node_ids(label, 3);
    let mut runtime = DfsTestRuntime::new(label, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        runtime.define_node(node.clone()).unwrap();
    }
    runtime.start_nodes().unwrap();
    let shard = runtime.root_shard().unwrap();
    let primary = shard.primary_node_id.clone();
    let requester_node = nodes.iter().find(|node| **node != primary).unwrap().clone();
    let mut primary_view = runtime.client(&primary).unwrap();
    let original = primary_view
        .posix_write_file(
            "/private-custody",
            b"original data",
            crate::DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
        )
        .unwrap();
    let object = primary_view
        .metadata_backend()
        .unwrap()
        .get_object(&original.object_id)
        .unwrap()
        .unwrap();
    let target = DfsVersion::generate();
    let mut private = runtime.client(&requester_node).unwrap();
    private
        .content
        .put_object_version(object.object_id.as_str(), target.as_str(), PAYLOAD)
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    runtime.install_fault(FaultRule {
        source: Some(primary.clone()),
        target: Some(requester_node.clone()),
        route: Some(crate::DFS_DATA_FETCH_BLOB_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 32,
        action: FaultAction::PauseBeforeDispatch(gate.clone()),
    });
    let operation = sinomo_core::Id::generate().to_string();
    let baseline = runtime.rpc_observations().len();
    let initial = private
        .route_distributed_manifest(
            sinomo_core::DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version: target.clone(),
                requesting_node_id: requester_node.clone(),
            },
            Some(object.version.clone()),
            crate::content_hash(PAYLOAD),
            PAYLOAD.len() as u64,
            vec![primary.clone()],
            crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            object,
            shard,
            DfsDistributedWriteOptions {
                operation_id: Some(operation.clone()),
                repair: DfsWriteRepair::Background,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        initial,
        crate::NetworkedDfsWriteOutcome::Pending { .. }
    ));
    if !terminal_before_drain {
        private
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        assert!(matches!(
            private.write_outcome(&operation).unwrap(),
            crate::NetworkedDfsWriteOutcome::Pending { .. }
        ));
    }
    runtime.clear_faults();
    gate.add_permits(64);
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        let actual = primary_view
            .metadata_backend()
            .unwrap()
            .get_object(&original.object_id)
            .unwrap()
            .unwrap();
        if actual.version == target {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "actual primary fetch and guarded publication must complete"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    loop {
        private
            .process_incoming_without_deferred_parent_touches()
            .unwrap();
        if matches!(private.write_outcome(&operation).unwrap(), crate::NetworkedDfsWriteOutcome::Committed { ref version, .. } if version == &target)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "original private requester must adopt its native resident completion"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let dispatches = runtime
        .rpc_observations()
        .iter()
        .skip(baseline)
        .filter(|event| {
            event.source == requester_node
                && event.target == primary
                && event.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && event.phase == RpcPhase::BeforeDispatch
        })
        .count();
    assert_eq!(
        dispatches, 1,
        "completion never replays the original mutation"
    );
    assert_eq!(
        private.posix_read_file("/private-custody").unwrap(),
        PAYLOAD
    );
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop((private, primary_view));
    runtime
        .close()
        .expect("all normal owned work and receivers drained");
}

#[test]
fn native_private_requester_adopts_resident_terminal_after_inline_pending() {
    run_private_requester_custody(false);
}

#[test]
fn native_private_requester_adopts_resident_terminal_before_inline_pending() {
    run_private_requester_custody(true);
}
