//! Opened-root confinement through the actual split native authorities.
use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule, RpcPhase};
use crate::*;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const SCOPE: &str = "/services/opened-scope-native/cap";
const PARENT: &str = "/services/opened-scope-native/cap/level/mid/parent";
const PATH: &str = "/services/opened-scope-native/cap/level/mid/parent/note.md";
const BYTES: &[u8] = b"actual opened Services scope content";

struct GateCleanup(Arc<tokio::sync::Semaphore>);
impl Drop for GateCleanup {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn split(
    name: &str,
) -> (
    DfsTestRuntime,
    Vec<sinomo_core::node::NodeId>,
    sinomo_core::node::NodeId,
    sinomo_core::node::NodeId,
) {
    let (native, initial, _) =
        crate::test_support::current_native_metadata_service(name, 3, PATH).unwrap();
    drop(initial);
    let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
    let catalog = native.metadata_shards().unwrap();
    let root = catalog
        .iter()
        .find(|shard| shard.shard_id == "root")
        .unwrap();
    let target = nodes
        .iter()
        .find(|node| **node != root.primary_node_id)
        .unwrap()
        .clone();
    let services = catalog
        .iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap();
    if services.primary_node_id != target {
        native
            .reassign_metadata_shard(
                &services.file_system_id,
                &services.shard_id,
                target.clone(),
                nodes
                    .iter()
                    .filter(|node| **node != target)
                    .cloned()
                    .collect(),
            )
            .unwrap();
    }
    let caller = nodes
        .iter()
        .find(|node| **node != root.primary_node_id && **node != target)
        .unwrap()
        .clone();
    assert_ne!(caller, target);
    let mut setup = native.client(&caller).unwrap();
    setup
        .posix_mkdir_all_beneath_no_links("/services", PARENT)
        .unwrap();
    drop(setup);
    (native, nodes, target, caller)
}

fn options(nodes: &[sinomo_core::node::NodeId]) -> DfsWriteOptions {
    DfsWriteOptions {
        policy: DfsReplicationPolicy::new(2, 3).unwrap(),
        replica_candidates: nodes.to_vec(),
        wait_for_commit: true,
        foreground_replica_repair: true,
    }
}

fn evidence(
    native: &DfsTestRuntime,
    caller: &sinomo_core::node::NodeId,
    label: &str,
    facts: serde_json::Value,
) {
    let records = native.checkpoint_rpc_observation_window().unwrap();
    if records > 0 {
        native
            .export_latest_rpc_observation_checkpoint(
                &native
                    .data_dir(caller)
                    .root()
                    .join(format!("opened-scope-{label}-rpc.json")),
            )
            .unwrap();
    }
    std::fs::write(
        native
            .data_dir(caller)
            .root()
            .join(format!("opened-scope-{label}.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"checkpointed_rpc_records":records,"facts":facts}),
        )
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn opened_services_scope_preserves_remote_create_replace_and_absolute_boundary() {
    let (native, nodes, primary, caller) = split("native-opened-services-scope");
    let mut client = native.client(&caller).unwrap();
    let created = client
        .posix_create_file_beneath_no_links(SCOPE, PATH, BYTES, options(&nodes))
        .unwrap();
    let mut owner = native.metadata_primary_service(PATH).unwrap();
    assert_eq!(owner.node_id(), &primary);
    let original = owner
        .metadata_backend()
        .unwrap()
        .intents_for_object(&created.object_id)
        .unwrap()
        .into_iter()
        .find(|record| record.target_version == created.version)
        .unwrap();
    let C::OpenedScope {
        root,
        directories,
        target,
        ..
    } = original.publication.current_precondition().unwrap()
    else {
        panic!("new rooted APIs must retain their opened-scope publication");
    };
    assert_eq!(root, SCOPE);
    assert_eq!(directories.first().unwrap().path, SCOPE);
    assert_eq!(directories.last().unwrap().path, PARENT);
    assert!(
        directories
            .iter()
            .all(|directory| directory.path.starts_with(SCOPE))
    );
    assert!(matches!(target, Target::Absent {}));
    let prepared = owner
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&format!("write:{}", original.intent_id))
        .unwrap()
        .unwrap();
    assert_eq!(prepared.status, DfsPreparedOperationStatus::Committed);
    assert!(
        prepared
            .expected_path_occupants
            .iter()
            .all(|entry| !entry.path.is_empty()),
        "Services effect must not carry an absolute Root comparison"
    );
    let replaced = client
        .posix_replace_file_if_version_beneath_no_links(
            SCOPE,
            PATH,
            &created.object_id,
            &created.version,
            b"actual replacement",
            options(&nodes),
        )
        .unwrap();
    assert_ne!(replaced.version, created.version);
    let read = client
        .posix_read_file_range_beneath_no_links_distributed_outcome(SCOPE, PATH, 0, 1024)
        .unwrap();
    let DfsPosixDistributedOpenedFileRangeReadOutcome::Found { stat, read } = read else {
        panic!("new Services scope must read the actual replacement");
    };
    assert_eq!(stat.object_id, created.object_id);
    assert_eq!(stat.version, replaced.version);
    assert_eq!(read.bytes, b"actual replacement");
    let absolute = client.posix_create_file_beneath_no_links(
        "/",
        "/services/opened-scope-global-boundary.md",
        BYTES,
        options(&nodes),
    );
    evidence(
        &native,
        &caller,
        "services-positive",
        serde_json::json!({
            "original":original,"exact_preparation":prepared,"created":format!("{created:?}"),
            "replaced":format!("{replaced:?}"),"absolute_root_result":format!("{absolute:?}"),
            "physical_services_primary":primary,
            "qualification":"actual split native owner and W2/N3 content; source DATA includes OpenedScope for old-reader whole-store compatibility; true absolute full-walk closure is a separate required boundary"
        }),
    );
    assert!(matches!(
        absolute,
        Err(DfsPosixError::Dfs(
            DfsError::UnsupportedWritePrecondition { .. }
        ))
    ));
    assert!(
        owner
            .metadata_backend()
            .unwrap()
            .get_object_by_path(
                &sifs_file_system_id(),
                "/services/opened-scope-global-boundary.md"
            )
            .unwrap()
            .is_none()
    );
    drop(owner);
    drop(client);
    native.close().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Change {
    RootReplacement,
    IntermediateMode,
    OutsideRootMode,
    OutsideAncestorLink,
}

fn race(name: &str, change: Change) {
    let (native, nodes, primary, caller) = split(name);
    const REPLACEMENT_ROOT: &str = "/services/opened-scope-replacement";
    if matches!(change, Change::RootReplacement) {
        let mut actor = native.metadata_primary_service(SCOPE).unwrap();
        actor
            .posix_mkdir_all_beneath_no_links(
                "/services",
                "/services/opened-scope-replacement/level/mid/parent",
            )
            .unwrap();
    }
    let mut client = native.client(&caller).unwrap();
    let root_before = client
        .posix_open_beneath_no_links_distributed(SCOPE, SCOPE)
        .unwrap();
    let parent_before = client
        .posix_open_beneath_no_links_distributed(SCOPE, PARENT)
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    native.install_fault(FaultRule {
        source: Some(caller.clone()),
        target: Some(primary.clone()),
        route: Some(DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into()),
        call_id: None,
        origin: None,
        remaining_hits: 1,
        action: FaultAction::PauseBeforeDispatch(Arc::clone(&gate)),
    });
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    client.set_transport_operation_deadline(Some(deadline));
    let outcome = Arc::new(Mutex::new(None));
    let captured_outcome = Arc::clone(&outcome);
    let write_options = options(&nodes);
    let (result, completed_at) = std::thread::scope(|scope| {
        let _cleanup = GateCleanup(Arc::clone(&gate));
        let worker = scope.spawn(move || {
            let result =
                client.posix_create_file_beneath_no_links(SCOPE, PATH, BYTES, write_options);
            let completed_at = Instant::now();
            *captured_outcome.lock().unwrap() = Some(format!("{result:#?}"));
            (result, completed_at)
        });
        let entered = loop {
            let entered = native.rpc_observations().iter().any(|call| {
                call.source == caller
                    && call.target == primary
                    && call.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                    && call.phase == RpcPhase::BeforeDispatch
            });
            if entered || Instant::now() >= deadline {
                break entered;
            }
            std::thread::yield_now();
        };
        evidence(
            &native,
            &caller,
            "before-race",
            serde_json::json!({
                "change":format!("{change:?}"),"actual_original_entered":entered,
                "actual_worker_outcome":*outcome.lock().unwrap(),"root_before":format!("{root_before:?}"),
                "parent_before":format!("{parent_before:?}")
            }),
        );
        assert!(
            entered,
            "the original real content carrier must pause after its opened walk"
        );
        match change {
            Change::RootReplacement => {
                let mut actor = native.metadata_primary_service(SCOPE).unwrap();
                actor
                    .posix_rename(SCOPE, "/services/retired-opened-scope")
                    .unwrap();
                actor.posix_rename(REPLACEMENT_ROOT, SCOPE).unwrap();
                let changed = actor.posix_stat_distributed(SCOPE).unwrap();
                assert_ne!(
                    (changed.object_id, changed.version),
                    (root_before.object_id.clone(), root_before.version.clone())
                );
            }
            Change::IntermediateMode => {
                let mut actor = native.metadata_primary_service(SCOPE).unwrap();
                let intermediate = "/services/opened-scope-native/cap/level/mid";
                let previous = actor.posix_stat_distributed(intermediate).unwrap();
                let changed = actor
                    .posix_set_mode(intermediate, previous.mode ^ 0o010)
                    .unwrap();
                assert_ne!(changed.version, previous.version);
                let root_after = actor.posix_stat_distributed(SCOPE).unwrap();
                let parent_after = actor.posix_stat_distributed(PARENT).unwrap();
                assert_eq!(
                    (root_after.object_id, root_after.version),
                    (root_before.object_id.clone(), root_before.version.clone())
                );
                assert_eq!(
                    (parent_after.object_id, parent_after.version),
                    (
                        parent_before.object_id.clone(),
                        parent_before.version.clone()
                    )
                );
            }
            Change::OutsideRootMode => {
                let mut actor = native.metadata_primary_service("/").unwrap();
                let previous = actor.posix_stat_distributed("/").unwrap();
                let changed = actor.posix_set_mode("/", previous.mode ^ 0o010).unwrap();
                assert_ne!(changed.version, previous.version);
            }
            Change::OutsideAncestorLink => {
                let mut actor = native.metadata_primary_service(SCOPE).unwrap();
                actor
                    .posix_mkdir_all("/services/opened-scope-outside/cap/level/mid/parent")
                    .unwrap();
                actor
                    .posix_rename(
                        "/services/opened-scope-native",
                        "/services/retired-opened-outer",
                    )
                    .unwrap();
                actor
                    .posix_symlink(
                        "/services/opened-scope-outside",
                        "/services/opened-scope-native",
                        options(&nodes),
                    )
                    .unwrap();
            }
        }
        gate.add_permits(1);
        worker.join().unwrap()
    });
    native.clear_faults();
    let mut owner = native.metadata_primary_service(PATH).unwrap();
    let actual = owner
        .metadata_backend()
        .unwrap()
        .get_object_by_path(&sifs_file_system_id(), PATH)
        .unwrap();
    evidence(
        &native,
        &caller,
        "after-race",
        serde_json::json!({
            "change":format!("{change:?}"),"actual_result":format!("{result:#?}"),"actual_target":actual,
            "original_deadline_met":completed_at < deadline,
            "qualification":"same original request, no mutation replay; opened-root and every in-scope generation remain fenced, while an outside Root mode is not re-resolved into the effect"
        }),
    );
    match change {
        Change::RootReplacement => {
            assert!(
                matches!(result, Err(DfsPosixError::Conflict { ref path, .. }) if path == SCOPE)
            );
            assert!(actual.is_none());
            assert!(
                owner
                    .metadata_backend()
                    .unwrap()
                    .get_object_by_path(
                        &sifs_file_system_id(),
                        "/services/retired-opened-scope/level/mid/parent/note.md"
                    )
                    .unwrap()
                    .is_none()
            );
        }
        Change::IntermediateMode => {
            assert!(
                matches!(result, Err(DfsPosixError::Conflict { ref path, .. })
                if path == "/services/opened-scope-native/cap/level/mid")
            );
            assert!(actual.is_none());
        }
        Change::OutsideRootMode => {
            let created = result.unwrap();
            let actual = actual.unwrap();
            assert_eq!(actual.object_id, created.object_id);
            assert_eq!(owner.posix_read_file_distributed(PATH).unwrap(), BYTES);
        }
        Change::OutsideAncestorLink => {
            assert!(matches!(result, Err(DfsPosixError::Conflict { .. })));
            assert!(actual.is_none());
            for path in [
                "/services/opened-scope-outside/cap/level/mid/parent/note.md",
                "/services/retired-opened-outer/cap/level/mid/parent/note.md",
            ] {
                assert!(
                    owner
                        .metadata_backend()
                        .unwrap()
                        .get_object_by_path(&sifs_file_system_id(), path)
                        .unwrap()
                        .is_none()
                );
            }
        }
    }
    assert!(
        completed_at < deadline,
        "the original operation keeps its existing caller deadline"
    );
    drop(owner);
    native.close().unwrap();
}

#[test]
fn opened_services_root_replacement_after_open_conflicts_without_effect() {
    race(
        "native-opened-scope-root-replacement",
        Change::RootReplacement,
    );
}

#[test]
fn opened_services_intermediate_generation_is_fenced_beyond_parent_identity() {
    race(
        "native-opened-scope-intermediate-mode",
        Change::IntermediateMode,
    );
}

#[test]
fn opened_services_effect_does_not_resolve_changed_outside_root_prefix() {
    race("native-opened-scope-outside-mode", Change::OutsideRootMode);
}

#[test]
fn opened_services_outside_ancestor_link_cannot_redirect_the_original_effect() {
    race(
        "native-opened-scope-outside-link",
        Change::OutsideAncestorLink,
    );
}
