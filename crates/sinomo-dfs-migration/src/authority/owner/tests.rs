use super::inventory::{Inventory, JobReference, ObservedState, UnitKey, UnitReference, UnitRole};
use super::scheduler::Scheduler;
use std::path::PathBuf;

#[test]
fn native_read_admission_unavailable_preserves_pending_and_real_recovery() {
    use sinomo_core::{network::NetworkError, service::ServiceError};
    use sinomo_dfs::MetadataBackendError;
    let (native, nodes) = crate::authority::placement_work::native_tests::native_migration_cluster(
        "migration-native-read-admission", 1,
    );
    let context = native.component_context(&nodes[0]).unwrap();
    let owner = crate::authority::placement_work::native_tests::canonical_migration_runtime(&context);
    let keys = [sinomo_dfs::file_system_config_key(&native.file_system().file_system_id)];
    let (original, _) = owner.authority.read_exact(&keys).unwrap();
    // Inject only a failed read. No test-supplied success, proof, or effect can
    // make the next actual native observation current.
    for failure in [
        NetworkError::ClusterReadOnly { detail: "injected unavailable native read admission".into() },
        NetworkError::VerifiedClusterNonLeader { node_id: nodes[0].clone(), detail: "leader changed".into() },
        NetworkError::UnknownNode { node_id: nodes[0].clone() },
        NetworkError::timeout("native read", sinomo_core::network::NetworkTimeoutPhase::FirstByte,
            std::time::Duration::from_millis(1), sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
        NetworkError::ObjectRpcRouteSaturated { route: "/kv/read".into(), concurrency_limit: 1 },
    ] {
        let unavailable = owner.authority.read_native::<()>(async {
            Err(ServiceError::Network(failure))
        }).unwrap_err();
        assert!(matches!(unavailable, MetadataBackendError::StoragePreparationPending {
            root, processed_records: 0,
        } if root == owner.metadata_root));
    }
    let malformed = owner.authority.read_native::<()>(async {
        Err(ServiceError::Network(NetworkError::Serialization {
            detail: "invalid native response".into(),
        }))
    }).unwrap_err();
    assert!(matches!(malformed, MetadataBackendError::Serde(_)));
    let mounting = owner.authority.read_peer::<()>(async {
        Err(NetworkError::UnknownNode { node_id: nodes[0].clone() })
    }).unwrap_err();
    assert!(matches!(mounting, MetadataBackendError::StoragePreparationPending { .. }));
    let denied = owner.authority.read_peer::<()>(async { Err(NetworkError::AuthDenied) }).unwrap_err();
    assert!(matches!(denied, MetadataBackendError::Serde(_)));
    let (recovered, proof) = owner.authority.read_exact(&keys).unwrap();
    crate::authority::current_authority(&proof).unwrap();
    assert_eq!(recovered, original);
    drop(owner);
    drop(context);
    native.close().unwrap();
}

fn version(name: &str) -> UnitKey {
    UnitKey {
        relative_root: PathBuf::from("dfs/content"),
        role: UnitRole::ContentVersion {
            object_id: "object".into(),
            version: name.into(),
        },
    }
}

#[test]
fn demanded_work_cannot_starve_cold_inventory_or_replace_its_original_job() {
    let cold = version("cold");
    let original = JobReference {
        checkpoint: PathBuf::from("dfs/content-conversion/original.sobj"),
        identity: "source-generation-and-cut".into(),
    };
    let mut retained = UnitReference::discovered(cold.clone());
    retained.job = Some(original.clone());
    retained.state = ObservedState::Waiting {
        evidence: "original source verification".into(),
    };
    let mut scheduler = Scheduler::new();
    scheduler.recovered(retained).unwrap();
    // Repeated frontend demand must never replace a retained conversion job.
    scheduler.enqueue(cold.clone(), true).unwrap();
    scheduler.enqueue(cold.clone(), true).unwrap();
    let (id, selected) = scheduler.next().unwrap();
    assert_eq!(selected.unit, cold);
    assert_eq!(selected.job, Some(original));
    scheduler.enqueue(cold.clone(), true).unwrap();
    assert!(
        scheduler.next().is_none(),
        "a running job must not gain a second executor"
    );
    scheduler.finish(&id, selected);

    let untouched = version("never-requested");
    scheduler.enqueue(untouched.clone(), false).unwrap();
    let mut observed_cold = false;
    for request in 0..4 {
        scheduler
            .enqueue(version(&format!("demand-{request}")), true)
            .unwrap();
        let (id, selected) = scheduler.next().unwrap();
        observed_cold |= selected.unit == untouched;
        scheduler.finish(&id, selected);
    }
    assert!(
        observed_cold,
        "continuous demand starved background migration"
    );
}

#[test]
fn restart_retains_job_identity_but_requeues_verified_observations_for_validation() {
    let root = sinomo_testdir::TestDir::new("migration-inventory-restart");
    let inventory = Inventory::open(&root).unwrap();
    let key = version("original");
    let mut reference = UnitReference::discovered(key.clone());
    reference.job = Some(JobReference {
        checkpoint: PathBuf::from("dfs/migration/original/checkpoint.sobj"),
        identity: "original-cut".into(),
    });
    reference.state = ObservedState::VerifiedCurrent;
    inventory.record(&reference).unwrap();
    drop(inventory);
    let reopened = Inventory::open(&root).unwrap();
    let retained = reopened.read(&key).unwrap().unwrap();
    assert_eq!(retained, reference);
    let mut scheduler = Scheduler::new();
    scheduler.recovered(retained).unwrap();
    let (_, selected) = scheduler
        .next()
        .expect("persisted Ready must be revalidated");
    assert_eq!(selected.job, reference.job);
    assert_eq!(selected.unit, key);
}

#[test]
fn inventory_refuses_foreign_paths_and_corrupt_retained_records() {
    let root = sinomo_testdir::TestDir::new("migration-inventory-corrupt");
    let inventory = Inventory::open(&root).unwrap();
    let mut key = version("kept");
    key.relative_root = PathBuf::from("../other-owner");
    assert!(inventory.record(&UnitReference::discovered(key)).is_err());
    let key = version("kept");
    let mut reference = UnitReference::discovered(key.clone());
    reference.job = Some(JobReference {
        checkpoint: PathBuf::from("../other-job.sobj"),
        identity: "cut".into(),
    });
    assert!(inventory.record(&reference).is_err());
    reference.job = None;
    inventory.record(&reference).unwrap();
    let id = key.id().unwrap();
    let path = inventory
        .root()
        .join("units")
        .join(&id[..2])
        .join(format!("{id}.sobj"));
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
    assert!(inventory.read(&key).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes[..bytes.len() / 2]);
}
