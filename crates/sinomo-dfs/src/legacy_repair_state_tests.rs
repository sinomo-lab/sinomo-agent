use super::*;
use crate::runtime_host::{DfsDataOwnerLock, DfsRuntimePaths};

#[test]
fn opening_a_data_root_removes_the_old_queue_custody_and_handoff_pages() {
    let root = sinomo_testdir::TestDir::new("legacy-repair-state-open");
    let data = SinomoDataDir::new(root.path());
    let dfs = data.dfs_dir();
    let handoff_pages = dfs
        .join("metadata")
        .join("services")
        .join("journal")
        .join("repair-handoffs-v1")
        .join("handoff-source");
    std::fs::create_dir_all(&handoff_pages).unwrap();
    std::fs::write(handoff_pages.join("cut.sobj"), b"custody cut").unwrap();
    std::fs::create_dir_all(dfs.join("maintenance")).unwrap();
    for name in QUEUE_FILES {
        std::fs::write(
            dfs.join("maintenance").join(name),
            b"queue rows and custody",
        )
        .unwrap();
    }
    let unrelated = dfs
        .join("maintenance")
        .join("metadata-replication-suppressions.sobj");
    std::fs::write(&unrelated, b"kept").unwrap();
    let migration_copy = dfs
        .join("migration-retained")
        .join("local-state")
        .join("repair-v8")
        .join("repair-attempts.sobj");
    std::fs::create_dir_all(migration_copy.parent().unwrap()).unwrap();
    std::fs::write(&migration_copy, b"converted queue").unwrap();

    let owner = DfsDataOwnerLock::acquire(&DfsRuntimePaths::new(&data)).unwrap();
    for path in legacy_repair_state_paths(&data) {
        assert!(!path.exists(), "{} survived the open", path.display());
    }
    assert!(
        !dfs.join("metadata/services/journal/repair-handoffs-v1")
            .exists()
    );
    assert!(unrelated.exists(), "other maintenance state is untouched");
    assert!(dfs.join("metadata/services/journal").exists());
    drop(owner);
    // A second open finds nothing to remove.
    assert_eq!(remove_legacy_repair_state(&data).unwrap(), 0);
}
