use super::*;
use super::discovery::Discovery;
use super::inventory::{Inventory, UnitRole};

#[test]
fn discovery_recovers_cold_units_across_bounded_frontiers_and_restart_epoch() {
    let root = sinomo_testdir::TestDir::new("migration-bounded-cold-discovery");
    let retained = root.join("dfs/metadata/.metadata-retained");
    // More children than the durable frontier limit, with no foreground demand.
    for index in 0..320 {
        let snapshot = retained.join(format!("branch-{index:04}/source/records/tables/snapshot.sobj"));
        std::fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        std::fs::write(snapshot, b"opaque original DATA; discovery grants no decoding authority").unwrap();
    }
    let inventory = Inventory::open(root.path()).unwrap();
    let mut discovery = Discovery::open(root.path(), inventory.root()).unwrap();
    let mut found = BTreeSet::new();
    for _ in 0..4096 {
        if !discovery.has_work() { break; }
        let batch = discovery.step(16).unwrap();
        assert!(batch.len() <= 16);
        for reference in batch {
            if matches!(reference.unit.role, UnitRole::RetainedArtifact) { found.insert(reference.unit.relative_root.clone()); }
            inventory.record(&reference).unwrap();
        }
        discovery.commit().unwrap();
    }
    assert_eq!(found.len(), 320, "bounded frontier lost never-requested source roots");
    drop(discovery);
    // A new descendant after a completed traversal cannot be hidden by an old
    // directory observation when the owner restarts.
    let added = retained.join("later/records/tables/snapshot.sobj");
    std::fs::create_dir_all(added.parent().unwrap()).unwrap();
    std::fs::write(added, b"new retained source").unwrap();
    let mut resumed = Discovery::open(root.path(), inventory.root()).unwrap();
    let mut observed_added = false;
    for _ in 0..4096 {
        if !resumed.has_work() { break; }
        for reference in resumed.step(16).unwrap() {
            observed_added |= reference.unit.relative_root == std::path::Path::new("dfs/metadata/.metadata-retained/later");
            if let Some(old) = inventory.read(&reference.unit).unwrap() { inventory.record(&old).unwrap(); }
            else { inventory.record(&reference).unwrap(); }
        }
        resumed.commit().unwrap();
    }
    assert!(observed_added);
}
