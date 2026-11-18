//! A shard without a storage record whose copies are current format.
use super::*;

/// Services' storage record is removed, as a deployment that predates
/// storage records has none. After a restart its copies are old storage that
/// migration owns, whatever their format: services' own primary converts it
/// on its owner's schedule, every member activates the image, and the shard
/// serves again.
#[test]
fn native_a_record_less_shard_with_current_copies_converts_on_its_owners_schedule() {
    let mut cluster = start(
        "shard-conversion-record-less-current",
        3,
        &[
            ("root", 0, &[1, 2]),
            ("services", 1, &[0, 2]),
            ("users", 2, &[0, 1]),
        ],
    );
    write_namespace(&cluster);
    let fs = cluster.fs.clone();
    let owner =
        canonical_migration_runtime(&cluster.native.component_context(&cluster.nodes[0]).unwrap());
    crate::authority::native_fixtures::remove_storage_record(&owner, &fs, "services");
    drop(owner);
    hold(&mut cluster, &[]);
    let owners = owners(&cluster);
    let deadline = Instant::now() + Duration::from_secs(120);
    while !storage(&owners[0], &fs, "services")
        .is_some_and(|record| record.converted_activation().is_some())
    {
        assert!(
            Instant::now() < deadline,
            "services did not convert: record {:?}, primary scheduler {}",
            storage(&owners[0], &fs, "services"),
            owners[1].diagnostic_scheduler()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let config = shard_config(&owners[0], &fs, "services");
    let record = await_activated(&cluster, &owners, "services", &configured(&config));
    let (attempt, _) = record.converted_activation().unwrap();
    assert_eq!(
        (attempt.epoch, &attempt.primary_node_id),
        (config.epoch, &cluster.nodes[1])
    );
    for reader in 0..cluster.nodes.len() {
        await_serving(&cluster, reader, &config, "/services/file");
    }
    write_beside_closed(&cluster, 0, "/services/written-after-conversion");
    drop(owners);
    cluster.native.close().unwrap();
}
