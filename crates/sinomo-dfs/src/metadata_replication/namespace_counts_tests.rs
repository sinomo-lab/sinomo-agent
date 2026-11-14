use super::*;
use crate::DfsMetadataBackend;

#[test]
fn native_count_catalog_rejects_owned_unknown_rows_and_ignores_ancestor_copies() {
    let (runtime, mut service, services) = crate::test_support::current_native_metadata_service(
        "native-count-snapshot-catalog",
        2,
        "/services",
    )
    .unwrap();
    let file = service
        .posix_write_file(
            "/services/count-snapshot-file",
            b"original bytes",
            crate::DfsWriteOptions::replicated_eventual(vec![services.primary_node_id.clone()]),
        )
        .unwrap();
    let root_id = DfsMetadataShardId::new("root");
    let fs = services.file_system_id.clone();
    let context = service
        .retained_metadata_replica_context(&fs, &root_id)
        .unwrap()
        .unwrap();
    let catalog = context.namespace_catalog.as_ref().unwrap();
    let original = runtime
        .node_metadata(&context.shard.primary_node_id)
        .unwrap()
        .export_shard_table_snapshot(&root_id)
        .unwrap()
        .unwrap();
    let mut tables =
        crate::metadata_table_store::metadata_tables_from_snapshot(*original.tables).unwrap();
    catalog.validate_snapshot(&root_id, &tables).unwrap();
    // Classification DATA only: a physical ancestor can retain an old copy of
    // a row now owned by a more-specific shard. It is not another live inode.
    let mut ancestor = runtime
        .node_metadata(&services.primary_node_id)
        .unwrap()
        .get_object(&file.object_id)
        .unwrap()
        .unwrap();
    ancestor.namespace_link_count = None;
    tables.objects.insert(ancestor.object_id.clone(), ancestor);
    catalog.validate_snapshot(&root_id, &tables).unwrap();
    let root = tables
        .objects
        .values_mut()
        .find(|row| row.path == "/")
        .unwrap();
    root.namespace_link_count = None;
    assert!(catalog.validate_snapshot(&root_id, &tables).is_err());
    let projection = service
        .control_plane
        .as_ref()
        .unwrap()
        .write_configuration_projection()
        .unwrap()
        .unwrap();
    let entries = projection
        .entries(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)
        .unwrap();
    let mut partial = context.clone();
    partial
        .namespace_catalog
        .as_mut()
        .unwrap()
        .shards
        .retain(|shard| shard.shard_id == "root");
    assert!(
        !crate::metadata_replica_context_matches_authority_entries(entries, &partial).unwrap(),
        "native current-context revalidation rejects a partial catalog before snapshot permission"
    );
    drop(service);
    runtime.close().unwrap();
}
