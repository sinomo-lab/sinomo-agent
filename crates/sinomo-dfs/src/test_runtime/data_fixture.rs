//! Exact current DATA from a real stopped native producer for kernel-only tests.
//! The copy includes its original committed genesis and generation identity;
//! it carries no live service, configuration permit, or acknowledgement authority.
use super::*;

pub(crate) fn copy_native_genesis_data(destination: &std::path::Path, shard: &str) {
    copy_native_genesis_data_into(destination, shard, None);
}

pub(crate) fn copy_native_genesis_data_into_selected(
    destination: &std::path::Path, shard: &str,
    filesystem: &crate::DfsFileSystemId, layout: &crate::DfsSelectedOriginalDataLayout,
) {
    let stage = layout.stage(filesystem, &crate::DfsMetadataShardId::new(shard), destination).unwrap();
    assert!(stage.contains_only_marker().unwrap(), "selected DATA copy requires its exact fresh admitted marker");
    copy_native_genesis_data_into(destination, shard, Some(stage));
}

fn copy_native_genesis_data_into(destination: &std::path::Path, shard: &str,
    selected: Option<crate::selected_fixture::SelectedOriginalFixtureIdentity>) {
    if destination.join("metadata-generation.sobj").exists() {
        return;
    }
    assert!(
        selected.as_ref().is_some_and(|stage| stage.contains_only_marker().unwrap())
            || std::fs::read_dir(destination).unwrap().next().is_none(),
        "a DATA fixture must not replace retained storage"
    );
    let destination = destination.to_owned();
    let shard = shard.to_owned();
    // Kernel tests may already be inside a Tokio runtime. The actual fixture
    // owns its normal independent host lifecycle on this dedicated thread.
    std::thread::spawn(move || {
        let name = format!("native-kernel-data-{}", sinomo_core::Id::generate());
        let (mut native, nodes) = DfsTestRuntime::start_cluster(&name, 1).unwrap();
        let node = nodes[0].clone();
        if !native
            .metadata_shards()
            .unwrap()
            .iter()
            .any(|row| row.shard_id == shard)
        {
            let phase = native
                .begin_deferred_native_genesis(&shard, &format!("/services/kernel-data-{shard}"))
                .unwrap();
            native.complete_deferred_native_genesis(&phase).unwrap();
        }
        let source = crate::local_dfs_metadata_shard_path(
            native.data_dir(&node),
            &crate::DfsMetadataShardId::new(&shard),
        );
        native.stop_node(&node).unwrap();
        let mut backend =
            crate::LocalSobjMetadataBackend::new(crate::DfsMetadataShardId::new(&shard), &source)
                .unwrap();
        backend
            .persist_current_metadata_checkpoint_for_test()
            .unwrap();
        drop(backend);
        fn copy(source: &std::path::Path, destination: &std::path::Path) {
            std::fs::create_dir_all(destination).unwrap();
            for entry in std::fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                let target = destination.join(entry.file_name());
                let kind = entry.file_type().unwrap();
                if kind.is_dir() {
                    copy(&entry.path(), &target);
                } else {
                    assert!(kind.is_file(), "native fixture contains a non-file entry");
                    std::fs::copy(entry.path(), target).unwrap();
                }
            }
        }
        copy(&source, &destination);
        native.close().unwrap();
    })
    .join()
    .expect("actual native DATA producer");
}
