//! Test fixtures over a real native DFS runtime with the migration
//! component mounted, shared by the migration owner's tests.
use super::placement_job_source::OriginalPlacementJob;
use super::placement_work::OriginalPlacementGoal;
use super::*;
use crate::authority::{shared_owner, storage_preparation_provider};

pub(in crate::authority) fn native_migration_cluster(
    name: &str,
    count: usize,
) -> (
    sinomo_dfs::test_support::DfsTestRuntime,
    Vec<sinomo_core::node::NodeId>,
) {
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, count);
    let mut native = sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    for node in &nodes[1..] {
        native.define_node(node.clone()).unwrap();
    }
    native
        .configure_component_factories(vec![
            Arc::new(
                sinomo_dfs::component_factory_with_storage_preparation(Arc::new(
                    storage_preparation_provider,
                ))
                .unwrap(),
            ),
            Arc::new(staged_import_component_factory().unwrap()),
        ])
        .unwrap();
    native.start_nodes().unwrap();
    (native, nodes)
}

pub(in crate::authority) fn canonical_migration_runtime(
    context: &sinomo_module::ComponentContext,
) -> Arc<crate::authority::MigrationOwner> {
    let root = context.data_dir_root().canonicalize().unwrap();
    let lifecycle = sinomo_dfs::dfs_storage_preparation_lifecycle(&root)
        .expect("normal DFS factory registered its actual provider lifecycle");
    let runtime = shared_owner(context, lifecycle.clone()).unwrap();
    assert!(runtime.lifecycle.same_channel(&lifecycle));
    runtime
}

/// Continue only an unfinished actual source-writer admission. The caller owns
/// the immutable request and one acceptance deadline across all stages.
/// An in-flight normal RPC keeps its existing transport limits.
pub(in crate::authority) fn continue_native_source_step<T>(
    source_root: &std::path::Path,
    deadline: std::time::Instant,
    mut step: impl FnMut() -> std::result::Result<T, MetadataBackendError>,
) -> std::result::Result<T, MetadataBackendError> {
    let source_root = source_root.canonicalize().map_err(error)?;
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(error(
                "native source stage exceeded its single acceptance deadline",
            ));
        }
        match step() {
            Err(MetadataBackendError::StoragePreparationPending {
                root,
                processed_records: 0,
            }) if root == source_root => std::thread::yield_now(),
            Ok(value) if std::time::Instant::now() < deadline => return Ok(value),
            Ok(_) => {
                return Err(error(
                    "native source stage completed after its acceptance deadline",
                ));
            }
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn native_source_continuation_requires_actual_own_writer_release_and_keeps_other_errors_closed() {
    use fs2::FileExt as _;
    let directory = sinomo_testdir::TestDir::new("native-source-continuation-guards");
    let own = directory.path().join("own");
    let foreign = directory.path().join("foreign");
    for root in [&own, &foreign] {
        std::fs::create_dir_all(root.join("journal")).unwrap();
    }
    let writer = std::fs::File::create(foreign.join("journal/write.lock")).unwrap();
    writer.lock_exclusive().unwrap();
    let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let mut attempts = 0;
    let foreign_result = continue_native_source_step(&own, deadline, || {
        attempts += 1;
        crate::metadata::retain_source_fence(&foreign)
    });
    assert!(
        matches!(foreign_result, Err(MetadataBackendError::StoragePreparationPending {
        root, processed_records: 0
    }) if root == foreign.canonicalize().unwrap())
    );
    assert_eq!(attempts, 1, "foreign custody cannot enter the continuation");
    let mut own_writer = Some(std::fs::File::create(own.join("journal/write.lock")).unwrap());
    own_writer.as_ref().unwrap().lock_exclusive().unwrap();
    let mut own_attempts = 0;
    let fence = continue_native_source_step(&own, deadline, || {
        own_attempts += 1;
        let result = crate::metadata::retain_source_fence(&own);
        if own_attempts == 1 {
            assert!(
                matches!(&result, Err(MetadataBackendError::StoragePreparationPending {
                root, processed_records: 0
            }) if *root == own.canonicalize().unwrap())
            );
            drop(own_writer.take());
        }
        result
    })
    .unwrap();
    assert_eq!(
        own_attempts, 2,
        "the original writer must actually release custody"
    );
    drop(fence);
    std::fs::remove_file(own.join("journal/write.lock")).unwrap();
    std::fs::create_dir(own.join("journal/write.lock")).unwrap();
    assert!(matches!(
        continue_native_source_step(&own, deadline, || {
            crate::metadata::retain_source_fence(&own)
        }),
        Err(MetadataBackendError::Serde(_))
    ));
    let mut attempted_after_deadline = false;
    assert!(
        continue_native_source_step(&own, std::time::Instant::now(), || {
            attempted_after_deadline = true;
            crate::metadata::retain_source_fence(&own)
        })
        .is_err()
    );
    assert!(!attempted_after_deadline);
}

#[derive(Serialize)]
struct OriginalV0Job<'a> {
    job_id: &'a str,
    file_system_id: &'a str,
    path: &'a str,
    scope: &'a str,
    state: &'a str,
    total_object_count: usize,
    scanned_object_count: usize,
    matched_object_count: usize,
    updated_object_count: usize,
    scheduled_repair_count: usize,
    completed_repair_count: usize,
    pending_repair_count: usize,
    revision: u64,
    created_at_unix_nanos: u64,
    updated_at_unix_nanos: u64,
}

/// A pending original v0 placement goal captured over `assignments`, from
/// its row as an in-process store holds it; no cluster is needed.
pub(in crate::authority) fn pending_v0_goal_for_test(
    job_id: &str,
    assignments: &[DfsMetadataShardConfig],
) -> OriginalPlacementGoal {
    let fs = sinomo_dfs::sifs_file_system_id();
    let source = OriginalV0Job {
        job_id,
        file_system_id: fs.as_str(),
        path: "/users",
        scope: "subtree",
        state: "pending",
        total_object_count: 0,
        scanned_object_count: 0,
        matched_object_count: 0,
        updated_object_count: 0,
        scheduled_repair_count: 0,
        completed_repair_count: 0,
        pending_repair_count: 0,
        revision: 1,
        created_at_unix_nanos: 1,
        updated_at_unix_nanos: 1,
    };
    let key = format!("/dfs/filesystems/{fs}/placement-reconciliations/{job_id}");
    let value =
        sinomo_core::object::serde_ext::to_bytes(&source, 1, sinomo_core::object::LengthMode::U32)
            .unwrap();
    let mut store = sinomo_core::kv::InProcessKvStore::default();
    store.put(key.clone(), value, None).unwrap();
    let entry = store.get(&key).cloned().unwrap();
    let job = OriginalPlacementJob::decode(&entry).unwrap();
    OriginalPlacementGoal::for_test(job, assignments.to_vec())
}

/// Remove `shard`'s storage record, as a deployment that predates storage
/// records has none: the shard's copy is then old storage that migration
/// converts per shard.
pub(in crate::authority) fn remove_storage_record(
    owner: &MigrationOwner,
    file_system_id: &sinomo_dfs::DfsFileSystemId,
    shard: &str,
) {
    let key = sinomo_dfs::metadata_shard_storage_key(file_system_id, shard);
    let (entries, _) = owner
        .authority
        .read_exact(std::slice::from_ref(&key))
        .unwrap();
    let born = entries[0]
        .entry
        .clone()
        .expect("a born shard carries a storage record");
    let committed = owner
        .authority
        .commit(KvTxnRequest::new(
            KvRequestId::generate(),
            KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key: key.clone(),
                    value: born.value,
                }],
                success: vec![KvOp::Delete { key }],
                failure: Vec::new(),
            },
        ))
        .unwrap();
    NativeImportAuthority::require_committed(&committed).unwrap();
}
