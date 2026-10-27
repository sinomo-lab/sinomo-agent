//! Actual native singleton genesis and its one semantic reconciliation edge.
//! These controls do not fabricate publication results or replica evidence.
use super::*;
use crate::test_runtime::{DfsTestRuntime, executor};
use crate::{DfsMessageExecutionScope, DfsPreparedOperationRecord, DfsPreparedOperationStatus};
use std::fs;
use std::time::SystemTime;

#[derive(Debug, Eq, PartialEq)]
struct ShardImage {
    original: DfsPreparedOperationRecord,
    frames: Vec<Vec<u8>>,
    files: Vec<(PathBuf, Vec<u8>, SystemTime)>,
}

fn image(
    data: &SinomoDataDir,
    shard: &DfsMetadataShardConfig,
    original: &DfsPreparedOperationRecord,
    deadline: Instant,
) -> ShardImage {
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let root = local_dfs_metadata_shard_path(data, &id);
    let _fence = loop {
        assert!(
            Instant::now() < deadline,
            "native image exceeded original operation deadline"
        );
        if let Some(guard) = crate::metadata_lock::try_metadata_inspection_lock(
            root.clone(),
            crate::metadata_cursor::journal_write_lock_path(&root),
        )
        .unwrap()
        {
            break guard;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let mut backend =
        crate::LocalSobjMetadataBackend::open_for_inspection(id.clone(), root.clone()).unwrap();
    backend.begin_inspection_snapshot().unwrap();
    let observed = backend
        .prepared_operation_record(&original.digest.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(observed.digest, original.digest);
    assert_eq!(observed.payload, original.payload);
    let entries =
        crate::metadata_journal::durable_journal_entries_since_limited(&root, 0, 256).unwrap();
    assert!(
        entries.len() < 256,
        "bounded control must observe the entire canonical prefix"
    );
    let frames = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
        &root,
        &entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .into_iter()
    .zip(&entries)
    .map(|(frame, entry)| {
        let frame = frame.expect("actual immutable frame");
        assert_eq!(frame.entry(), entry);
        frame.exact_bytes().to_vec()
    })
    .collect();
    let mut pending = vec![root.clone()];
    let mut files = Vec::new();
    let mut total = 0usize;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(
                !metadata.file_type().is_symlink(),
                "native shard evidence cannot follow aliases"
            );
            if metadata.is_dir() {
                pending.push(path);
            } else {
                assert!(metadata.is_file());
                total = total
                    .checked_add(usize::try_from(metadata.len()).unwrap())
                    .unwrap();
                assert!(
                    total <= 8 * 1024 * 1024 && files.len() < 256,
                    "bounded shard evidence inventory"
                );
                files.push((
                    path.strip_prefix(&root).unwrap().to_owned(),
                    fs::read(&path).unwrap(),
                    metadata.modified().unwrap(),
                ));
            }
        }
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    assert!(Instant::now() < deadline);
    ShardImage {
        original: observed,
        frames,
        files,
    }
}

fn forget_settled_signature(data: &SinomoDataDir) {
    // Exercise the actual already-Committed publication pass, not merely its
    // successful-pass cache hit. This changes only the existing test-local
    // in-memory optimization; no original, authority or retry edge is invented.
    if let Some(signatures) = DFS_DEFAULT_MATERIALIZATION_SIGNATURES.get() {
        signatures.lock().unwrap().remove(&data.root());
    }
}

#[test]
fn native_genesis_commit_requests_one_followup_and_unchanged_pass_stays_idle() {
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-genesis-semantic-followup", 1).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let primary = native.root_shard().unwrap().primary_node_id;
        let phase = native
            .begin_deferred_native_genesis(
                "semantic-followup-scope",
                &crate::sifs_node_path(&primary),
            )
            .unwrap();
        let shard = phase.shard().clone();
        let original = phase.original_record().clone();
        let data = native.data_dir(&primary).clone();
        let shards = native.metadata_shards().unwrap();
        let file_system = native.file_system();
        assert_eq!(original.status, DfsPreparedOperationStatus::Prepared);
        assert!(original.committed_entry.is_none());
        assert!(phase.deferral_hits() > 0);
        let paths = ClusterPaths::new(&data);
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&data),
        )
        .unwrap();
        let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(&data).unwrap();
        let control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        executor().block_on(async {
            let deadline=Instant::now()+sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline),async {
                let scope=DfsMessageExecutionScope::GlobalExclusive;
                let admission=scheduler.admit(&scope).await.unwrap();
                let owners=crate::DfsPublicationExecutionOwners::from_runtime(data.root(),scope,admission,owner);
                let mut host=DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(
                    data.clone(),paths,primary.clone(),Box::new(control),None,owners,
                );
                // Consume only any completed setup edge while this actual
                // owner excludes concurrent materializer publication.
                let _=take_default_cluster_dfs_materialization_immediate_retry(&data);
                forget_settled_signature(&data);
                let before=image(&data,&shard,&original,deadline);
                let held=host.materialize_default_sifs_metadata_on_shards_async(
                    file_system.clone(),shards.clone(),nodes.clone(),primary.clone(),
                ).await.unwrap_err();
                assert!(held.to_string().contains(genesis::NATIVE_GENESIS_COMPLETION_DEFERRED),"{held}");
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data),"held original cannot assert publication or wake idle reconciliation");
                assert_eq!(image(&data,&shard,&original,deadline),before);
                // Drop only this original negative hold while the actual
                // global native owner still has admission; no replacement ID.
                drop(phase);
                host.materialize_default_sifs_metadata_on_shards_async(
                    file_system.clone(),shards.clone(),nodes.clone(),primary.clone(),
                ).await.unwrap();
                let committed=image(&data,&shard,&original,deadline);
                assert_eq!(committed.original.status,DfsPreparedOperationStatus::Committed);
                assert_eq!(committed.original.committed_entry.as_ref().unwrap().sequence,original.digest.prepare_sequence+1);
                assert_eq!(committed.frames.len(),before.frames.len()+1);
                assert!(take_default_cluster_dfs_materialization_immediate_retry(&data),"actual new commit must survive settled-signature recording as one follow-up edge");
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data));
                forget_settled_signature(&data);
                host.materialize_default_sifs_metadata_on_shards_async(
                    file_system.clone(),shards.clone(),nodes.clone(),primary.clone(),
                ).await.unwrap();
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data),"already-Committed native pass must not self-wake");
                assert_eq!(image(&data,&shard,&original,deadline),committed,"unchanged pass must preserve exact files/mtimes and canonical frames");
                host.materialize_default_sifs_metadata_on_shards_async(file_system,shards,nodes.clone(),primary).await.unwrap();
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data),"settled cache hit must stay idle");
                assert_eq!(image(&data,&shard,&original,deadline),committed);
                assert!(Instant::now()<deadline);
            }).await.expect("original five-second native owner phase");
        });
    }));
    let closed = native.close();
    match result {
        Ok(()) => closed.unwrap(),
        Err(panic) => {
            let _ = closed;
            std::panic::resume_unwind(panic)
        }
    }
}

#[test]
fn native_genesis_confirmed_progress_survives_later_materialization_error() {
    let (native, nodes) =
        DfsTestRuntime::start_cluster("native-genesis-followup-later-error", 1).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let primary = native.root_shard().unwrap().primary_node_id;
        let phase = native
            .begin_deferred_native_genesis(
                "semantic-followup-later-error",
                &crate::sifs_node_path(&primary),
            )
            .unwrap();
        let shard = phase.shard().clone();
        let original = phase.original_record().clone();
        let data = native.data_dir(&primary).clone();
        let full_shards = native.metadata_shards().unwrap();
        let file_system = native.file_system();
        let paths = ClusterPaths::new(&data);
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&data),
        )
        .unwrap();
        let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(&data).unwrap();
        let control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        executor().block_on(async {
            let deadline=Instant::now()+sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline),async {
                let scope=DfsMessageExecutionScope::GlobalExclusive;
                let admission=scheduler.admit(&scope).await.unwrap();
                let owners=crate::DfsPublicationExecutionOwners::from_runtime(data.root(),scope,admission,owner);
                let mut host=DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(
                    data.clone(),paths,primary.clone(),Box::new(control),None,owners,
                );
                let _=take_default_cluster_dfs_materialization_immediate_retry(&data);
                forget_settled_signature(&data);
                let before=image(&data,&shard,&original,deadline);
                drop(phase);
                // A genuine current prepared scope commits first. The later
                // existing bootstrap-object routing must reject absent root
                // coverage; this supplies no fake publication result or hook.
                let failed=host.materialize_default_sifs_metadata_on_shards_async(
                    file_system.clone(),vec![shard.clone()],nodes.clone(),primary.clone(),
                ).await.unwrap_err();
                let original_error=failed.to_string();
                assert!(matches!(&failed, DfsError::Metadata { message }
                    if message == "current bootstrap object has no exact assignment"),
                    "{original_error}");
                let committed=image(&data,&shard,&original,deadline);
                assert_eq!(committed.original.status,DfsPreparedOperationStatus::Committed);
                assert_eq!(committed.original.committed_entry.as_ref().unwrap().sequence,original.digest.prepare_sequence+1);
                assert_eq!(committed.frames.len(),before.frames.len()+1);
                assert!(take_default_cluster_dfs_materialization_immediate_retry(&data),"later Result error must retain prior actual publication edge");
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data));
                let repeated=host.materialize_default_sifs_metadata_on_shards_async(
                    file_system.clone(),vec![shard.clone()],nodes.clone(),primary.clone(),
                ).await.unwrap_err();
                assert_eq!(repeated.to_string(),original_error,"same existing error is returned unchanged");
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data),"repeated error without new semantic progress must not self-wake");
                assert_eq!(image(&data,&shard,&original,deadline),committed);
                // The valid current topology remains usable, with no second
                // commit or synthetic follow-up for the same original.
                host.materialize_default_sifs_metadata_on_shards_async(file_system,full_shards,nodes.clone(),primary).await.unwrap();
                assert!(!take_default_cluster_dfs_materialization_immediate_retry(&data));
                assert_eq!(image(&data,&shard,&original,deadline),committed);
                assert!(Instant::now()<deadline);
            }).await.expect("original five-second native owner phase");
        });
    }));
    let closed = native.close();
    match result {
        Ok(()) => closed.unwrap(),
        Err(panic) => {
            let _ = closed;
            std::panic::resume_unwind(panic)
        }
    }
}
