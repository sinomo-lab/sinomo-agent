//! Patch writes regression tests.

use super::*;

/// `node`'s durable metadata for `object_id` with the receipt locations of its
/// current version, as a store-backed read projects it. Callers delegating to
/// their Main keep no store, so replica convergence is observed here.
fn node_store_object_with_receipts(
    runtime: &crate::test_support::DfsTestRuntime,
    node: &NodeId,
    object_id: &DfsObjectId,
) -> Option<DfsObjectMetadata> {
    let store = runtime.node_metadata(node).expect("node metadata store");
    let mut object = store.get_object(object_id).expect("node object lookup")?;
    if let Some(hash) = object.content_hash.clone() {
        for receipt in store
            .receipts_for_version(object_id, &object.version)
            .expect("node receipt lookup")
        {
            if receipt.content_hash.as_deref() == Some(hash.as_str())
                && !object.current_locations.contains(&receipt.replica_node_id)
            {
                object.current_locations.push(receipt.replica_node_id);
            }
        }
    }
    Some(object)
}

#[test]
fn sifs_posix_write_at_self_only_uses_extent_patch_manifest() {
    let root = temp_root("sifs-posix-write-at-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_extent/work";
    let file = "/services/sandbox/instances/sandbox_write_at_extent/work/file.bin";

    let mut initial = vec![0_u8; 256 * 1024 + 19];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let initial_hash = content_hash(&initial);
    let patch = b"small-random-write";
    let patch_offset = 128 * 1024 + 11;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);
    let expected_hash = content_hash(&expected);

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_write_at_self_only_uses_extent_patch_manifest",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let stat = dfs
        .posix_write_file_at(
            file,
            patch_offset as u64,
            patch,
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("write extent patch");

    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("patched manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert!(
        !manifest.extents.is_empty(),
        "local self-only write_at should preserve unchanged bytes as extents"
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash)
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "patched write_at should not materialize a full replacement blob"
    );
    assert_eq!(
        dfs.posix_read_file_distributed(file).expect("read patched"),
        expected
    );
    assert_eq!(
        dfs.posix_read_file_range_distributed(file, patch_offset as u64 - 8, patch.len() + 16)
            .expect("read patched range"),
        expected[patch_offset - 8..patch_offset + patch.len() + 8]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_from_reader_uses_extent_patch_manifest() {
    let root = temp_root("sifs-posix-write-at-reader-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_reader_extent").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_reader_extent/work";
    let file = "/services/sandbox/instances/sandbox_write_at_reader_extent/work/file.bin";

    let mut initial = vec![0_u8; 320 * 1024 + 29];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let initial_hash = content_hash(&initial);
    let patch = (0..80 * 1024 + 23)
        .map(|index| (index % 241) as u8)
        .collect::<Vec<_>>();
    let patch_hash = content_hash(&patch);
    let patch_offset = 160 * 1024 + 7;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_write_at_from_reader_uses_extent_patch_manifest",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let stat = dfs
        .posix_write_file_at_from_reader(
            file,
            patch_offset as u64,
            std::io::Cursor::new(patch.clone()),
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("streaming write_at extent patch");

    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("patched manifest");
    assert_eq!(manifest.content_hash, expected_hash);
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash)
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == patch_hash)
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "streamed write_at should not materialize a full replacement blob"
    );
    assert_eq!(
        dfs.posix_read_file_range_distributed(file, patch_offset as u64 - 8, patch.len() + 16)
            .expect("read patched range"),
        expected[patch_offset - 8..patch_offset + patch.len() + 8]
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_patch_reader_failure_preserves_head_without_replaying_consumed_input() {
    struct FailingReader {
        remaining: usize,
        failed: bool,
        reads_after_failure: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl Read for FailingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.failed {
                self.reads_after_failure
                    .set(self.reads_after_failure.get() + 1);
                return Ok(0);
            }
            if self.remaining == 0 {
                self.failed = true;
                return Err(std::io::Error::other("injected patch stream failure"));
            }
            let size = buffer.len().min(self.remaining);
            buffer[..size].fill(0x5a);
            self.remaining -= size;
            Ok(size)
        }
    }

    // Eventual uses the standalone self-only path; confirmed uses the common
    // intent-backed patch path. Neither may fall back after consuming input.
    for policy in [
        DfsObjectPolicy::ReplicatedEventual,
        DfsObjectPolicy::ReplicatedConfirmed,
    ] {
        let root = temp_root("patch-consumed-reader-failure");
        let node_id = NodeId::from_legacy_named("node_patch_reader_failure").expect("node");
        let path = "/users/patch-reader-failure.bin";
        let initial = vec![0x31; 160 * 1024];
        let (runtime, mut dfs) = native_sifs_service(
            "sifs_patch_reader_failure_preserves_head_without_replaying_consumed_input",
            node_id.clone(),
        );
        let before = dfs
            .posix_write_file(
                path,
                &initial,
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("initial write");
        // The caller delegating to its Main keeps no store; the policy change
        // is published by the shard's store-backed opener.
        let mut object = runtime
            .node_metadata(&node_id)
            .unwrap()
            .get_object(&before.object_id)
            .unwrap()
            .expect("object");
        object.policy = policy;
        let mut owner = runtime.metadata_primary_service(path).unwrap();
        owner.add_object_metadata(object).expect("policy");
        drop(owner);
        let reads_after_failure = std::rc::Rc::new(std::cell::Cell::new(0));
        let error = dfs
            .posix_write_file_at_from_reader(
                path,
                19,
                FailingReader {
                    remaining: 70 * 1024,
                    failed: false,
                    reads_after_failure: reads_after_failure.clone(),
                },
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect_err("partially consumed patch must fail");
        assert!(
            error.to_string().contains("injected patch stream failure"),
            "{error}"
        );
        assert_eq!(
            reads_after_failure.get(),
            0,
            "failed input must not be replayed"
        );
        let after = dfs.posix_stat_distributed(path).expect("unchanged head");
        assert_eq!(after.version, before.version);
        assert_eq!(after.content_hash, before.content_hash);
        assert_eq!(after.size_bytes, before.size_bytes);
        assert_eq!(
            dfs.posix_read_file_distributed(path)
                .expect("unchanged content"),
            initial
        );
        std::fs::remove_dir_all(root).ok();
    }
}

#[test]
fn sifs_posix_write_at_from_reader_streams_sparse_extension_as_extent_patch() {
    let root = temp_root("sifs-posix-write-at-reader-sparse-extent-patch");
    let node_id = NodeId::from_legacy_named("node_sifs_write_at_reader_sparse").expect("node");
    let work = "/services/sandbox/instances/sandbox_write_at_reader_sparse/work";
    let file = "/services/sandbox/instances/sandbox_write_at_reader_sparse/work/file.bin";

    let initial = (0..192 * 1024 + 29)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let initial_hash = content_hash(&initial);
    let patch = (0..96 * 1024 + 23)
        .map(|index| (index % 241) as u8)
        .collect::<Vec<_>>();
    let gap_size = 80 * 1024 + 11;
    let patch_offset = initial.len() + gap_size;
    let mut sparse_patch = vec![0_u8; gap_size];
    sparse_patch.extend_from_slice(&patch);
    let sparse_patch_hash = content_hash(&sparse_patch);
    let mut expected = initial.clone();
    expected.extend_from_slice(&sparse_patch);
    let expected_hash = content_hash(&expected);

    struct StreamingReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl std::io::Read for StreamingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                assert_eq!(buffer.len(), 1, "POSIX write_at should only peek one byte");
            } else {
                assert_eq!(
                    buffer.len(),
                    64 * 1024,
                    "content ingest should retain its bounded streaming buffer"
                );
                self.bulk_reads.set(self.bulk_reads.get() + 1);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }

    let (_runtime, mut dfs) = native_sifs_service(
        "sifs_posix_write_at_from_reader_streams_sparse_extension_as_extent_patch",
        node_id.clone(),
    );
    dfs.posix_mkdir_all(work).expect("mkdir");
    dfs.posix_write_file(
        file,
        &initial,
        DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
    )
    .expect("write initial");

    let bulk_reads = std::rc::Rc::new(std::cell::Cell::new(0));
    let stat = dfs
        .posix_write_file_at_from_reader(
            file,
            patch_offset as u64,
            StreamingReader {
                bytes: patch,
                offset: 0,
                bulk_reads: bulk_reads.clone(),
            },
            DfsWriteOptions::replicated_eventual(vec![node_id]),
        )
        .expect("stream sparse write_at");

    assert!(bulk_reads.get() > 1, "patch input should be streamed");
    assert_eq!(stat.size_bytes, Some(expected.len() as u64));
    assert_eq!(stat.content_hash.as_deref(), Some(expected_hash.as_str()));
    let manifest = dfs
        .content()
        .read_manifest(stat.object_id.as_str(), stat.version.as_str())
        .expect("sparse patch manifest");
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == initial_hash),
        "the unchanged base should remain an extent"
    );
    assert!(
        manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == sparse_patch_hash),
        "the generated zero gap and caller stream should share one patch extent"
    );
    assert!(
        !dfs.content()
            .has_blob(&manifest.content_hash)
            .expect("logical blob check"),
        "sparse write_at should not materialize the full logical object"
    );
    assert_eq!(
        dfs.posix_read_file_distributed(file)
            .expect("read sparse file"),
        expected
    );

    std::fs::remove_dir_all(root).ok();
}

#[test]
fn sifs_posix_write_at_replicates_extent_patch_without_full_blob() {
    let (mut runtime, owner, replica, mut owner_dfs, mut replica_dfs) =
        native_patch_services("sifs_posix_write_at_replicates_extent_patch_without_full_blob");
    let path = "/services/replicated-write-at-patch.bin";

    let mut initial = vec![0_u8; 384 * 1024 + 37];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    let base = owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("native committed base on both content owners");
    let object_id = base.object_id;
    let base_hash = base.content_hash.expect("actual committed base hash");
    assert_eq!(owner_dfs.content().read_blob(&base_hash).unwrap(), initial);
    assert_eq!(
        replica_dfs.content().read_blob(&base_hash).unwrap(),
        initial
    );

    let patch = b"remote extent patch";
    let patch_offset = 192 * 1024 + 13;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);
    let expected_hash = content_hash(&expected);
    let pending_stat = owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("owner patch write");
    assert_eq!(pending_stat.size_bytes, Some(expected.len() as u64));

    let mut committed = false;
    for _ in 0..16 {
        runtime
            .maintenance_tick(&owner, Duration::from_secs(5))
            .expect("normal owner maintenance");
        replica_dfs.process_incoming().expect("replica incoming");
        owner_dfs.process_incoming().expect("owner incoming");
        replica_dfs
            .process_incoming()
            .expect("replica commit incoming");
        let owner_object =
            node_store_object_with_receipts(&runtime, &owner, &object_id).expect("owner object");
        let replica_object = node_store_object_with_receipts(&runtime, &replica, &object_id)
            .expect("replica object");
        if owner_object.content_hash.as_deref() == Some(expected_hash.as_str())
            && replica_object.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            committed = true;
            break;
        }
    }
    assert!(committed, "replicated patch write did not commit");

    let committed_owner = node_store_object_with_receipts(&runtime, &owner, &object_id)
        .expect("committed owner metadata");
    assert_eq!(
        committed_owner.content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(
        owner_dfs
            .posix_read_file_distributed(path)
            .expect("owner read"),
        expected
    );
    assert_eq!(
        node_store_object_with_receipts(&runtime, &replica, &object_id)
            .expect("replica metadata")
            .current_locations
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([owner.clone(), replica.clone()])
    );
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), committed_owner.version.as_str())
        .expect("replica patched manifest");
    assert_eq!(replica_manifest.content_hash, expected_hash);
    assert!(
        !replica_manifest.extents.is_empty(),
        "remote patch stage should preserve unchanged bytes as extents"
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == base_hash)
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == content_hash(patch))
    );
    assert!(
        !replica_dfs
            .content()
            .has_blob(&replica_manifest.content_hash)
            .expect("logical blob check"),
        "replicated patch write should not materialize a full replacement blob on the replica"
    );

    drop(replica_dfs);
    drop(owner_dfs);
    runtime.close().unwrap();
}

#[test]
fn sifs_posix_write_at_replicates_large_extent_patch_in_chunks_without_full_blob() {
    let (mut runtime, owner, replica, mut owner_dfs, mut replica_dfs) = native_patch_services(
        "sifs_posix_write_at_replicates_large_extent_patch_in_chunks_without_full_blob",
    );
    let path = "/services/replicated-write-at-large-patch.bin";

    let mut initial = vec![0_u8; 768 * 1024 + 19];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 241) as u8;
    }
    let base = owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("native committed base on both content owners");
    let object_id = base.object_id;
    let base_hash = base.content_hash.expect("actual committed base hash");
    assert_eq!(owner_dfs.content().read_blob(&base_hash).unwrap(), initial);
    assert_eq!(
        replica_dfs.content().read_blob(&base_hash).unwrap(),
        initial
    );

    let mut patch = vec![0_u8; 160 * 1024 + 17];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = 255_u8.wrapping_sub((index % 251) as u8);
    }
    let patch_hash = content_hash(&patch);
    let patch_offset = 257 * 1024 + 3;
    let mut expected = initial.clone();
    expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
    let expected_hash = content_hash(&expected);
    owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            &patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("owner large patch write");

    // The live Main publishes the asynchronous write. A fixed number of
    // foreground polling turns does not establish a replica deadline.
    let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    loop {
        replica_dfs.process_incoming().expect("replica incoming");
        owner_dfs.process_incoming().expect("owner incoming");
        replica_dfs
            .process_incoming()
            .expect("replica commit incoming");
        let owner_object =
            node_store_object_with_receipts(&runtime, &owner, &object_id).expect("owner object");
        let replica_object = node_store_object_with_receipts(&runtime, &replica, &object_id)
            .expect("replica object");
        if owner_object.content_hash.as_deref() == Some(expected_hash.as_str())
            && replica_object.content_hash.as_deref() == Some(expected_hash.as_str())
        {
            break;
        }
        assert!(Instant::now() < deadline,
            "expected {expected_hash}, owner {:?}, replica {:?}",
            owner_object.content_hash, replica_object.content_hash);
        std::thread::sleep(Duration::from_millis(2));
    }

    let committed_owner = node_store_object_with_receipts(&runtime, &owner, &object_id)
        .expect("committed owner metadata");
    assert_eq!(
        owner_dfs
            .posix_read_file_range_distributed(path, patch_offset as u64, patch.len())
            .expect("owner patched range"),
        patch
    );
    let replica_manifest = replica_dfs
        .content()
        .read_manifest(object_id.as_str(), committed_owner.version.as_str())
        .expect("replica patched manifest");
    assert_eq!(replica_manifest.content_hash, expected_hash);
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == base_hash)
    );
    assert!(
        replica_manifest
            .extents
            .iter()
            .any(|extent| extent.content_hash == patch_hash)
    );
    assert!(
        !replica_dfs
            .content()
            .has_blob(&replica_manifest.content_hash)
            .expect("logical blob check"),
        "chunked patch stage should not materialize a full replacement blob"
    );

    drop(replica_dfs);
    drop(owner_dfs);
    runtime.close().unwrap();
}

#[test]
fn sifs_posix_write_at_retries_lost_patch_stage_as_patch() {
    let (mut runtime, owner, replica, owner_browser, replica_dfs) =
        native_patch_services("sifs_posix_write_at_retries_lost_patch_stage_as_patch");
    drop(owner_browser);
    // Inspect and retry the same real Core that owns this pending patch.
    let mut owner_dfs = runtime.metadata_primary_service("/services").unwrap();
    let path = "/services/retry-write-at-patch.bin";

    let mut initial = vec![0_u8; 256 * 1024 + 31];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("initial self write");

    let first_observation = runtime.rpc_observations().len();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_STAGE_PATCH_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });

    let patch = b"lost patch stage should retry as patch";
    let patch_offset = 96 * 1024 + 5;
    owner_dfs
        .posix_write_file_at(
            path,
            patch_offset as u64,
            patch,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("patch write");

    let first_attempts = native_patch_attempts(&runtime, &owner, &replica, first_observation);
    assert!(
        first_attempts > 0,
        "the actual first stage must attempt a patch RPC"
    );
    for _ in 0..16 {
        // The pending patch belongs to this actual Core producer. Advance its
        // normal retry state before Main publishes the resulting durable work.
        owner_dfs
            .retry_pending_stages(8)
            .expect("actual producer patch retry");
        owner_dfs
            .process_incoming()
            .expect("actual producer patch completion");
        runtime
            .maintenance_tick(&owner, Duration::from_secs(5))
            .expect("normal pending-patch repair pass");
        if native_patch_attempts(&runtime, &owner, &replica, first_observation) > first_attempts {
            break;
        }
    }
    assert!(
        native_patch_attempts(&runtime, &owner, &replica, first_observation) > first_attempts,
        "the lost actual stage must retry as a patch before any semantic rejection"
    );
    runtime.clear_faults();
    drop(replica_dfs);
    drop(owner_dfs);
    runtime.close().unwrap();
}

#[test]
fn sifs_cluster_posix_contract_concurrent_write_at_truncate_races() {
    let mut cluster = sifs_posix_cluster("sifs-contract-write-at-truncate", "/users");
    let scenario = "concurrent-write-at-truncate-races";
    let path = "/users/contract-ranged.bin";
    let base = b"abcdefghij".to_vec();
    let mut observations = Vec::new();

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);
    sifs_cluster_publish_metadata_until(&mut cluster, "range race replica catch-up", |cluster| {
        super::cross_shard::node_store_object_at(&cluster._runtime, &cluster.observer_node, path)
            .is_some()
    });
    let base_stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("base stat");
    record_posix_observation(
        &mut observations,
        scenario,
        "seed",
        &cluster.writer_node,
        path,
        Instant::now(),
        "committed",
        Some(&base_stat),
    );

    let patch = b"XYZ".to_vec();
    let write_at = cluster.writer.posix_write_file_at(
        path,
        4,
        &patch,
        sifs_cluster_write_options(&cluster.writer_node.clone()),
    );
    let shrink = cluster.observer.posix_truncate_file(
        path,
        6,
        sifs_cluster_write_options(&cluster.observer_node.clone()),
    );
    let overwrite = cluster.primary.posix_write_file(
        path,
        b"whole-file",
        sifs_cluster_write_options(&cluster.primary_node.clone()),
    );
    for (op, node, result) in [
        ("write-at", cluster.writer_node.clone(), write_at),
        ("truncate-shrink", cluster.observer_node.clone(), shrink),
        ("whole-overwrite", cluster.primary_node.clone(), overwrite),
    ] {
        match result {
            Ok(stat) => record_posix_observation(
                &mut observations,
                scenario,
                op,
                &node,
                path,
                Instant::now(),
                "committed",
                Some(&stat),
            ),
            Err(DfsPosixError::PendingWrite { intent_id, .. }) => record_posix_observation(
                &mut observations,
                scenario,
                op,
                &node,
                path,
                Instant::now(),
                format!("pending:{intent_id}"),
                None,
            ),
            Err(DfsPosixError::Conflict { .. }) => {
                panic!("{op} created a DFS conflict during normal operation")
            }
            Err(DfsPosixError::Dfs(DfsError::Busy { .. })) => record_posix_observation(
                &mut observations,
                scenario,
                op,
                &node,
                path,
                Instant::now(),
                "retry-busy",
                None,
            ),
            Err(error) => panic!("{op} returned unexpected error: {error}"),
        }
    }
    sifs_posix_cluster_pump_until(&mut cluster, "write-at/truncate race settle", |cluster| {
        cluster.primary.posix_stat_distributed(path).is_ok()
            && sifs_routed_writes_settled(&cluster.writer)
            && sifs_routed_writes_settled(&cluster.observer)
    });

    let final_bytes = cluster
        .primary
        .posix_read_file_distributed(path)
        .expect("primary read after race");
    let mut patched = base.clone();
    patched.splice(4..7, patch);
    let shrunk = base[..6].to_vec();
    let mut extended_after_shrink = shrunk.clone();
    extended_after_shrink.resize(12, 0);
    let valid = [
        base.as_slice(),
        patched.as_slice(),
        shrunk.as_slice(),
        extended_after_shrink.as_slice(),
        b"whole-file".as_slice(),
    ];
    assert!(
        valid.contains(&final_bytes.as_slice()),
        "final bytes must match a serialized write_at/truncate/overwrite order: {final_bytes:?}"
    );

    cluster
        .writer
        .posix_truncate_file(
            path,
            12,
            sifs_cluster_write_options(&cluster.writer_node.clone()),
        )
        .unwrap_or_else(|error| panic!("truncate extension after race failed: {error}"));
    sifs_posix_cluster_pump_until(&mut cluster, "post-race truncate extension", |cluster| {
        cluster
            .writer
            .posix_stat_distributed(path)
            .is_ok_and(|stat| stat.size_bytes == Some(12))
    });
    let extended = cluster
        .writer
        .posix_read_file_distributed(path)
        .expect("extended read");
    assert_eq!(
        &extended[final_bytes.len().min(12)..],
        vec![0; 12usize.saturating_sub(final_bytes.len())].as_slice(),
        "truncate extension must zero-fill holes"
    );
    assert_sifs_cluster_no_posix_leaks(&cluster, scenario);
    finish_posix_observations(&observations);
    drop(cluster);
}

#[test]
fn sifs_cluster_posix_write_at_beyond_eof_reads_back_zero_filled_hole() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-hole", "/users");
    let path = "/users/sparse.bin";
    let base = b"0123456789".to_vec();
    let patch = b"PATCH".to_vec();
    let hole_offset = 16u64;

    sifs_cluster_routed_posix_write(&mut cluster, path, &base);

    let mut expected = base.clone();
    expected.resize(hole_offset as usize, 0);
    expected.extend_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    let options = sifs_cluster_write_options(&cluster.writer_node.clone());
    match cluster
        .writer
        .posix_write_file_at(path, hole_offset, &patch, options)
    {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed write_at failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "write_at hole commit", |cluster| {
        cluster
            .writer
            .posix_stat_distributed(path)
            .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    // POSIX hole semantics: bytes between the old EOF and the write offset
    // must read back as zeros.
    let read = cluster
        .writer
        .posix_read_file_distributed(path)
        .expect("writer read");
    assert_eq!(read, expected);
    let stat = cluster
        .writer
        .posix_stat_distributed(path)
        .expect("writer stat");
    assert_eq!(stat.size_bytes, Some(expected.len() as u64));

    // Range reads observe the same bytes (hole inclusive).
    let range = cluster
        .writer
        .posix_read_file_range_distributed(path, base.len() as u64, expected.len())
        .expect("writer range read");
    assert_eq!(range, expected[base.len()..].to_vec());

    // The primary agrees on the patched content.
    let read = cluster
        .primary
        .posix_read_file_distributed(path)
        .expect("primary read");
    assert_eq!(read, expected);

    drop(cluster);
}

#[test]
fn sifs_cluster_routed_posix_write_at_from_reader_streams_fallback_overlay() {
    let mut cluster = sifs_posix_cluster("sifs-cluster-routed-reader-overlay", "/users");
    let path = "/users/routed-reader-overlay.bin";
    // The final assertion reads locally on the writer. Make its intended
    // one-copy placement explicit for both the base and streamed replacement.
    let mut placement = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 1).unwrap();
    placement.required_node_ids = vec![cluster.writer_node.clone()];
    let accepted = cluster
        ._runtime
        .apply_placement_policy(
            &cluster.primary_node,
            path,
            DfsPlacementScope::Subtree,
            placement,
        )
        .unwrap();
    super::writes::finish_native_placement_fixture_apply(&cluster._runtime, &accepted);
    // Consume the accepted managed policy, not only candidate hints. Main's
    // shared placement view may otherwise choose another valid W1/N1 holder.
    let mut write_options = sifs_cluster_write_options(&cluster.writer_node);
    write_options.policy = cluster
        .writer
        .managed_write_replication_policy_for_path(path, DfsObjectPolicy::ReplicatedEventual, 1)
        .unwrap();
    assert_eq!(write_options.policy.min_commit_replicas, 1);
    assert_eq!(write_options.policy.desired_replicas, 1);
    assert_eq!(
        write_options.policy.required_node_ids,
        vec![cluster.writer_node.clone()]
    );

    let base = (0..256 * 1024 + 17)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let patch = (0..2 * 1024 * 1024 + 192 * 1024 + 31)
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    let patch_offset = base.len() + 64 * 1024 + 9;
    let mut expected = base.clone();
    expected.resize(patch_offset, 0);
    expected.extend_from_slice(&patch);
    let expected_hash = content_hash(&expected);

    sifs_cluster_routed_posix_write_with_options(&mut cluster, path, &base, write_options.clone());

    struct RoutedStreamingReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: Arc<AtomicUsize>,
    }

    impl std::io::Read for RoutedStreamingReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                assert_eq!(buffer.len(), 1, "POSIX write_at should only peek one byte");
            } else {
                assert!(
                    buffer.len() <= 1024 * 1024,
                    "the routed fallback read buffer must remain bounded, got {} bytes",
                    buffer.len()
                );
                self.bulk_reads.fetch_add(1, Ordering::SeqCst);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }

    let bulk_reads = Arc::new(AtomicUsize::new(0));
    let options = write_options;
    let write =
        sifs_cluster_writer_call_with_pump(&mut cluster, "routed reader overlay write_at", {
            let bulk_reads = Arc::clone(&bulk_reads);
            move |writer| {
                writer.posix_write_file_at_from_reader(
                    path,
                    patch_offset as u64,
                    RoutedStreamingReader {
                        bytes: patch,
                        offset: 0,
                        bulk_reads,
                    },
                    options,
                )
            }
        });
    match write {
        Ok(_) | Err(DfsPosixError::PendingWrite { .. }) => {}
        Err(error) => panic!("routed reader overlay write_at failed: {error}"),
    }
    sifs_posix_cluster_pump_until(&mut cluster, "routed reader overlay commit", |cluster| {
        sifs_routed_writes_settled(&cluster.writer)
            && cluster
                .writer
                .posix_stat_distributed(path)
                .is_ok_and(|stat| stat.content_hash.as_deref() == Some(expected_hash.as_str()))
    });

    assert_eq!(
        cluster
            .writer
            .posix_stat_distributed(path)
            .unwrap()
            .current_locations,
        vec![cluster.writer_node.clone()],
        "the streamed overlay has a committed writer copy"
    );

    assert!(
        bulk_reads.load(Ordering::SeqCst) > 2,
        "the caller's ranged-write input should be consumed incrementally"
    );
    assert_eq!(
        cluster
            .writer
            .posix_read_file_distributed(path)
            .expect("writer read"),
        expected
    );
    let primary = cluster
        .primary
        .posix_read_file_distributed(path)
        .expect("primary read");
    assert_eq!(primary, expected);

    drop(cluster);
}

fn native_patch_services(
    name: &str,
) -> (
    crate::test_support::DfsTestRuntime,
    NodeId,
    NodeId,
    NetworkedDfsService<crate::DfsNodeTransport>,
    NetworkedDfsService<crate::DfsNodeTransport>,
) {
    let (runtime, nodes) = crate::test_support::DfsTestRuntime::start_cluster(name, 2).unwrap();
    let owner = runtime
        .metadata_shards()
        .unwrap()
        .into_iter()
        .find(|shard| shard.shard_id == "services")
        .unwrap()
        .primary_node_id;
    let replica = nodes.into_iter().find(|node| *node != owner).unwrap();
    let owner_dfs = runtime.client(&owner).unwrap();
    let replica_dfs = runtime.client(&replica).unwrap();
    (runtime, owner, replica, owner_dfs, replica_dfs)
}

fn native_patch_attempts(
    runtime: &crate::test_support::DfsTestRuntime,
    owner: &NodeId,
    replica: &NodeId,
    after: usize,
) -> usize {
    let observations = runtime.rpc_observations();
    let actual = observations[after..]
        .iter()
        .filter(|call| call.source == *owner && call.target == *replica)
        .collect::<Vec<_>>();
    assert!(
        actual
            .iter()
            .all(|call| call.route != crate::DFS_DATA_STAGE_BLOB_ROUTE),
        "a patch retry cannot send a full blob before an explicit patch rejection"
    );
    let attempts = actual
        .iter()
        .filter(|call| {
            call.route == crate::DFS_DATA_STAGE_PATCH_ROUTE
                && call.phase == crate::test_runtime::RpcPhase::Constructed
        })
        .count();
    if attempts > 0 {
        assert!(
            actual.iter().any(|call| {
                call.route == crate::DFS_DATA_STAGE_PATCH_ROUTE
                    && call.phase == crate::test_runtime::RpcPhase::EndedWithoutResponse
            }),
            "the lost stage needs an actual failed normal RPC observation"
        );
    }
    attempts
}

#[test]
fn sifs_posix_write_at_retries_lost_patch_attachment() {
    let (runtime, owner, replica, owner_browser, replica_dfs) =
        native_patch_services("sifs_posix_write_at_retries_lost_patch_attachment");
    drop(owner_browser);
    // Inspect and retry the same real Core that owns this pending patch.
    let mut owner_dfs = runtime.metadata_primary_service("/services").unwrap();
    let path = "/services/retry-write-at-patch.bin";

    let mut initial = vec![0_u8; 512 * 1024 + 11];
    for (index, byte) in initial.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    owner_dfs
        .posix_write_file(
            path,
            &initial,
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).unwrap(),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: true,
                foreground_replica_repair: true,
            },
        )
        .expect("initial self write");

    let first_observation = runtime.rpc_observations().len();
    runtime.install_fault(crate::test_runtime::FaultRule {
        source: Some(owner.clone()),
        target: Some(replica.clone()),
        route: Some(crate::DFS_DATA_STAGE_PATCH_ROUTE.into()),
        call_id: None,
        origin: Some(crate::test_runtime::RpcOrigin::RegisteredHost),
        remaining_hits: 32,
        action: crate::test_runtime::FaultAction::BudgetExhausted,
    });

    let mut patch = vec![0_u8; 4 * 1024 * 1024 + 160 * 1024 + 5];
    for (index, byte) in patch.iter_mut().enumerate() {
        *byte = (index % 239) as u8;
    }
    let patch_hash = content_hash(&patch);
    struct StreamingPatchReader {
        bytes: Vec<u8>,
        offset: usize,
        bulk_reads: std::rc::Rc<std::cell::Cell<usize>>,
    }
    impl std::io::Read for StreamingPatchReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == 0 {
                if buffer.len() != 1 {
                    return Err(std::io::Error::other(format!(
                        "expected initial POSIX peek buffer, got {}",
                        buffer.len()
                    )));
                }
            } else if buffer.len() != 64 * 1024 {
                return Err(std::io::Error::other(format!(
                    "expected content-store streaming buffer, got {}",
                    buffer.len()
                )));
            } else {
                self.bulk_reads.set(self.bulk_reads.get() + 1);
            }
            if self.offset >= self.bytes.len() {
                return Ok(0);
            }
            let len = buffer.len().min(self.bytes.len() - self.offset);
            buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
            self.offset += len;
            Ok(len)
        }
    }
    let bulk_reads = std::rc::Rc::new(std::cell::Cell::new(0));
    owner_dfs
        .posix_write_file_at_from_reader(
            path,
            96 * 1024 + 5,
            StreamingPatchReader {
                bytes: patch.clone(),
                offset: 0,
                bulk_reads: bulk_reads.clone(),
            },
            DfsWriteOptions {
                policy: DfsReplicationPolicy::new(2, 2).expect("strict policy"),
                replica_candidates: vec![owner.clone(), replica.clone()],
                wait_for_commit: false,
                foreground_replica_repair: true,
            },
        )
        .expect("patch write");
    assert!(
        bulk_reads.get() > 1,
        "replicated write_at reader should stream the patch through the content store"
    );
    let pending = owner_dfs
        .writes
        .pending_writes
        .values()
        .next()
        .expect("pending patch write");
    let pending_patch = pending.patch.as_ref().expect("pending patch metadata");
    assert_eq!(pending_patch.patch_hash, patch_hash);
    assert_eq!(pending_patch.patch_size_bytes, patch.len() as u64);
    assert!(
        owner_dfs
            .content()
            .has_blob(&pending_patch.patch_hash)
            .expect("pending patch blob exists"),
        "pending patch retry should stream from the content-addressed patch blob"
    );

    let first_attempts = native_patch_attempts(&runtime, &owner, &replica, first_observation);
    assert!(
        first_attempts > 0,
        "the actual first stage must attempt a patch RPC"
    );
    for _ in 0..16 {
        // The pending patch belongs to this actual Core producer. Advance its
        // normal retry state before Main publishes the resulting durable work.
        owner_dfs
            .retry_pending_stages(8)
            .expect("actual producer patch retry");
        owner_dfs
            .process_incoming()
            .expect("actual producer patch completion");
        runtime
            .maintenance_tick(&owner, Duration::from_secs(5))
            .expect("normal pending-patch repair pass");
        if native_patch_attempts(&runtime, &owner, &replica, first_observation) > first_attempts {
            break;
        }
    }
    assert!(
        native_patch_attempts(&runtime, &owner, &replica, first_observation) > first_attempts,
        "the lost actual stage must retry as a patch before any semantic rejection"
    );
    runtime.clear_faults();
    drop(replica_dfs);
    drop(owner_dfs);
    runtime.close().unwrap();
}
