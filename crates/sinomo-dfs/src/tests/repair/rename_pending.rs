use super::*;
use crate::test_runtime::{DfsTestRuntime, FaultAction, FaultRule};

// This owns only its negative metadata carrier rules. Every unwind restores
// those rules; no queue, native body, fate or unrelated fault is rewritten.
struct MetadataAckFault<'a> {
    runtime: &'a DfsTestRuntime,
    source: NodeId,
    target: NodeId,
    armed: bool,
}
impl MetadataAckFault<'_> {
    fn heal(&mut self) -> Vec<usize> {
        if !self.armed {
            return Vec::new();
        }
        self.armed = false;
        let mut consumed = Vec::new();
        self.runtime.remove_faults_matching(|rule| {
            let selected = rule.source.as_ref() == Some(&self.source)
                && rule.target.as_ref() == Some(&self.target)
                && rule.call_id.is_none()
                && rule.origin.is_none()
                && matches!(
                    rule.route.as_deref(),
                    Some(
                        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                            | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE
                    )
                )
                && matches!(&rule.action, FaultAction::BudgetExhausted);
            if selected {
                consumed.push(32 - rule.remaining_hits);
            }
            selected
        });
        consumed
    }
}
impl Drop for MetadataAckFault<'_> {
    fn drop(&mut self) {
        let _ = self.heal();
    }
}

#[test]
fn native_pending_rename_batch_recovers_before_next_bounded_plan() {
    const FROM: &str = "/users/native-pending-rename-old";
    const TO: &str = "/users/native-pending-rename-new";
    let (runtime, mut browser, shard) = crate::test_support::current_native_metadata_service(
        "native-pending-rename-preplan",
        3,
        FROM,
    )
    .unwrap();
    let primary = shard.primary_node_id.clone();
    let assigned = std::iter::once(primary.clone())
        .chain(shard.replica_node_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(assigned.len(), 3, "actual native metadata assignment");
    browser.posix_mkdir_all(&format!("{FROM}/nested")).unwrap();
    let mut seeds = Vec::new();
    for index in 0..4 {
        let bytes = format!("pending-rename-child-{index}").into_bytes();
        let stat = browser
            .posix_write_file(
                &format!("{FROM}/nested/{index}.txt"),
                &bytes,
                DfsWriteOptions::replicated_eventual(vec![primary.clone()]),
            )
            .unwrap();
        seeds.push((stat, bytes));
    }
    drop(browser);

    let mut core = runtime.metadata_primary_service(FROM).unwrap();
    assert_eq!(core.node_id(), &primary);
    let scheduler =
        crate::runtime_host::daemon_dfs_mutation_scheduler(runtime.data_dir(&primary)).unwrap();
    scheduler.request_foreground_admission();
    let handle = runtime.runtime_handle();
    let admission = handle
        .block_on(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                scheduler.admit(&crate::DfsMessageExecutionScope::GlobalExclusive),
            )
            .await
        })
        .expect("actual Global admission keeps the original network budget")
        .unwrap();
    let actual_owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(runtime.data_dir(&primary)),
    )
    .unwrap();
    let execution = core.publication_execution.install(
        crate::publication_guard::DfsPublicationExecutionOwners::from_runtime(
            runtime.data_dir(&primary).root(),
            crate::DfsMessageExecutionScope::GlobalExclusive,
            admission,
            actual_owner,
        ),
    );
    assert!(scheduler.global_is_exclusively_held());
    let mut job = core.posix_rename_directory_bounded(FROM, TO, 1).unwrap();
    job = core
        .posix_resume_namespace_job_bounded(&job.job_id, 1)
        .unwrap();
    assert!(
        matches!(job.pending_frames.last(),
        Some(DfsNamespaceJobFrame::RenameDirectoryScan { source_path, cursor: None, .. })
            if source_path == &format!("{FROM}/nested")),
        "the real job must be at its unvisited four-child frame: {job:?}"
    );
    let saved_job = job.clone();
    let target = shard.replica_node_ids.last().unwrap().clone();
    for route in [
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE,
    ] {
        runtime.install_fault(FaultRule {
            source: Some(primary.clone()),
            target: Some(target.clone()),
            route: Some(route.into()),
            call_id: None,
            origin: None,
            remaining_hits: 32,
            action: FaultAction::BudgetExhausted,
        });
    }
    let mut faults = MetadataAckFault {
        runtime: &runtime,
        source: primary.clone(),
        target,
        armed: true,
    };
    let failed_started = Instant::now();
    let first = core.posix_resume_namespace_job_bounded(&job.job_id, 4);
    let failed_elapsed = failed_started.elapsed();
    let backend = core.metadata_backend().unwrap();
    let pending = backend.list_prepared_operations().unwrap();
    let after_job = backend.get_namespace_job(&job.job_id).unwrap();
    let snapshot = backend
        .export_shard_table_snapshot(&DfsMetadataShardId::new(&shard.shard_id))
        .unwrap();
    let output = runtime
        .data_dir(&primary)
        .root()
        .join("pending-rename-native-control.json");
    std::fs::write(&output, serde_json::to_vec_pretty(&serde_json::json!({
        "qualification":"real native pending-prepare component control; no hosted preemption claim",
        "first_result":format!("{first:?}"), "failed_elapsed_nanos":failed_elapsed.as_nanos(),
        "actual_assignment":shard, "job_before":saved_job, "actual_job_after":after_job,
        "actual_pending_prepares":pending, "actual_native_snapshot":snapshot,
        "actual_carriers_debug":format!("{:#?}",runtime.rpc_observations()),
    })).unwrap()).unwrap();
    assert!(
        matches!(&first, Err(DfsPosixError::Dfs(DfsError::MetadataRecoveryQuorumUnavailable {
        shard_id, required: 3, verified: 2, ..
    })) if shard_id == &shard.shard_id),
        "the actual named ACK fault must reach the normal native3/2 gate: {first:?}"
    );
    assert!(failed_elapsed < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    assert_eq!(
        after_job.as_ref(),
        Some(&saved_job),
        "a failed move cannot advance its durable cursor"
    );
    let candidates = pending
        .into_iter()
        .filter(|record| {
            record.file_system_id == shard.file_system_id
                && record.shard_id.0 == shard.shard_id
                && record.transaction.objects.len() == 4
                && record.transaction.objects.iter().all(|object| {
                    seeds.iter().any(|(seed, _)| {
                        seed.object_id == object.object_id
                            && seed.version == object.version
                            && object.path == seed.path.replacen(FROM, TO, 1)
                    })
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        candidates.len(),
        1,
        "exactly one actual four-child native body"
    );
    let original = candidates.into_iter().next().unwrap();
    original.validate().unwrap();
    assert_eq!(original.status, crate::DfsPreparedOperationStatus::Prepared);
    assert!(
        original.metadata_effects.is_empty(),
        "the legacy batch contains no job-cursor effect"
    );
    assert_eq!(original.digest.owner_epoch, shard.epoch);
    assert!(
        snapshot.as_ref().unwrap().materialized_through_sequence
            >= original.digest.prepare_sequence,
        "the actual native table cut includes this exact Prepared slot"
    );
    for (seed, bytes) in &seeds {
        assert!(
            seed.current_locations.contains(&primary),
            "actual seed holder, not candidate hint"
        );
        assert_eq!(
            backend.get_object(&seed.object_id).unwrap().unwrap().path,
            seed.path
        );
        assert!(
            backend
                .get_object_by_path(&seed.file_system_id, &seed.path.replacen(FROM, TO, 1))
                .unwrap()
                .is_none()
        );
        let (_manifest, actual_bytes) = core
            .content()
            .read_object_version(seed.object_id.as_str(), seed.version.as_str())
            .unwrap();
        assert_eq!(&actual_bytes, bytes);
        let hash = crate::content_store::content_hash(bytes);
        assert_eq!(seed.content_hash.as_deref(), Some(hash.as_str()));
    }
    let consumed = faults.heal();
    assert_eq!(
        consumed.len(),
        2,
        "only the two owned route rules were healed"
    );
    assert!(
        consumed.iter().any(|count| *count > 0),
        "the actual named metadata refusal was reached"
    );
    let next_started = Instant::now();
    let next = core.posix_resume_namespace_job_bounded(&job.job_id, 1);
    let next_elapsed = next_started.elapsed();
    // Export the actual next outcome BEFORE its unchanged success oracle.
    let after = core
        .metadata_backend()
        .unwrap()
        .prepared_operation_record(&original.operation_id)
        .unwrap();
    std::fs::write(
        output.with_file_name("pending-rename-next-outcome.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"next_result":format!("{next:?}"),
            "next_elapsed_nanos":next_elapsed.as_nanos(),"actual_original_after":after}))
        .unwrap(),
    )
    .unwrap();
    job = next.expect("the next normal one-step plan recovers the existing batch first");
    assert!(next_elapsed < sinomo_core::network::DEFAULT_NETWORK_TIMEOUT);
    let after = after.expect("the original native body remains retained");
    assert_eq!(after.status, crate::DfsPreparedOperationStatus::Committed);
    assert_eq!(after.digest, original.digest);
    assert_eq!(after.payload, original.payload);
    let finish_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    while job.status == DfsNamespaceJobStatus::Running {
        assert!(
            Instant::now() < finish_deadline,
            "finish keeps the existing bounded action budget"
        );
        job = core
            .posix_resume_namespace_job_bounded(&job.job_id, 1)
            .unwrap();
    }
    assert_eq!(job.status, DfsNamespaceJobStatus::Completed);
    assert!(matches!(
        core.posix_stat(FROM),
        Err(DfsPosixError::NotFound { .. })
    ));
    for (seed, bytes) in seeds {
        assert_eq!(
            core.posix_read_file_distributed(&seed.path.replacen(FROM, TO, 1))
                .unwrap(),
            bytes
        );
    }
    runtime.assert_fault_budgets().unwrap();
    assert_eq!(runtime.observation_dropped_count(), 0);
    drop(faults);
    drop(execution);
    drop(core);
    runtime.close().unwrap();
}
