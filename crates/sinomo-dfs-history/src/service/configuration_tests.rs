//! Reproduce a real configuration withdrawal after the first ready observation.
use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn native_history_restore_replans_only_before_the_original_job_is_admitted() {
    let name = "history-preeffect-configuration-replan";
    let nodes = sinomo_dfs::test_support::DfsTestRuntime::ordered_node_ids(name, 1);
    let mut native = sinomo_dfs::test_support::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
    native
        .configure_component_factories(vec![Arc::new(crate::component_factory().unwrap())])
        .unwrap();
    native.start_nodes().unwrap();
    let mut writer = native.client(&nodes[0]).unwrap();
    let saved = writer
        .posix_write_file(
            "/services/preeffect-history-source",
            b"retained source bytes",
            sinomo_dfs::DfsWriteOptions::replicated_eventual(nodes.clone()),
        )
        .unwrap();
    let source = DfsFileVersionIdentity {
        file_system_id: saved.file_system_id.clone(),
        object_id: saved.object_id.clone(),
        version: saved.version.clone(),
    };
    writer
        .file_history_pin(DfsFileHistoryPinRequest {
            path: saved.path.clone(),
            identity: source.clone(),
            operation: DfsFileHistoryPinOperation::Capture {
                pin_id: "preeffect-source".into(),
            },
            operation_id: sinomo_core::Id::generate().to_string(),
        })
        .unwrap();
    let request = DfsFileHistoryRestoreRequest {
        source,
        destination_path: "/services/preeffect-history-restored".into(),
        expected_destination: None,
        operation_id: sinomo_core::Id::generate().to_string(),
    };
    let job_id = crate::validation::file_restore_job_id(&nodes[0], &request.operation_id);
    let original_request = request.clone();
    // The node's durable store; the delegating writer keeps none.
    let before = native
        .node_metadata(&nodes[0])
        .unwrap()
        .journal_last_sequence()
        .unwrap();
    let writer = Arc::new(Mutex::new(writer));
    let publisher = Arc::new(Mutex::new(native.configuration_publisher(&nodes[0])));
    let data_dir = Arc::new(native.data_dir(&nodes[0]).clone());
    let handle = native.runtime_handle();
    let host = dfs_browser_component_host(&data_dir).unwrap();
    let backend = Arc::new(
        FeatureBackend::for_data_dir(Arc::clone(&data_dir), &handle, Arc::clone(&host)).unwrap(),
    );
    let gap = Arc::new(
        native
            .lose_configuration_delta_and_pause_rebuild(
                &nodes[0],
                sinomo_dfs::file_system_config_key(&saved.file_system_id),
            )
            .unwrap(),
    );
    let attempts = Arc::new(AtomicUsize::new(0));
    let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
    let pending_tx = Arc::new(Mutex::new(Some(pending_tx)));
    let attempt_publisher = Arc::clone(&publisher);
    let attempt_backend = Arc::clone(&backend);
    let attempt_gap = Arc::clone(&gap);
    let attempt_count = Arc::clone(&attempts);
    let attempt_original = original_request.clone();
    let attempt_pending = Arc::clone(&pending_tx);
    let attempt_file_system = saved.file_system_id.clone();
    handle.block_on(async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let mut call = Box::pin(run_configured_browser_task(
            Arc::clone(&data_dir), DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE.into(),
            "native history preeffect replan", handle.clone(), Arc::clone(&host), request,
            move |payload| {
                if attempt_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    // The shared runner already observed Ready. Publish real
                    // settings now, before this worker's configuration borrow.
                    let mut publisher = attempt_publisher.lock().unwrap();
                    let mut store = sinomo_dfs::DfsControlPlaneStore::new(publisher.as_mut());
                    for keep_last in [1, 2] {
                        let expected = store.get_file_system_config(&attempt_file_system).unwrap().unwrap();
                        let policy = DfsFileHistoryPolicy { rules: vec![DfsFileHistoryRule {
                            path: "/services/unrelated-preeffect-policy".into(),
                            retention: DfsFileHistoryRetention { keep_last, keep_for_seconds: 0 },
                        }] };
                        let mut desired = expected.clone();
                        desired.retention_policies.insert(crate::HISTORY_MODULE_ID.into(), crate::encode_history_policy(&policy));
                        assert!(store.compare_and_put_file_system_config(Some(&expected), &desired).unwrap());
                    }
                    drop(publisher);
                    attempt_gap.wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).unwrap();
                    assert!(attempt_gap.dropped_delta().is_some());
                }
                let result = attempt_backend.try_restore_file_history(payload)?;
                if let DfsBrowserMutationAttempt::ConfigurationPending(original) = &result {
                    assert_eq!(original, &attempt_original);
                    if let Some(pending) = attempt_pending.lock().unwrap().take() {
                        pending.send(()).unwrap();
                    }
                }
                Ok(result)
            },
        ));
        tokio::select! {
            pending = tokio::time::timeout_at(deadline, pending_rx) => pending.unwrap().unwrap(),
            result = &mut call => panic!("restore completed before typed native refusal: {result:?}"),
        }
        // The typed refusal owns no durable job, intent, parent or FIFO permit.
        let fifo = tokio::time::timeout_at(deadline,
            sinomo_dfs::browser_test_support::acquire_browser_fifo_for_test(&data_dir),
        ).await.unwrap().unwrap();
        drop(fifo);
        {
            let metadata = native.node_metadata(&nodes[0]).unwrap();
            assert_eq!(metadata.journal_last_sequence().unwrap(), before);
            assert!(metadata.get_namespace_job(&job_id).unwrap().is_none());
            assert!(metadata.get_namespace_job_receipt(&job_id).unwrap().is_none());
            assert!(metadata.get_object_by_path(&saved.file_system_id, &original_request.destination_path).unwrap().is_none());
        }
        gap.release();
        let response = tokio::time::timeout_at(deadline, call).await.unwrap().unwrap();
        assert_eq!(response.path, original_request.destination_path);
        let mut writer = writer.lock().unwrap();
        assert_eq!(writer.posix_read_file_distributed(&response.path).unwrap(), b"retained source bytes");
        let intents = native.node_metadata(&nodes[0]).unwrap().intents_for_object_version_page(
            &response.committed.object_id, &response.committed.version, None, 16,
        ).unwrap();
        assert_eq!(intents.intents.iter().filter(|intent| intent.status == sinomo_dfs::DfsWriteIntentStatus::Committed).count(), 1);
    });
    assert!(
        (2..=3).contains(&attempts.load(Ordering::SeqCst)),
        "a real refused plan and one execution stay within the existing replan bound"
    );
    drop(pending_tx);
    drop(gap);
    drop(backend);
    drop(host);
    drop(writer);
    drop(publisher);
    native.close().unwrap();
}
