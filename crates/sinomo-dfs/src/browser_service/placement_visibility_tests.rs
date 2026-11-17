//! Policy commits and local configuration publication use their actual native owners.
use super::write_options_tests::{native_browser_policy, native_browser_runtime};
use super::*;
use crate::test_support::DfsTestRuntime;
use sinomo_core::node::NodeId;

fn prepare(
    native: &DfsTestRuntime,
    caller: &NodeId,
    path: &str,
    timeout_millis: u64,
) -> (
    Arc<DfsBrowserBackend>,
    crate::DaemonDfsBrowserPlacementUpdateV2Request,
) {
    let mut client = native.client(caller).unwrap();
    client.posix_mkdir_all(path).unwrap();
    drop(client);
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::new(native.data_dir(caller).clone()),
        &native.runtime_handle(),
    )
    .unwrap();
    let old = backend
        .stat(crate::DaemonDfsBrowserStatRequest {
            path: path.into(),
            max_staleness_millis: 0,
        })
        .unwrap()
        .into_result()
        .unwrap();
    assert_eq!(old.placement.unwrap().policy.desired_replicas, 1);
    let policy =
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 2)
            .unwrap();
    let preview: crate::DaemonDfsBrowserPlacementPreviewResponse = native
        .rpc_request(
            caller,
            caller,
            ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
                DAEMON_RPC_CATEGORY,
                &crate::DaemonDfsBrowserPlacementPreviewRequest {
                    path: path.into(),
                    policy: Some(policy.clone()),
                    scope: crate::DfsPlacementScope::Subtree,
                },
            )
            .unwrap(),
            Duration::from_secs(10),
        )
        .unwrap()
        .decode_body()
        .unwrap();
    assert!(
        preview.preview.is_satisfiable(),
        "actual preview is not eligible: {:?}",
        preview.preview.blocking_diagnostics
    );
    (
        backend,
        crate::DaemonDfsBrowserPlacementUpdateV2Request {
            path: path.into(),
            policy: Some(policy),
            scope: crate::DfsPlacementScope::Subtree,
            expected_preview_token: preview.preview.preview_token,
            timeout_millis: Some(timeout_millis),
        },
    )
}

fn apply(
    native: &DfsTestRuntime,
    caller: &NodeId,
    request: crate::DaemonDfsBrowserPlacementUpdateV2Request,
) -> tokio::task::JoinHandle<Result<crate::DaemonDfsBrowserPlacementUpdateResponse, String>> {
    let future = native.rpc_result_future(
        caller,
        caller,
        sinomo_core::network::ObjectRpcCall::value(
            ObjectRpcRequest::encode_body(
                DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
                DAEMON_RPC_CATEGORY,
                &request,
            )
            .unwrap(),
        ),
        Duration::from_secs(20),
    );
    native.runtime_handle().spawn(async move {
        future
            .await?
            .into_materialized_value_response(16 * 1024 * 1024)
            .await
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())
    })
}

fn jobs_for(
    native: &DfsTestRuntime,
    observer: &NodeId,
    path: &str,
) -> Vec<crate::DfsPlacementReconciliationJob> {
    // This observes the actual KV outcome independently of the DFS publication
    // deliberately held by the fault. Opening another filesystem client would
    // itself join that held publication and cannot inspect the committed job.
    let mut control = native.configuration_publisher(observer);
    crate::DfsControlPlaneStore::new(control.as_mut())
        .placement_reconciliations(&crate::sifs_file_system_id())
        .unwrap()
        .into_iter()
        .filter(|job| job.path == path)
        .collect()
}

#[test]
fn placement_visibility_wait_releases_fifo_and_accepts_a_later_published_cut() {
    for supersede in [false, true] {
        let name = if supersede {
            "placement-visibility-later-cut"
        } else {
            "placement-visibility-exact-cut"
        };
        let (native, nodes) = native_browser_runtime(name, 3);
        let caller = &nodes[2];
        let path = "/documents/policy-visibility";
        let (backend, request) = prepare(&native, caller, path, 10_000);
        let opens = dfs_browser_backend_open_count(native.data_dir(caller));
        let gap = native
            .lose_configuration_delta_and_pause_rebuild(
                caller,
                crate::file_system_config_key(&crate::sifs_file_system_id()),
            )
            .unwrap();
        let pending = apply(&native, caller, request);
        let revision = gap
            .wait_dropped_delta(Duration::from_secs(5))
            .unwrap()
            .revision;
        // The existing gap detector sees a withheld observation when a later
        // real commit arrives. Preserve both native updates; never synthesize
        // a configuration view or a successful acknowledgement.
        native_browser_policy(
            &crate::SharedDfsControlPlaneKvClient::new(native.configuration_publisher(&nodes[0])),
            if supersede {
                path
            } else {
                "/documents/unrelated-publication"
            },
            crate::DfsPlacementPolicy::new(
                sinomo_core::DfsObjectPolicy::ReplicatedEventual,
                1,
                if supersede { 3 } else { 1 },
            )
            .unwrap(),
        );
        gap.wait_rebuild(Duration::from_secs(5)).unwrap();
        assert!(
            !pending.is_finished(),
            "Apply must not report visibility through a held real rebuild"
        );
        native.runtime_handle().block_on(async {
            let fifo = tokio::time::timeout(
                Duration::from_secs(1),
                acquire_browser_fifo_for_test(native.data_dir(caller)),
            )
            .await
            .expect("post-commit visibility must release the browser FIFO")
            .unwrap();
            drop(fifo);
        });
        assert!(
            backend.service.try_lock().is_ok(),
            "no service mutex across the publication wait"
        );
        let original_jobs = jobs_for(&native, &nodes[0], path);
        assert_eq!(original_jobs.len(), 1);
        gap.release();
        let result = native.runtime_handle().block_on(pending).unwrap().unwrap();
        result.require_published().unwrap();
        assert_eq!(result.committed_revision, revision.0);
        assert_eq!(result.reconciliation.job_id, original_jobs[0].job_id);
        assert_eq!(
            result.reconciliation.revision, 0,
            "return the exact accepted job, not later progress"
        );
        let current = backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: path.into(),
                max_staleness_millis: 60_000,
            })
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(
            current.placement.unwrap().policy.desired_replicas,
            if supersede { 3 } else { 2 }
        );
        assert_eq!(
            jobs_for(&native, &nodes[0], path).len(),
            1,
            "visibility never resubmits Apply"
        );
        assert_eq!(
            dfs_browser_backend_open_count(native.data_dir(caller)),
            opens
        );
        drop(gap);
        drop(backend);
        native.close().unwrap();
    }
}

#[test]
fn placement_visibility_deadline_returns_the_committed_job_without_reapplying() {
    let (native, nodes) = native_browser_runtime("placement-visibility-deadline", 3);
    let caller = &nodes[2];
    let path = "/documents/policy-deadline";
    let (backend, request) = prepare(&native, caller, path, 2_000);
    let gap = native
        .lose_configuration_delta_and_pause_rebuild(
            caller,
            crate::file_system_config_key(&crate::sifs_file_system_id()),
        )
        .unwrap();
    let pending = apply(&native, caller, request);
    let revision = gap
        .wait_dropped_delta(Duration::from_secs(5))
        .unwrap()
        .revision;
    native_browser_policy(
        &crate::SharedDfsControlPlaneKvClient::new(native.configuration_publisher(&nodes[0])),
        "/documents/deadline-followup",
        crate::DfsPlacementPolicy::new(sinomo_core::DfsObjectPolicy::ReplicatedEventual, 1, 1)
            .unwrap(),
    );
    gap.wait_rebuild(Duration::from_secs(5)).unwrap();
    let result = native.runtime_handle().block_on(pending).unwrap().unwrap();
    assert!(matches!(
        result.visibility,
        crate::DfsPlacementConfigurationVisibility::Pending { .. }
    ));
    assert_eq!(result.committed_revision, revision.0);
    let error = result.require_published().unwrap_err();
    assert!(error.contains(&result.reconciliation.job_id));
    assert!(!error.contains(crate::DFS_PLACEMENT_APPLY_REJECTED_PREFIX));
    let jobs = jobs_for(&native, &nodes[0], path);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, result.reconciliation.job_id);
    gap.release();
    native
        .runtime_handle()
        .block_on(backend.host.placement_configuration_published(revision))
        .unwrap();
    assert_eq!(
        backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: path.into(),
                max_staleness_millis: 0,
            })
            .unwrap()
            .into_result()
            .unwrap()
            .placement
            .unwrap()
            .policy
            .desired_replicas,
        2
    );
    assert_eq!(jobs_for(&native, &nodes[0], path).len(), 1);
    drop(gap);
    drop(backend);
    native.close().unwrap();
}

#[test]
fn placement_expired_before_execution_does_not_commit_a_job() {
    let (native, nodes) = native_browser_runtime("placement-expired-before-effect", 3);
    let caller = &nodes[2];
    let path = "/documents/policy-not-applied";
    let (backend, request) = prepare(&native, caller, path, 0);
    let error = native
        .runtime_handle()
        .block_on(apply(&native, caller, request))
        .unwrap()
        .unwrap_err();
    assert!(
        error.contains("deadline elapsed before the placement mutation"),
        "{error}"
    );
    assert!(jobs_for(&native, &nodes[0], path).is_empty());
    drop(backend);
    native.close().unwrap();
}

#[test]
fn placement_lost_commit_reply_remains_unknown_and_does_not_resubmit() {
    use sinomo_core::kv::KvOp;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (native, nodes) = native_browser_runtime("placement-lost-commit-reply", 3);
    let caller = &nodes[2];
    let path = "/documents/policy-lost-reply";
    let (backend, request) = prepare(&native, caller, path, 10_000);
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    let prefix = crate::placement_reconciliations_prefix(&crate::sifs_file_system_id());
    let lost = native
        .native_kv_reply_hook(caller, move |request| {
            if request.txn.success.iter().any(|op| {
                matches!(op,
            KvOp::Put { key, .. } if key.starts_with(&prefix))
            }) {
                observed.fetch_add(1, Ordering::SeqCst);
                return Err(ServiceError::Network(
                    sinomo_core::network::NetworkError::delivery_failed(
                        "placement native lost reply",
                        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                        "reply lost after the actual placement commit",
                    ),
                ));
            }
            Ok(())
        })
        .unwrap();
    let error = native
        .runtime_handle()
        .block_on(apply(&native, caller, request))
        .unwrap()
        .unwrap_err();
    assert!(
        error.contains("reply lost after the actual placement commit"),
        "{error}"
    );
    assert!(!error.contains(crate::DFS_PLACEMENT_APPLY_REJECTED_PREFIX));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(jobs_for(&native, &nodes[0], path).len(), 1);
    drop(lost);
    drop(backend);
    native.close().unwrap();
}
