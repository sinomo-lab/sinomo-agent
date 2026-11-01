//! Core-only native cold-browser admission; no optional history/attachment
//! factory is supplied to the actual Main fixture. The held gap is real native
//! configuration work, not a fabricated readiness response.
use super::write_options_tests::native_browser_runtime;
use super::*;
use std::task::Poll;

fn admission_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    })
}

struct Sink(tokio::sync::oneshot::Sender<sinomo_core::network::ObjectRpcOutgoingResponse>);
impl sinomo_core::service::ResponseSink for Sink {
    fn send(
        self: Box<Self>,
        response: sinomo_core::network::ObjectRpcOutgoingResponse,
    ) -> Result<(), ServiceError> {
        self.0
            .send(response)
            .map_err(|_| ServiceError::Service("native test response dropped".into()))
    }
}

struct Core {
    native: Option<crate::test_support::DfsTestRuntime>,
    data: Arc<SinomoDataDir>,
    node: sinomo_core::node::NodeId,
    host: Option<Arc<dyn DfsBrowserServiceHost>>,
    authority: Option<Arc<crate::DfsMetadataAuthorityRuntime>>,
    // Preowned normal native settings producer, independent of the DFS
    // configuration projection deliberately held by these controls.
    control: Option<Mutex<Box<dyn crate::DfsControlPlaneKvClient>>>,
    shards: Vec<crate::DfsMetadataShardConfig>,
    original: crate::DaemonDfsBrowserStatResponse,
}
impl Core {
    fn restarted(name: &str) -> Self {
        let (mut native, nodes) = native_browser_runtime(name, 1);
        let node = nodes[0].clone();
        let data = Arc::new(native.data_dir(&node).clone());
        let backend =
            DfsBrowserBackend::for_component_data_dir(Arc::clone(&data), &native.runtime_handle())
                .unwrap();
        let path = "/services/cold-browser-read/value.txt";
        backend
            .write_file(crate::DaemonDfsBrowserWriteFileRequest {
                path: path.into(),
                bytes: b"native core-only retained bytes".to_vec(),
            })
            .unwrap();
        let original = backend
            .stat(crate::DaemonDfsBrowserStatRequest {
                path: path.into(),
                max_staleness_millis: 0,
            })
            .unwrap()
            .into_result()
            .unwrap();
        drop(backend);
        // Actual Main component stop/reopen preserves the same native DATA.
        // No synthetic slot eviction substitutes for the lifecycle boundary.
        native.stop_node(&node).unwrap();
        native.reopen_node(node.clone()).unwrap();
        let slot = browser_backend_slot(&data).unwrap();
        assert!(
            slot.backend.lock().unwrap().is_none(),
            "actual restart leaves browser cold"
        );
        let host = dfs_browser_component_host(&data).unwrap();
        let paths = ClusterPaths::new(&data);
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
        let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
            &paths,
            &state.cluster_id,
            &node,
            Some(&native.runtime_handle()),
        )
        .unwrap();
        // Readiness comes from THIS actual attached owner, not from generic
        // native invariant/layout probes or an optional feature's startup.
        crate::test_runtime::executor().block_on(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                authority.ready_write_configuration_projection_async(),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        });
        assert!(matches!(
            authority.write_configuration_readiness().unwrap(),
            crate::DfsWriteConfigurationReadiness::Ready(Some(_))
        ));
        let mut publisher = native.configuration_publisher(&node);
        // Open/read the actual native configuration while healthy, before
        // any gap or denied floor. No late DFS startup opener is required.
        publisher
            .get(&crate::file_system_config_key(&crate::sifs_file_system_id()))
            .unwrap()
            .unwrap();
        let control = Mutex::new(publisher);
        let shards = native.metadata_shards().unwrap();
        Self {
            native: Some(native),
            data,
            node,
            host: Some(host),
            authority: Some(authority),
            control: Some(control),
            shards,
            original,
        }
    }
    fn native(&self) -> &crate::test_support::DfsTestRuntime {
        self.native.as_ref().unwrap()
    }
    fn gap(&self) -> crate::test_runtime::DfsTestNativeConfigurationFault {
        let key = crate::file_system_config_key(&crate::sifs_file_system_id());
        let mut control = self.control.as_ref().unwrap().lock().unwrap();
        let entry = control.get(&key).unwrap().unwrap();
        let marker_prefix = format!("/tests/sinomo-dfs/cold-browser/{}", self.original.object_id);
        let first_marker = format!("{marker_prefix}/first");
        let fault = self
            .native()
            .lose_configuration_delta_and_pause_rebuild(&self.node, first_marker.clone())
            .unwrap();
        let mut revisions = Vec::new();
        for (ordinal, value) in [
            ("first", "first native cold-browser companion"),
            ("second", "second native cold-browser companion"),
        ] {
            let marker_key = format!("{marker_prefix}/{ordinal}");
            let response = control
                .txn(KvTxn {
                    compares: vec![
                        sinomo_core::kv::KvCompare::ValueEquals {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                        },
                        sinomo_core::kv::KvCompare::Missing {
                            key: marker_key.clone(),
                        },
                    ],
                    success: vec![
                        KvOp::Put {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                            lease_id: entry.lease_id.clone(),
                        },
                        KvOp::put_serde(marker_key, value, None).unwrap(),
                    ],
                    failure: Vec::new(),
                })
                .unwrap();
            assert!(response.succeeded);
            assert!(response.revision > entry.modify_revision);
            revisions.push(response.revision);
        }
        assert!(revisions[1] > revisions[0]);
        fault
            .wait_rebuild(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        let dropped = fault.dropped_delta().unwrap();
        assert!(dropped.events.iter().any(|event| event.key == first_marker));
        let actual_entry = control.get(&key).unwrap().unwrap();
        assert_eq!(
            actual_entry.value, entry.value,
            "actual semantic FS configuration never changed"
        );
        assert!(matches!(
            self.authority
                .as_ref()
                .unwrap()
                .write_configuration_readiness()
                .unwrap(),
            crate::DfsWriteConfigurationReadiness::Pending
        ));
        self.save("native-cold-browser-gap.json",serde_json::json!({
            "actual_revisions":revisions,"entry_before":entry,"entry_after":actual_entry,
            "dropped_actual_delta":dropped,
            "qualification":"actual guarded native KV transactions; unchanged FS bytes and unique companion keys produce a real continuity gap without policy/job/epoch mutation",
        }));
        fault
    }
    fn cut(&self) -> serde_json::Value {
        let cuts = self
            .shards
            .iter()
            .map(|shard| {
                let id = crate::DfsMetadataShardId::new(&shard.shard_id);
                let mut read = crate::LocalSobjMetadataBackend::open_for_inspection(
                    id.clone(),
                    crate::local_dfs_metadata_shard_path(&self.data, &id),
                )
                .unwrap();
                read.begin_inspection_snapshot().unwrap();
                let prefix = crate::DfsMetadataBackend::metadata_prefix_proof(&read, &id).unwrap();
                let tables = read.export_table_snapshot();
                read.end_inspection_snapshot();
                serde_json::json!({"shard":id,"prefix":prefix,"tables":tables})
            })
            .collect::<Vec<_>>();
        serde_json::json!(cuts)
    }
    fn save(&self, name: &str, value: serde_json::Value) {
        std::fs::write(
            self.data.root().join(name),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    }
    fn stat_request(
        &self,
    ) -> (
        ServiceContext,
        ObjectRpcRequest,
        tokio::sync::oneshot::Receiver<sinomo_core::network::ObjectRpcOutgoingResponse>,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let context = ServiceContext::new(
            Arc::clone(&self.data),
            None,
            ServiceSigner::Local,
            sinomo_core::network::ObjectRpcIncomingBody::empty(),
            Box::new(Sink(tx)),
        );
        let request = ObjectRpcRequest::encode_body(
            DAEMON_DFS_BROWSER_STAT_ROUTE,
            DAEMON_RPC_CATEGORY,
            &crate::DaemonDfsBrowserStatRequest {
                path: self.original.path.clone(),
                max_staleness_millis: 0,
            },
        )
        .unwrap();
        (context, request, rx)
    }
}
impl Drop for Core {
    fn drop(&mut self) {
        self.host.take();
        self.authority.take();
        self.control.take();
        if let Some(native) = self.native.take() {
            let result = native.close();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}

#[test]
fn cold_core_browser_stat_after_actual_restart_joins_held_configuration_without_open_effects() {
    let core = Core::restarted("native-core-only-cold-browser-ready");
    let before = core.cut();
    let opens = dfs_browser_backend_open_count(&core.data);
    let fault = core.gap();
    let (context, request, receiver) = core.stat_request();
    let service = HostedDfsBrowserService::new(
        Some(core.native().runtime_handle()),
        Arc::clone(core.host.as_ref().unwrap()),
    );
    // Deliberately use the distinct admission executor. Backend initialization
    // must still use the existing host's actual native runtime.
    let (initial, outcome, returned) = admission_runtime().block_on(async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let mut future = Box::pin(service.handle(context, request));
        // This is a held-observation subwindow inside the SAME original five
        // second call, not a changed action deadline. A first poll alone could
        // merely enqueue the old blocking open and release the gap too early.
        let held_observation_end = tokio::time::Instant::now() + Duration::from_millis(150);
        let initial = match tokio::time::timeout_at(held_observation_end, future.as_mut()).await {
            Err(_) => Poll::Pending,
            Ok(result) => Poll::Ready(result),
        };
        let held_cold = browser_backend_slot(&core.data).unwrap().backend.lock().unwrap().is_none();
        let held_opens = dfs_browser_backend_open_count(&core.data);
        core.save("native-cold-browser-held.json", serde_json::json!({
            "initial":format!("{initial:?}"),"held_cold":held_cold,"opens_before":opens,"held_opens":held_opens,
            "actual_readiness":format!("{:?}",core.authority.as_ref().unwrap().write_configuration_readiness()),
            "core_only":"normal DfsTestRuntime supplies core DFS,KV and service-catalog factories; no optional factory supplied",
        }));
        fault.release();
        let outcome = match initial {
            Poll::Pending => tokio::time::timeout_at(deadline, future).await.map_err(|e|e.to_string())
                .and_then(|v|v.map_err(|e|e.to_string())),
            Poll::Ready(result) => result.map_err(|e|e.to_string()),
        };
        let returned = if outcome.is_ok() {
            let response = receiver.await.unwrap();
            let bytes = response.body.collect_to_vec(256 * 1024).await.unwrap();
            let decoded: crate::DaemonDfsBrowserStatOutcomeResponse =
                sinomo_core::network::ObjectRpcResponse::from_encoded_body(response.response.route,bytes)
                    .decode_body().unwrap();
            Some(decoded.into_result().unwrap())
        } else { None };
        ((held_cold, held_opens), outcome, returned)
    });
    let after = core.cut();
    core.save("native-cold-browser-final.json",serde_json::json!({
        "outcome":format!("{outcome:?}"),"returned":returned,"original":core.original,
        "before":before,"after":after,"opens_before":opens,"opens_after":dfs_browser_backend_open_count(&core.data),
    }));
    assert!(initial.0);
    assert_eq!(
        initial.1, opens,
        "no cold backend construction while actual owner is Pending"
    );
    outcome.unwrap();
    let returned = returned.unwrap();
    assert_eq!(returned.object_id, core.original.object_id);
    assert_eq!(returned.version, core.original.version);
    assert_eq!(returned.content_hash, core.original.content_hash);
    assert_eq!(
        before, after,
        "STAT adds no prepare,intent,namespace or metadata effects"
    );
    assert_eq!(dfs_browser_backend_open_count(&core.data), opens + 1);
}

#[test]
fn warm_core_browser_admission_does_not_preflight_an_unrelated_configuration_gap() {
    let core = Core::restarted("native-warm-browser-no-cold-preflight");
    let backend = DfsBrowserBackend::for_component_data_dir(
        Arc::clone(&core.data),
        &core.native().runtime_handle(),
    )
    .unwrap();
    let fault = core.gap();
    let opens = dfs_browser_backend_open_count(&core.data);
    let outcome = admission_runtime().block_on(async {
        let slot = browser_backend_slot(&core.data).unwrap();
        let busy = Arc::clone(&slot.execution).lock_owned().await;
        let unqueued = tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            run_blocking_browser_task_unqueued(
                Arc::clone(&core.data),
                "test-warm-unqueued",
                "unqueued",
                || Ok(19),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            unqueued, 19,
            "existing progress/attachment bypass must not join the namespace FIFO"
        );
        // The actual initialized-slot guard makes try_lock return WouldBlock.
        // UNKNOWN contention must not be relabelled cold and join Pending.
        let initialized = slot.backend.lock().unwrap();
        assert!(initialized.is_some());
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let callback_data = Arc::clone(&core.data);
        let callback_runtime = core.native().runtime_handle();
        let mut contended = Box::pin(run_blocking_browser_task_unqueued(
            Arc::clone(&core.data),
            "test-warm-contended",
            "warm contended",
            move || {
                // This is the real existing getter used by unqueued callers;
                // the held warm cache mutex prevents a toy instant reply.
                let _ =
                    DfsBrowserBackend::for_component_data_dir(callback_data, &callback_runtime)?;
                Ok(29)
            },
        ));
        let first = std::future::poll_fn(|cx| {
            Poll::Ready(std::future::Future::poll(contended.as_mut(), cx))
        })
        .await;
        assert!(
            matches!(first, Poll::Pending),
            "actual state observation waits for the held existing mutex"
        );
        drop(initialized);
        let contended = tokio::time::timeout_at(deadline, contended)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            contended, 29,
            "observed warm slot must not join the still-Pending configuration"
        );
        drop(busy);
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            run_blocking_browser_task(
                Arc::clone(&core.data),
                "test-warm-admission",
                "warm admission",
                || Ok(17),
            ),
        )
        .await
        .unwrap()
    });
    let still_pending = matches!(
        core.authority
            .as_ref()
            .unwrap()
            .write_configuration_readiness()
            .unwrap(),
        crate::DfsWriteConfigurationReadiness::Pending
    );
    fault.release();
    assert_eq!(outcome.unwrap(), 17);
    assert!(
        still_pending,
        "this checks wrapper no-touch only; warm stat's own native read admission remains unchanged"
    );
    assert_eq!(dfs_browser_backend_open_count(&core.data), opens);
    drop(backend);
}

#[test]
fn browser_admission_without_registered_host_does_not_create_a_runtime_or_backend() {
    let root = sinomo_testdir::TestDir::new("browser-standalone-admission-no-host");
    let data = Arc::new(SinomoDataDir::new(root.path()));
    assert!(
        browser_backend_slot(&data)
            .unwrap()
            .host
            .lock()
            .unwrap()
            .is_none()
    );
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let result = admission_runtime().block_on(run_blocking_browser_task(
        Arc::clone(&data),
        "test-no-host",
        "no host",
        move || {
            task_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(23)
        },
    ));
    assert_eq!(result.unwrap(), 23);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        browser_backend_slot(&data)
            .unwrap()
            .backend
            .lock()
            .unwrap()
            .is_none()
    );
    assert_eq!(dfs_browser_backend_open_count(&data), 0);
}

#[test]
fn expired_caller_deadline_cancels_cold_pending_join_without_open_or_effect() {
    let core = Core::restarted("native-cold-browser-expired-owner-join");
    let fault = core.gap();
    let before = core.cut();
    let opens = dfs_browser_backend_open_count(&core.data);
    let (context, request, _response) = core.stat_request();
    let service = HostedDfsBrowserService::new(
        Some(core.native().runtime_handle()),
        Arc::clone(core.host.as_ref().unwrap()),
    );
    let outcome = admission_runtime().block_on(async {
        let expired = tokio::time::Instant::now() - Duration::from_nanos(1);
        tokio::time::timeout_at(expired, service.handle(context, request)).await
    });
    let after = core.cut();
    let cold = browser_backend_slot(&core.data)
        .unwrap()
        .backend
        .lock()
        .unwrap()
        .is_none();
    fault.release();
    core.save("native-cold-browser-expired.json",serde_json::json!({
        "outcome":format!("{outcome:?}"),"cold":cold,"opens_before":opens,"opens_after":dfs_browser_backend_open_count(&core.data),
        "before":before,"after":after,"limit":"expired absolute caller budget while actual core owner is Pending; no general publication-fate claim",
    }));
    assert!(outcome.is_err());
    assert!(cold);
    assert_eq!(dfs_browser_backend_open_count(&core.data), opens);
    assert_eq!(before, after);
}

#[test]
fn cold_browser_queued_while_current_releases_fifo_to_join_a_later_native_gap() {
    let core = Core::restarted("native-cold-browser-fifo-gap");
    let before = core.cut();
    let opens = dfs_browser_backend_open_count(&core.data);
    let (context, request, receiver) = core.stat_request();
    let service = HostedDfsBrowserService::new(
        Some(core.native().runtime_handle()),
        Arc::clone(core.host.as_ref().unwrap()),
    );
    let outcome = admission_runtime().block_on(async {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let slot = browser_backend_slot(&core.data).unwrap();
        let admission = Arc::clone(&slot.execution).lock_owned().await;
        let mut future = Box::pin(service.handle(context,request));
        let first = std::future::poll_fn(|cx| Poll::Ready(std::future::Future::poll(future.as_mut(),cx))).await;
        assert!(matches!(first,Poll::Pending),"actual cold request queues behind the owned FIFO");
        assert_eq!(dfs_browser_backend_open_count(&core.data),opens);
        let fault = core.gap();
        drop(admission);
        let held_end = tokio::time::Instant::now()+Duration::from_millis(150);
        let observed = tokio::time::timeout_at(held_end,future.as_mut()).await;
        let cold = slot.backend.lock().unwrap().is_none();
        let held_opens = dfs_browser_backend_open_count(&core.data);
        let fifo_released = slot.execution.try_lock().is_ok();
        core.save("native-cold-browser-fifo-gap-held.json",serde_json::json!({
            "observed":format!("{observed:?}"),"cold":cold,"fifo_released":fifo_released,
            "opens_before":opens,"held_opens":held_opens,
            "actual_readiness":format!("{:?}",core.authority.as_ref().unwrap().write_configuration_readiness()),
        }));
        fault.release();
        let still_pending = observed.is_err();
        let result = match observed {
            Ok(value)=>value.map_err(|e|e.to_string()),
            Err(_)=>tokio::time::timeout_at(deadline,future).await.map_err(|e|e.to_string())
                .and_then(|v|v.map_err(|e|e.to_string())),
        };
        let returned = if result.is_ok() {
            let response = receiver.await.unwrap();
            let bytes = response.body.collect_to_vec(256*1024).await.unwrap();
            let decoded:crate::DaemonDfsBrowserStatOutcomeResponse =
                sinomo_core::network::ObjectRpcResponse::from_encoded_body(response.response.route,bytes)
                    .decode_body().unwrap();
            Some(decoded.into_result().unwrap())
        }else{None};
        (still_pending,cold,fifo_released,held_opens,result,returned)
    });
    let after = core.cut();
    core.save(
        "native-cold-browser-fifo-gap-final.json",
        serde_json::json!({
            "outcome":format!("{:?}",outcome.4),"returned":outcome.5,"before":before,"after":after,
        }),
    );
    assert!(outcome.0 && outcome.1 && outcome.2);
    assert_eq!(outcome.3, opens);
    outcome.4.unwrap();
    let returned = outcome.5.unwrap();
    assert_eq!(returned.object_id, core.original.object_id);
    assert_eq!(returned.version, core.original.version);
    assert_eq!(dfs_browser_backend_open_count(&core.data), opens + 1);
    assert_eq!(before, after);
}

#[test]
fn cold_browser_permanent_native_floor_failure_preserves_error_without_open() {
    let core = Core::restarted("native-cold-browser-floor-unavailable");
    let counter_scope = core.data.root();
    assert!(
        browser_backend_slot(&core.data)
            .unwrap()
            .backend
            .lock()
            .unwrap()
            .is_none()
    );
    let before = core.cut();
    let opens = dfs_browser_backend_open_count(&core.data);
    let denied = core.native().deny_configuration_floor(&core.node).unwrap();
    let (context, request, _response) = core.stat_request();
    let service = HostedDfsBrowserService::new(
        Some(core.native().runtime_handle()),
        Arc::clone(core.host.as_ref().unwrap()),
    );
    let outcome = admission_runtime().block_on(async {
        tokio::time::timeout(
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            service.handle(context, request),
        )
        .await
        .unwrap()
    });
    let after = core.cut();
    let error = outcome.unwrap_err().to_string();
    core.save(
        "native-cold-browser-unavailable.json",
        serde_json::json!({
            "error":error,"before":before,"after":after,"opens_before":opens,
            "opens_after":dfs_browser_backend_open_count(&core.data),
            "counter_scope":counter_scope,
            "counter_qualification":"existing open counter is keyed by this exact data_dir.root; no global reset or cross-target total",
        }),
    );
    assert!(
        error.contains("native configuration floor is unavailable at the injected read boundary"),
        "{error}"
    );
    assert_eq!(dfs_browser_backend_open_count(&core.data), opens);
    assert!(
        browser_backend_slot(&core.data)
            .unwrap()
            .backend
            .lock()
            .unwrap()
            .is_none()
    );
    assert_eq!(before, after);
    drop(denied);
}
