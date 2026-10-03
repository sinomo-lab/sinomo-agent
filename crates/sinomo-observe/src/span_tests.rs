use super::*;

fn recorder(capture_id: Option<&str>) -> (tracing::Dispatch, ObserveTraceLayer) {
    let layer = ObserveTraceLayer {
        trace_capacity: TRACE_BUFFER_LIMIT,
        logs: Arc::new(Mutex::new(crate::logs::LogBuffer::default())),
        traces: Arc::new(Mutex::new(VecDeque::new())),
        dropped_traces: Arc::new(AtomicU64::new(0)),
        capture: Arc::new(Mutex::new(
            capture_id.map(|id| ActiveObserveCapture::new(id.into(), None)),
        )),
    };
    (
        tracing::Dispatch::new(Registry::default().with(layer.clone())),
        layer,
    )
}

#[test]
fn stages_retain_explicit_parent_updates_and_event_overrides() {
    let (dispatch, layer) = recorder(Some("capture"));
    tracing::dispatcher::with_default(&dispatch, || {
        let parent = tracing::trace_span!(target: "sinomo.perf", "parent", stage = "test.parent", traceparent = "parent-trace", value = 1u64);
        parent.record("value", 2u64);
        let unrelated = tracing::info_span!("unrelated", traceparent = "wrong").entered();
        let child = tracing::trace_span!(target: "sinomo.perf", parent: &parent, "child", stage = "test.child").entered();
        tracing::trace!(target: "sinomo.perf", "inherits");
        tracing::trace!(target: "sinomo.perf", traceparent = "override", "explicit");
        drop(child);
        drop(unrelated);
    });
    let traces = layer.traces.lock().unwrap();
    let inherited = traces
        .iter()
        .find(|r| r.fields["message"] == "inherits")
        .unwrap();
    assert_eq!(inherited.traceparent.as_deref(), Some("parent-trace"));
    assert_eq!(inherited.spans.len(), 2);
    assert_eq!(inherited.spans[0].fields["value"], 2);
    assert_eq!(
        traces
            .iter()
            .find(|r| r.fields["message"] == "explicit")
            .unwrap()
            .traceparent
            .as_deref(),
        Some("override")
    );
    assert_eq!(
        traces
            .iter()
            .filter(|r| r.fields["record_kind"] == "span_started")
            .count(),
        2
    );
    assert_eq!(
        traces
            .iter()
            .filter(|r| r.fields["record_kind"] == "span_closed")
            .count(),
        2
    );
    assert!(
        traces
            .iter()
            .filter(|r| r.fields["record_kind"] == "span_closed")
            .all(|r| r.fields["capture_complete"] == true)
    );
    assert!(
        traces
            .iter()
            .zip(traces.iter().skip(1))
            .all(|(a, b)| a.sequence < b.sequence)
    );
}

#[test]
fn capture_cut_is_censored_and_does_not_retag_old_span() {
    let (dispatch, layer) = recorder(Some("first"));
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::trace_span!(target: "sinomo.perf", "work", stage = "work").entered();
        *layer.capture.lock().unwrap() = Some(ActiveObserveCapture::new("second".into(), None));
        drop(span);
    });
    let traces = layer.traces.lock().unwrap();
    let closed = traces
        .iter()
        .find(|r| r.fields["record_kind"] == "span_closed")
        .unwrap();
    assert_eq!(closed.capture_id.as_deref(), Some("first"));
    assert_eq!(closed.fields["capture_complete"], false);
}

#[test]
fn disabled_capture_does_not_record_span_lifetimes() {
    let (dispatch, layer) = recorder(None);
    tracing::dispatcher::with_default(&dispatch, || {
        let _span = tracing::trace_span!(target: "sinomo.perf", "work", stage = "work").entered();
    });
    assert!(layer.traces.lock().unwrap().is_empty());
}

#[test]
fn future_polling_on_different_threads_keeps_context() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    use tracing::Instrument;
    let (dispatch, layer) = recorder(Some("async"));
    let span = tracing::dispatcher::with_default(
        &dispatch,
        || tracing::trace_span!(target: "sinomo.perf", "async", stage = "test.async", traceparent = "rpc"),
    );
    let mut first = true;
    let future = std::future::poll_fn(move |_| {
        tracing::trace!(target: "sinomo.perf", "poll");
        if first {
            first = false;
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .instrument(span);
    let mut future = Box::pin(future);
    let first_dispatch = dispatch.clone();
    let mut future = std::thread::spawn(move || {
        tracing::dispatcher::with_default(&first_dispatch, || {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            )
        });
        future
    })
    .join()
    .unwrap();
    std::thread::spawn(move || {
        tracing::dispatcher::with_default(&dispatch, || {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
            )
        });
    })
    .join()
    .unwrap();
    let traces = layer.traces.lock().unwrap();
    let polls: Vec<_> = traces
        .iter()
        .filter(|r| r.fields["message"] == "poll")
        .collect();
    assert_eq!(polls.len(), 2);
    assert_eq!(polls[0].spans, polls[1].spans);
    assert!(
        polls
            .iter()
            .all(|r| r.traceparent.as_deref() == Some("rpc"))
    );
}

#[test]
fn file_io_records_exact_bytes_syncs_and_errors() {
    use sinomo_core::storage::io_trace;
    use std::io::{Seek, SeekFrom};
    let (dispatch, layer) = recorder(Some("io"));
    let path = std::env::temp_dir().join(format!(
        "sinomo-observe-io-{}-{}",
        std::process::id(),
        monotonic_test_id()
    ));
    tracing::dispatcher::with_default(&dispatch, || {
        let _span = tracing::trace_span!(target: "sinomo.perf", "operation", stage = "test.write")
            .entered();
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        io_trace::write_all(&mut file, b"hello").unwrap();
        io_trace::sync_all(&file).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = [0; 8];
        assert_eq!(io_trace::read(&mut file, &mut bytes).unwrap(), 5);
        let mut read_only = std::fs::File::open(&path).unwrap();
        assert!(io_trace::write_all(&mut read_only, b"x").is_err());
    });
    std::fs::remove_file(path).unwrap();
    let traces = layer.traces.lock().unwrap();
    let io: Vec<_> = traces
        .iter()
        .filter(|r| r.fields["record_kind"] == "span_closed" && r.fields.get("io_kind").is_some())
        .collect();
    assert_eq!(io.len(), 4);
    assert_eq!(io[0].fields["io_bytes"], 5);
    assert_eq!(io[1].fields["io_kind"], "disk.sync_all");
    assert_eq!(io[2].fields["io_bytes"], 5);
    assert_eq!(io[3].fields["outcome"], "error");
    assert_eq!(io[3].fields["io_bytes"], 0);
    assert!(
        io.iter()
            .all(|r| r.spans[0].fields["stage"] == "test.write")
    );
}

fn monotonic_test_id() -> u64 {
    NEXT_OBSERVE_SPAN_ID.fetch_add(1, Ordering::Relaxed)
}

#[test]
fn context_bounds_are_explicit_and_reused_capture_names_are_censored() {
    let (dispatch, layer) = recorder(Some("reused"));
    tracing::dispatcher::with_default(&dispatch, || {
        let parent =
            tracing::trace_span!(target: "sinomo.perf", "parent", stage = "parent").entered();
        let old_lease = layer.capture.lock().unwrap().as_ref().unwrap().expires_at;
        let mut replacement = ActiveObserveCapture::new("reused".into(), None);
        replacement.expires_at = old_lease + Duration::from_secs(1);
        *layer.capture.lock().unwrap() = Some(replacement);
        drop(parent);
        let mut nested = Vec::new();
        for _ in 0..20 {
            nested.push(tracing::info_span!("context", value = "x".repeat(4097)).entered());
        }
        tracing::info!("bounded");
        while nested.pop().is_some() {}
    });
    let records = layer.traces.lock().unwrap();
    let closed = records
        .iter()
        .find(|r| r.fields["record_kind"] == "span_closed")
        .unwrap();
    assert_eq!(closed.fields["capture_complete"], false);
    let bounded = records
        .iter()
        .find(|r| r.fields["message"] == "bounded")
        .unwrap();
    assert!(bounded.context_truncated);
    assert!(bounded.spans.len() <= TRACE_SCOPE_DEPTH_LIMIT);
    let bytes: usize = bounded
        .spans
        .iter()
        .map(|s| s.fields.to_string().len())
        .sum();
    assert!(bytes <= 17 * 1024);
}

#[test]
fn async_network_frames_record_matching_bytes_under_the_operation() {
    use sinomo_core::network::{
        ObjectFrameIoOptions, read_object_frame_async, write_object_frame_async,
    };
    use tracing::Instrument;
    let (dispatch, layer) = recorder(Some("network"));
    tracing::dispatcher::with_default(&dispatch, || {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let span = tracing::trace_span!(target: "sinomo.perf", "operation", stage = "test.network");
            async {
                let (mut writer, mut reader) = tokio::io::duplex(128);
                let value = vec![7u8; 4096];
                let (sent, received) = tokio::join!(
                    write_object_frame_async(&mut writer, &value, ObjectFrameIoOptions::default()),
                    read_object_frame_async::<_, Vec<u8>>(&mut reader, ObjectFrameIoOptions::default()),
                );
                sent.unwrap();
                assert_eq!(received.unwrap(), value);
            }.instrument(span).await;
        });
    });
    let records = layer.traces.lock().unwrap();
    let tx: Vec<_> = records
        .iter()
        .filter(|r| r.fields["io_kind"] == "net.tx")
        .collect();
    let rx: Vec<_> = records
        .iter()
        .filter(|r| r.fields["io_kind"] == "net.rx")
        .collect();
    assert_eq!(tx.len(), 1);
    assert_eq!(rx.len(), 1);
    assert_eq!(tx[0].fields["io_bytes"], rx[0].fields["io_bytes"]);
    assert!(tx[0].fields["io_bytes"].as_u64().unwrap() > 4096);
    assert_eq!(tx[0].spans[0].span_id, rx[0].spans[0].span_id);
}

#[test]
fn incremental_trace_cursor_reports_only_uncollected_loss() {
    let template = ObserveTraceRecord {
        sequence: 0,
        monotonic_ns: 0,
        spans: Vec::new(),
        context_truncated: false,
        timestamp: OffsetDateTime::now_utc(),
        level: "TRACE".into(),
        target: "sinomo.perf".into(),
        name: "test".into(),
        traceparent: None,
        capture_id: Some("c".into()),
        fields: serde_json::Value::Null,
    };
    let ring = (10..=12)
        .map(|sequence| ObserveTraceRecord {
            sequence,
            ..template.clone()
        })
        .collect();
    let request = ObserveTracesRequest {
        capture_id: Some("c".into()),
        after_sequence: Some(10),
        ..Default::default()
    };
    let response = trace_ring_response(&ring, &request, 9);
    assert_eq!(
        response
            .records
            .iter()
            .map(|r| r.sequence)
            .collect::<Vec<_>>(),
        [11, 12]
    );
    assert_eq!(response.last_sequence, 12);
    assert_eq!(response.first_available_sequence, Some(10));
    assert!(
        !response.truncated,
        "already exported eviction is not data loss"
    );
    assert!(
        trace_ring_response(
            &ring,
            &ObserveTracesRequest {
                after_sequence: Some(8),
                ..request.clone()
            },
            9
        )
        .truncated
    );
    assert!(
        trace_ring_response(
            &ring,
            &ObserveTracesRequest {
                after_sequence: Some(20),
                ..request.clone()
            },
            9
        )
        .truncated
    );
    let empty = trace_ring_response(
        &ring,
        &ObserveTracesRequest {
            after_sequence: Some(12),
            ..request.clone()
        },
        9,
    );
    assert!(empty.records.is_empty());
    assert_eq!(empty.last_sequence, 12);
    assert!(!empty.truncated);
    assert!(
        trace_ring_response(
            &ring,
            &ObserveTracesRequest {
                limit: 1,
                ..request
            },
            9
        )
        .truncated
    );
}

#[test]
fn shared_blocking_wrapper_keeps_dispatcher_parent_and_queue_work_scopes() {
    let (dispatch, layer) = recorder(Some("blocking"));
    let job = tracing::dispatcher::with_default(&dispatch, || {
        let _parent = tracing::trace_span!(target: "sinomo.perf", "rpc", stage = "network.server",
            rpc_request_id = "request-one")
        .entered();
        sinomo_core::service::observed_blocking_task("test", || {
            tracing::trace!(target: "sinomo.perf", io_kind = "disk.write", io_bytes = 7u64, "worker");
            42
        })
    });
    // Deliberately do not install a subscriber on the worker thread: propagation
    // belongs to the wrapper, not to this test or a global subscriber.
    assert_eq!(std::thread::spawn(job).join().unwrap(), 42);
    let records = layer.traces.lock().unwrap();
    let event = records
        .iter()
        .find(|r| r.fields["message"] == "worker")
        .unwrap();
    assert_eq!(event.spans[0].fields["rpc_request_id"], "request-one");
    assert_eq!(
        event.spans.last().unwrap().fields["stage"],
        "service.blocking.work"
    );
    for stage in [
        "service.blocking",
        "service.blocking.queue",
        "service.blocking.work",
    ] {
        let closed = records
            .iter()
            .find(|r| r.fields["record_kind"] == "span_closed" && r.fields["stage"] == stage)
            .unwrap();
        assert_eq!(closed.fields["completed"], true);
        assert_eq!(closed.spans[0].fields["rpc_request_id"], "request-one");
    }
}

#[test]
fn blocking_context_propagates_when_performance_spans_are_disabled() {
    let (_, layer) = recorder(None);
    let dispatch = tracing::Dispatch::new(
        Registry::default()
            .with(EnvFilter::new("info"))
            .with(layer.clone()),
    );
    let job = tracing::dispatcher::with_default(&dispatch, || {
        let _parent = tracing::info_span!("request", rpc_request_id = "info-request").entered();
        sinomo_core::service::observed_blocking_task("test", || tracing::info!("worker info"))
    });
    std::thread::spawn(job).join().unwrap();
    let records = layer.traces.lock().unwrap();
    let event = records
        .iter()
        .find(|r| r.fields["message"] == "worker info")
        .unwrap();
    assert_eq!(event.spans.len(), 1);
    assert_eq!(event.spans[0].fields["rpc_request_id"], "info-request");
}

#[test]
fn observation_export_is_suppressed_without_poisoning_shared_ancestry() {
    let (dispatch, layer) = recorder(Some("self-observation"));
    tracing::dispatcher::with_default(&dispatch, || {
        let _ambient = tracing::info_span!("connection").entered();
        {
            let _request = tracing::trace_span!(target: "sinomo.perf", "network.server",
                stage = "network.server", observation = false)
            .entered();
            let _handler = tracing::trace_span!(target: "sinomo.perf", "network.handler", stage = "network.handler").entered();
            {
                let _call = tracing::trace_span!(target: "sinomo.perf", "network.rpc",
                    stage = "network.rpc", observation = true)
                .entered();
                tracing::trace!(target: "sinomo.perf", "self export");
            }
            tracing::trace!(target: "sinomo.perf", "forwarded self response");
            tracing::warn!(target: "sinomo.perf", "export warning stays visible");
        }
        {
            let _request = tracing::trace_span!(target: "sinomo.perf", "ordinary",
                stage = "network.server", observation = false)
            .entered();
            tracing::trace!(target: "sinomo.perf", "ordinary write");
        }
    });
    let records = layer.traces.lock().unwrap();
    assert!(!records.iter().any(|r| r.fields["message"] == "self export"
        || r.fields["message"] == "forwarded self response"));
    assert!(
        records
            .iter()
            .any(|r| r.fields["message"] == "ordinary write")
    );
    assert!(
        records
            .iter()
            .any(|r| r.fields["message"] == "export warning stays visible")
    );
    let marker = records
        .iter()
        .find(|r| r.fields["record_kind"] == "observation")
        .unwrap();
    assert_eq!(marker.fields["observation"], true);
    assert!(
        marker.spans[0].fields.get("observation").is_none(),
        "shared connection must remain unmarked"
    );
}

#[test]
fn late_observation_classification_emits_marker_before_snapshot() {
    let (dispatch, layer) = recorder(Some("late"));
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::trace_span!(target: "sinomo.perf", "network.server",
            stage = "network.server", observation = false);
        let _entered = span.enter();
        span.record("observation", true);
        assert!(
            layer
                .traces
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.fields["record_kind"] == "observation")
        );
        let child = tracing::trace_span!(target: "sinomo.perf", "body", stage = "body");
        assert!(child.is_disabled());
    });
}

#[test]
fn eof_reads_report_known_zero_bytes() {
    use sinomo_core::storage::durable_file::{self, DurableLengthEndian, DurableRecordReadOptions};
    let (dispatch, layer) = recorder(Some("eof"));
    let path = std::env::temp_dir().join(format!(
        "sinomo-observe-eof-{}-{}",
        std::process::id(),
        monotonic_test_id()
    ));
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        let mut file = std::fs::File::open(&path).unwrap();
        assert!(
            durable_file::read_length_prefixed_record(
                &mut file,
                &path,
                DurableRecordReadOptions::new(DurableLengthEndian::Little)
            )
            .unwrap()
            .is_none()
        );
        assert!(
            durable_file::read_object_file_bytes_from_open_file(&path, file, 10)
                .unwrap()
                .is_empty()
        );
    });
    std::fs::remove_file(path).unwrap();
    let records = layer.traces.lock().unwrap();
    let closed: Vec<_> = records
        .iter()
        .filter(|r| r.fields["record_kind"] == "span_closed")
        .collect();
    assert_eq!(closed.len(), 2);
    assert!(
        closed
            .iter()
            .all(|r| r.fields["io_bytes"] == 0 && r.fields["outcome"] == "ok")
    );
}

#[test]
fn trace_capacity_is_explicit_and_bounded() {
    assert_eq!(parse_trace_capacity(None).unwrap(), 4096);
    assert_eq!(parse_trace_capacity(Some("32768")).unwrap(), 32768);
    for value in ["0", "32769", "invalid", "-1"] {
        assert!(parse_trace_capacity(Some(value)).is_err());
    }
}
