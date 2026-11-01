//! Native consumer controls. Replies are discarded only after the
//! actual registered Main delegate returns; no successful payload/ACK is made.
use super::*;
use sinomo_runtime::api::{
    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE, DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE, DaemonApiError, DaemonDfsBrowserReadFileRequest,
    DaemonDfsBrowserReadFileResponse, DaemonDfsBrowserWriteFileRequest,
    DaemonDfsBrowserWriteFileResponse,
};
use sinomo_runtime::dfs::{
    DfsMetadataBackend, DfsMetadataShardId, DfsPreparedOperationStatus, LocalSobjMetadataBackend,
    default_dfs_metadata_shards_root,
};
use sinomo_runtime::network::NetworkDeliveryState;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum Cut {
    FirstReadReplyLost,
    EveryReadReplyLost,
    WriteDefinitelyNotSent,
}
fn lost_reply(detail: &str) -> DaemonApiError {
    DaemonApiError::NetworkDelivery {
        detail: detail.into(),
        delivery_state: NetworkDeliveryState::PossiblyApplied,
    }
}
fn actual_native_case(name: &str, cut: Cut) {
    let root = sinomo_testdir::TestDir::new(name);
    let data = SinomoDataDir::new(root.path());
    let _owner = crate::native_test_fixture::NativeCliFixture::start(&data);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let clean = crate::native_test_fixture::browser(&data, runtime.handle()).unwrap();
    let node = clean.target.as_ref().unwrap().1.clone();
    let make_browser = |remote| SifsBrowser {
        backend: SifsBrowserBackend::Remote(remote),
        node_id: node.to_string(),
    };
    let mut store = SifsAgentChannelStore::from_browser(make_browser(clean.clone()));
    let id = SessionId::generate();
    let mut channel = AgentChannel::new_channel(id.clone(), "Actual lost reply");
    store.save_channel(&channel).unwrap();
    let message = channel.append_user_message(
        AGENT_CHANNEL_APP_USER_ID,
        "Native",
        "one immutable native message",
        AgentChannelSource::app(),
    );
    let path = store.message_record_path(&id, &message.message_id);
    let expected = serde_json::to_vec_pretty(&message).unwrap();
    let delegate = clean
        .test_rpc
        .clone()
        .expect("existing real registered Main delegate");
    let writes = Arc::new(AtomicUsize::new(0));
    let delegated_writes = Arc::new(AtomicUsize::new(0));
    let discarded_reads = Arc::new(AtomicUsize::new(0));
    let after_unknown_mutations = Arc::new(AtomicUsize::new(0));
    let applied = Arc::new(AtomicBool::new(false));
    let receipts = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let (calls, actual, lost_reads, late_mutations, written, evidence) = (
        writes.clone(),
        delegated_writes.clone(),
        discarded_reads.clone(),
        after_unknown_mutations.clone(),
        applied.clone(),
        receipts.clone(),
    );
    let target_path = path.clone();
    let payload = expected.clone();
    let wrapped = clean.clone().with_test_rpc(Arc::new(move |request, remaining| {
        let is_write = request.route == DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE;
        let is_mkdir = request.route == DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE;
        if written.load(Ordering::SeqCst) && (is_write || is_mkdir) {
            late_mutations.fetch_add(1, Ordering::SeqCst);
        }
        if is_write {
            let command: DaemonDfsBrowserWriteFileRequest = request.decode_body().unwrap();
            assert_eq!(command.path, target_path);
            assert_eq!(command.bytes, payload);
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if matches!(cut, Cut::WriteDefinitelyNotSent) {
                // The actual delegate is never called: the same native cut
                // below must contain no target object/final prepare.
                return Err(DaemonApiError::NetworkDelivery {
                    detail: "exact original write rejected before delegate dispatch".into(),
                    delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                });
            }
            let response = delegate(request, remaining)?;
            let accepted: DaemonDfsBrowserWriteFileResponse = response.decode_body().unwrap();
            actual.fetch_add(1, Ordering::SeqCst);
            evidence.lock().unwrap().push(serde_json::json!({"attempt":attempt,"actual_native_write_response":accepted}));
            written.store(true, Ordering::SeqCst);
            if attempt == 0 {
                drop(response);
                return Err(lost_reply("original immutable channel write applied; actual reply deliberately discarded"));
            }
            return Ok(response);
        }
        let read = if request.route == DAEMON_DFS_BROWSER_READ_FILE_ROUTE {
            Some(request.decode_body::<DaemonDfsBrowserReadFileRequest>().unwrap())
        } else { None };
        let response = delegate(request, remaining)?;
        if written.load(Ordering::SeqCst) && read.as_ref().is_some_and(|read| read.path == target_path) {
            let actual_read = response.decode_body::<DaemonDfsBrowserReadFileResponse>().unwrap().into_result().unwrap();
            assert_eq!(actual_read, payload, "actual Main read already holds exact original immutable bytes");
            let index = lost_reads.fetch_add(1, Ordering::SeqCst);
            if index == 0 || matches!(cut, Cut::EveryReadReplyLost) {
                evidence.lock().unwrap().push(serde_json::json!({"actual_read_response_discarded":true,"read_index":index,"remaining_ns":remaining.as_nanos()}));
                drop(response);
                return Err(lost_reply("actual read-back reply deliberately discarded; original write remains observationally unknown"));
            }
        }
        Ok(response)
    }));
    store.browser = make_browser(wrapped);
    let began = Instant::now();
    let result = store.append_message(&id, &message);
    let elapsed = began.elapsed();
    // Independent current physical cut, not the pending-only prepared scan.
    let backend = LocalSobjMetadataBackend::open_for_inspection(
        DfsMetadataShardId::new("services"),
        default_dfs_metadata_shards_root(&data).join("services"),
    )
    .unwrap();
    let snapshot = DfsMetadataBackend::export_shard_table_snapshot(
        &backend,
        &DfsMetadataShardId::new("services"),
    )
    .unwrap()
    .expect("fresh native inspection handle exports its one physical Services cut");
    let originals = snapshot
        .tables
        .prepared_operations_v7
        .iter()
        .filter(|record| {
            record.digest.operation_id.starts_with("write:")
                && record
                    .transaction
                    .objects
                    .iter()
                    .any(|object| object.path == path)
        })
        .collect::<Vec<_>>();
    for record in &originals {
        record.validate().unwrap();
    }
    let current = snapshot
        .tables
        .objects
        .iter()
        .find(|object| object.path == path);
    // A separate observer may see committed bytes even when the affected
    // caller retained Unknown. It does not convert that caller error to success.
    let observed = clean.read_file(&path);
    std::fs::write(root.join("channel-lost-reply-native.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "cut":format!("{cut:?}"),"result":format!("{result:?}"),"elapsed_ms":elapsed.as_millis(),
        "channel_id":id,"message_id":message.message_id,"path":path,
        "write_attempts":writes.load(Ordering::SeqCst),"delegated_native_writes":delegated_writes.load(Ordering::SeqCst),
        "post_ambiguity_mutations":after_unknown_mutations.load(Ordering::SeqCst),"read_calls_after_write":discarded_reads.load(Ordering::SeqCst),
        "native_reply_receipts":receipts.lock().unwrap().clone(),"actual_target_object":current,"actual_final_originals":originals,
        "independent_bytes_match":observed.as_ref().is_ok_and(|bytes|bytes==&expected),
        "qualification":"actual Main delegate apply then actual reply discard; native current Services cut; no fabricated successful result/ACK/body, no direct row mutation or global fault clear; no claim lost original receipt settled"})).unwrap()).unwrap();
    assert_eq!(
        writes.load(Ordering::SeqCst),
        1,
        "one original write request only"
    );
    assert_eq!(
        after_unknown_mutations.load(Ordering::SeqCst),
        0,
        "unknown write permits reads only, including parent handling"
    );
    if matches!(cut, Cut::WriteDefinitelyNotSent) {
        assert!(result.is_err());
        assert_eq!(delegated_writes.load(Ordering::SeqCst), 0);
        assert!(originals.is_empty());
        assert!(current.is_none());
        assert!(observed.is_err());
    } else {
        assert_eq!(delegated_writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            originals.len(),
            1,
            "one native final write Original, not one per resend"
        );
        assert_eq!(originals[0].status, DfsPreparedOperationStatus::Committed);
        assert_eq!(observed.unwrap(), expected);
        if matches!(cut, Cut::EveryReadReplyLost) {
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(
                    "original immutable channel write applied; actual reply deliberately discarded"
                ),
                "{error}"
            );
            assert!(error.contains("outcome remained unknown"), "{error}");
        } else {
            result.unwrap();
        }
        let observer = SifsAgentChannelStore::from_browser(make_browser(clean));
        let loaded = observer.load_channel(&id).unwrap().unwrap();
        assert_eq!(loaded.channel_id, id);
        assert_eq!(loaded.transcript.len(), 1);
        assert_eq!(loaded.transcript[0].message_id, message.message_id);
    }
}
#[test]
fn lost_native_write_and_first_read_reply_never_resend_immutable_channel_record() {
    actual_native_case(
        "native-channel-write-and-read-reply-loss",
        Cut::FirstReadReplyLost,
    );
}
#[test]
fn unobserved_native_channel_write_retains_original_error_without_resend() {
    actual_native_case(
        "native-channel-original-stays-unknown",
        Cut::EveryReadReplyLost,
    );
}
#[test]
fn definite_presend_channel_write_failure_does_not_create_or_retry_an_original() {
    actual_native_case(
        "native-channel-definitely-not-sent",
        Cut::WriteDefinitelyNotSent,
    );
}
