//! DEFAULT Main controls. All carrier replies, Original births, receipt effects
//! and closure come from the actual native owners; hooks supply negatives/DATA.
use super::*;
use crate::runtime_host::{DaemonDfsPreDispatchTestHookGuard, DaemonDfsPreDispatchTestPoint};
use crate::test_runtime::{
    DfsTestRuntime, FaultAction, FaultRule, RpcObservation, RpcOrigin, RpcPhase,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type GapService = NetworkedDfsService<DfsNodeTransport>;
type GapBirth = crate::repair_queue_store::RepairOriginalBirth;
const GAP_BYTES: usize = 256 * 1024 + 31;

fn gap_rpc_matches_original(observation: &RpcObservation, birth: &GapBirth) -> bool {
    let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
        return false;
    };
    observation.source == birth.original.source_node_id
        && observation.target == birth.original.target_node_id
        && observation.route == DFS_DATA_REPAIR_REPLICA_ROUTE
        && observation.dfs_request_id.as_deref() == Some(command.request_id.as_str())
        && observation.dfs_intent_id.as_deref() == Some(command.intent_id.as_str())
        && observation.dfs_file_system_id.as_ref() == Some(&command.file_system_id)
        && observation.dfs_object_id.as_ref() == Some(&command.object_id)
        && observation.dfs_version.as_ref() == Some(&command.version)
}

// The immutable Original stores an attachment-backed empty byte vector; the
// receiver has materialized that exact attachment before this observation.
// Normalize ONLY that representation field, never any immutable command header.
fn gap_received_original_bytes(
    returned: &GapReturned,
    original: &GapBirth,
    expected: &[u8],
) -> bool {
    let actual = &returned.command.routed_message;
    let (DfsNetworkMessage::RepairReplica(received), DfsNetworkMessage::RepairReplica(stored)) =
        (&actual.message, &original.original.message)
    else {
        return false;
    };
    if actual.source_node_id != original.original.source_node_id
        || actual.target_node_id != original.original.target_node_id
        || received.bytes.as_slice() != expected
        || original.attachment.total_size_bytes != expected.len() as u64
        || original.attachment.content_hash.as_ref() != Some(&stored.content_hash)
        || content_hash(&received.bytes) != stored.content_hash
    {
        return false;
    }
    let mut normalized = received.clone();
    if stored.bytes.is_empty() {
        normalized.bytes.clear();
    }
    normalized == *stored
}

#[derive(Clone, Debug)]
struct GapSubject {
    stat: DfsStat,
    target: NodeId,
    shard: DfsMetadataShardId,
}
impl GapSubject {
    fn actual_birth(&self, queue: &LocalDfsRepairQueueStore) -> Result<Option<GapBirth>, String> {
        let rows = queue.view().map_err(|e| e.to_string())?;
        for row in rows.records.iter().filter(|row| {
            row.object_id == self.stat.object_id
                && row.version == self.stat.version
                && row.missing_replica_node_id == self.target
        }) {
            let Some(birth) = queue
                .original_repair_for_slot(&row.intent_id, &self.target)
                .map_err(|e| e.to_string())?
            else {
                continue;
            };
            // Scheduling rows have no filesystem field. The immutable native
            // Original command supplies that binding; no row hint substitutes.
            if let DfsNetworkMessage::RepairReplica(command) = &birth.original.message
                && command.file_system_id == self.stat.file_system_id
                && command.object_id == self.stat.object_id
                && command.version == self.stat.version
                && command.intent_id == row.intent_id
                && Some(&command.content_hash) == self.stat.content_hash.as_ref()
                && birth.original.target_node_id == self.target
            {
                return Ok(Some(birth));
            }
        }
        Ok(None)
    }
    fn receipt(&self, receipt: &DfsReplicaReceiptRecord) -> bool {
        receipt.receipt.file_system_id == self.stat.file_system_id
            && receipt.receipt.object_id == self.stat.object_id
            && receipt.receipt.committed_version == self.stat.version
            && receipt.replica_node_id == self.target
            && receipt.content_hash == self.stat.content_hash
    }
    fn reply(&self, frame: &DfsRoutedMessage) -> bool {
        matches!(&frame.message, DfsNetworkMessage::RepairReplicaResponse(reply)
            if reply.file_system_id == self.stat.file_system_id
            && reply.object_id == self.stat.object_id && reply.version == self.stat.version
            && reply.replica_node_id == self.target
            && matches!(&reply.result, DfsBlobTransferResult::Committed { content_hash }
                if Some(content_hash) == self.stat.content_hash.as_ref()))
    }
}

#[derive(Clone, Debug, serde::Serialize, Eq, PartialEq)]
struct GapCut {
    prefix: DfsMetadataPrefixProof,
    object: Option<DfsObjectMetadata>,
    receipts: Vec<DfsReplicaReceiptRecord>,
    prepares: Vec<DfsPreparedOperationRecord>,
    intents: Vec<DfsWriteIntentRecord>,
}
fn gap_cut(
    data: &sinomo_core::local::SinomoDataDir,
    subject: &GapSubject,
) -> Result<GapCut, String> {
    let mut backend = LocalSobjMetadataBackend::open_for_inspection(
        subject.shard.clone(),
        default_dfs_metadata_shards_root(data).join(&subject.shard.0),
    )
    .map_err(|e| e.to_string())?;
    backend
        .begin_inspection_snapshot()
        .map_err(|e| e.to_string())?;
    let result = (|| {
        let snapshot = DfsMetadataBackend::export_shard_table_snapshot(&backend, &subject.shard)
            .map_err(|e| e.to_string())?
            .ok_or("selected native snapshot unavailable")?;
        let object = snapshot
            .tables
            .objects
            .iter()
            .find(|object| {
                object.file_system_id == subject.stat.file_system_id
                    && object.object_id == subject.stat.object_id
            })
            .cloned();
        let receipts = snapshot
            .tables
            .receipts
            .iter()
            .filter(|r| subject.receipt(r))
            .cloned()
            .collect();
        let prepares = snapshot
            .tables
            .prepared_operations_v7
            .iter()
            .filter(|record| {
                record.payload.metadata_effects.iter().any(|effect| {
                    matches!(effect,
                DfsPreparedMetadataEffect::PutReceipt { receipt } if subject.receipt(receipt))
                })
            })
            .cloned()
            .collect();
        let intents = snapshot
            .tables
            .intents
            .iter()
            .filter(|intent| {
                intent.file_system_id == subject.stat.file_system_id
                    && intent.object_id == subject.stat.object_id
                    && intent.target_version == subject.stat.version
            })
            .cloned()
            .collect();
        Ok(GapCut {
            prefix: snapshot.prefix_proof,
            object,
            receipts,
            prepares,
            intents,
        })
    })();
    backend.end_inspection_snapshot();
    result
}

#[derive(Clone, Debug, serde::Serialize)]
struct GapReturned {
    command: DfsMessageCommand,
    response: DfsMessageResponse,
}
#[derive(Default)]
struct GapSwitches {
    deny_receipt: AtomicBool,
    receipt_denials: AtomicUsize,
    receipt_bodies: Mutex<Vec<DfsPreparedOperationPayload>>,
    receipt_originals: Mutex<Vec<Result<GapBirth, String>>>,
    receipt_premise_errors: AtomicUsize,
    omitted_receipt_bodies: AtomicUsize,
    placement_entries: Mutex<Vec<(String, String, bool)>>,
    omitted_placement_entries: AtomicUsize,
    subject: Mutex<Option<GapSubject>>,
    returned: Mutex<Vec<GapReturned>>,
    omitted_returned: AtomicUsize,
    old_probe: AtomicBool,
    old_frame: Mutex<Option<DfsRoutedMessage>>,
    old_entry: Mutex<Option<(bool, Result<GapCut, String>)>>,
    old_exit: Mutex<Option<Result<GapCut, String>>>,
}
struct GapGateCleanup(Arc<tokio::sync::Semaphore>);
impl Drop for GapGateCleanup {
    fn drop(&mut self) {
        // Normal permit release must let the SAME pending native call finish.
        // Abort only on unwind; DefaultGapCase still closes all gates at teardown.
        if std::thread::panicking() {
            self.0.close();
        }
    }
}

struct DefaultGapCase {
    native: Option<DfsTestRuntime>,
    owner: NodeId,
    old_holder: NodeId,
    target: NodeId,
    path: String,
    bytes: Vec<u8>,
    subject: GapSubject,
    accepted: DfsPlacementReconciliationJob,
    initial_gate: Arc<tokio::sync::Semaphore>,
    gates: Vec<Arc<tokio::sync::Semaphore>>,
    switches: Arc<GapSwitches>,
    source_hook: Option<DaemonDfsPreDispatchTestHookGuard>,
    receiver_hook: Option<DaemonDfsPreDispatchTestHookGuard>,
    source: Option<GapService>,
    receiver: Option<GapService>,
    close_denial: Option<crate::repair_queue_store::OriginalRepairCloseDenialGuard>,
}
impl DefaultGapCase {
    fn runtime(&self) -> &DfsTestRuntime {
        self.native.as_ref().unwrap()
    }
    fn queue(&self) -> LocalDfsRepairQueueStore {
        LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(
            self.runtime().data_dir(&self.owner),
        ))
    }
    fn cut(&self) -> Result<GapCut, String> {
        gap_cut(self.runtime().data_dir(&self.owner), &self.subject)
    }
    fn birth(&self) -> Option<GapBirth> {
        let queue = self.queue();
        self.subject.actual_birth(&queue).unwrap()
    }
    fn evidence(&self, name: &str, more: serde_json::Value) {
        let queue = self.queue();
        let data = serde_json::json!({
            "qualification":"actual DEFAULT3native Main; observations only, no selected fixture or hand-authored repair/receipt/ACK/close",
            "owner":self.owner,"old_holder":self.old_holder,"target":self.target,
            "subject":{"file_system_id":self.subject.stat.file_system_id,
                "object_id":self.subject.stat.object_id,"version":self.subject.stat.version,
                "path":self.subject.stat.path,"content_hash":self.subject.stat.content_hash,
                "target":self.subject.target,"shard":self.subject.shard,
                "actual_stat_debug":format!("{:?}",self.subject.stat)},
            "accepted_job":self.accepted,
            "native_cut":self.cut(),"queue":format!("{:#?}",queue.view()),"birth":self.birth(),
            "receipt_denials":self.switches.receipt_denials.load(Ordering::SeqCst),
            "actual_before_prepare_bodies":*self.switches.receipt_bodies.lock().unwrap(),
            "actual_before_prepare_originals":*self.switches.receipt_originals.lock().unwrap(),
            "receipt_premise_errors":self.switches.receipt_premise_errors.load(Ordering::SeqCst),
            "omitted_receipt_bodies":self.switches.omitted_receipt_bodies.load(Ordering::SeqCst),
            "actual_placement_entries":*self.switches.placement_entries.lock().unwrap(),
            "omitted_placement_entries":self.switches.omitted_placement_entries.load(Ordering::SeqCst),
            "actual_receiver_returns":*self.switches.returned.lock().unwrap(),
            "omitted_receiver_returns":self.switches.omitted_returned.load(Ordering::SeqCst),
            "old_entry":*self.switches.old_entry.lock().unwrap(),
            "old_exit":*self.switches.old_exit.lock().unwrap(),
            "rpc_window":format!("{:#?}",self.runtime().rpc_observations()),
            "omitted_rpc_records":self.runtime().observation_dropped_count(),"facts":more,
        });
        std::fs::write(
            self.runtime().data_dir(&self.owner).root().join(name),
            serde_json::to_vec_pretty(&data).unwrap(),
        )
        .unwrap();
    }
    fn wait(&self, ready: impl FnMut() -> bool) -> bool {
        self.wait_until(
            Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            ready,
        )
    }
    fn wait_until(&self, deadline: Instant, mut ready: impl FnMut() -> bool) -> bool {
        loop {
            if Instant::now() >= deadline {
                return false;
            }
            if ready() {
                return Instant::now() < deadline;
            }
            std::thread::sleep(
                Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
    // SETUP eligibility only: wait once for the ACTUAL immutable absolute due
    // while no source mutation can run. This is not a longer business window,
    // a caller retry, a clock override, or permission to change native rows.
    fn qualify_recorded_due(&self, original: &GapBirth, label: &str) -> serde_json::Value {
        let queue = self.queue();
        let before_view = queue.view().unwrap();
        let before_birth = queue.original_repair(&original.original).unwrap().unwrap();
        let before_cut = self.cut().unwrap();
        let row = before_view
            .records
            .iter()
            .find(|row| {
                row.intent_id == before_birth.root.subject.intent_id
                    && row.missing_replica_node_id == self.target
            })
            .cloned();
        let wall_before = crate::metadata_records::dfs_now_unix_nanos();
        self.evidence(
            &format!("gap-{label}-eligibility-before.json"),
            serde_json::json!({
                "qualification":"SETUP actual recorded eligibility, not the5s business action",
                "actual_row":row,"actual_original":before_birth,"actual_prefix":before_cut.prefix,
                "actual_wall_unix_nanos":wall_before,
            }),
        );
        let row = row.expect("SETUP requires the actual named pending Original scheduling row");
        assert!(row.transfer_request_id.is_some() && row.completed_transfer_request_id.is_none());
        assert_ne!(
            row.next_retry_unix_nanos,
            u64::MAX,
            "SETUP cannot treat parked/unknown scheduling as transfer eligibility"
        );
        assert!(before_birth.closed_at_sequence.is_none());
        let waited_from = Instant::now();
        loop {
            let wall = crate::metadata_records::dfs_now_unix_nanos();
            if wall >= row.next_retry_unix_nanos {
                break;
            }
            std::thread::sleep(
                Duration::from_millis(10)
                    .min(Duration::from_nanos(row.next_retry_unix_nanos - wall)),
            );
        }
        let wall_after = crate::metadata_records::dfs_now_unix_nanos();
        let after_view = queue.view().unwrap();
        let after_row = after_view
            .records
            .iter()
            .find(|actual| {
                actual.intent_id == row.intent_id && actual.missing_replica_node_id == self.target
            })
            .cloned();
        let after_birth = queue.original_repair(&original.original).unwrap().unwrap();
        let after_cut = self.cut().unwrap();
        let facts = serde_json::json!({
            "qualification":"single REAL-clock SETUP wait for actual stored absolute due; no row/clock/ID/fate/lease rewrite",
            "actual_due_unix_nanos":row.next_retry_unix_nanos,
            "actual_wall_before_unix_nanos":wall_before,"actual_wall_after_unix_nanos":wall_after,
            "actual_waited_us":waited_from.elapsed().as_micros(),
            "before_queue_head":before_view.head_sequence,"after_queue_head":after_view.head_sequence,
            "before_row":row,"after_row":after_row,"before_original":before_birth,"after_original":after_birth,
            "before_native_cut":before_cut,"after_native_cut":after_cut,
        });
        self.evidence(
            &format!("gap-{label}-eligibility-qualified.json"),
            facts.clone(),
        );
        assert_eq!(
            after_row.as_ref(),
            Some(&row),
            "SETUP row changed; cannot search/restart eligibility"
        );
        assert_eq!(
            before_birth, after_birth,
            "SETUP Original history/custody changed during eligibility wait"
        );
        assert_eq!(
            before_cut, after_cut,
            "SETUP source metadata changed while mutation was excluded"
        );
        assert!(wall_after >= row.next_retry_unix_nanos);
        facts
    }
    fn remove_gate_fault(&self, gate: &Arc<tokio::sync::Semaphore>, after: bool) {
        let count = self.runtime().remove_faults_matching(|rule| {
            rule.source.as_ref() == Some(&self.owner)
                && rule.target.as_ref() == Some(&self.target)
                && rule.route.as_deref() == Some(DFS_DATA_REPAIR_REPLICA_ROUTE)
                && match &rule.action {
                    FaultAction::PauseBeforeDispatch(actual) if !after => Arc::ptr_eq(actual, gate),
                    FaultAction::PauseAfterDispatch(actual) if after => Arc::ptr_eq(actual, gate),
                    _ => false,
                }
        });
        assert_eq!(
            count, 1,
            "remove only this fixture's exact owned negative selector"
        );
    }
    fn initial_return(&self) -> DfsRoutedMessage {
        self.switches
            .returned
            .lock()
            .unwrap()
            .iter()
            .flat_map(|r| r.response.outbound_messages.iter())
            .find(|frame| self.subject.reply(frame))
            .expect("actual C returned frame captured as DATA")
            .clone()
    }
    fn goal(&self, original: &GapBirth) -> bool {
        let Ok(cut) = self.cut() else {
            return false;
        };
        let closed = self.queue().original_repair(&original.original).unwrap();
        cut.receipts.len() == 1
            && cut
                .object
                .as_ref()
                .is_some_and(|o| o.current_locations.contains(&self.target))
            && closed
                .as_ref()
                .is_some_and(|b| b.closed_at_sequence.is_some())
    }
    fn verify_goal(&mut self, original: &GapBirth) {
        let cut = self.cut().unwrap();
        let closed = self
            .queue()
            .original_repair(&original.original)
            .unwrap()
            .unwrap();
        assert!(closed.closed_at_sequence.is_some());
        assert_eq!(closed.original, original.original);
        assert_eq!(closed.attachment, original.attachment);
        assert_eq!(closed.root, original.root);
        assert!(!closed.observed.is_empty());
        assert_eq!(cut.receipts.len(), 1);
        let current = cut.object.unwrap();
        assert_eq!(current.object_id, self.subject.stat.object_id);
        assert_eq!(current.version, self.subject.stat.version);
        assert_eq!(current.content_hash, self.subject.stat.content_hash);
        assert!(current.current_locations.contains(&self.target));
        let (manifest, bytes) = self
            .receiver
            .as_ref()
            .unwrap()
            .content()
            .read_object_version(
                self.subject.stat.object_id.as_str(),
                self.subject.stat.version.as_str(),
            )
            .unwrap();
        assert_eq!(bytes, self.bytes);
        assert_eq!(manifest.content_hash, content_hash(&self.bytes));
        let source = self.runtime().client(&self.owner).unwrap();
        let released = source.content().inspect_repair_payload_root(&original.root);
        assert!(
            matches!(released,Err(crate::ContentStoreError::InvalidContentLayout { ref message })
            if message=="Repair dispatch retained root absent, changed or released"),
            "same native root must become nonlive only after actual closed evidence: {released:?}"
        );
        assert_eq!(self.runtime().observation_dropped_count(), 0);
        assert_eq!(self.switches.omitted_returned.load(Ordering::SeqCst), 0);
        assert_eq!(
            self.switches.receipt_premise_errors.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            self.switches.omitted_receipt_bodies.load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            self.switches
                .omitted_placement_entries
                .load(Ordering::SeqCst),
            0
        );
    }
    fn new(name: &'static str, deny_receipt: bool) -> (Self, GapBirth) {
        let (native, client, config) =
            crate::test_support::current_native_metadata_service(name, 3, "/services").unwrap();
        drop(client);
        let owner = config.primary_node_id.clone();
        let nodes = DfsTestRuntime::ordered_node_ids(name, 3);
        let others = nodes
            .iter()
            .filter(|n| **n != owner)
            .cloned()
            .collect::<Vec<_>>();
        let old_holder = others[0].clone();
        let target = others[1].clone();
        assert!(
            native.selected_original_data_layout(&owner).is_err(),
            "DEFAULT factory only"
        );
        let path = format!("/services/{name}.bin");
        let mut initial =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        initial.required_node_ids = vec![owner.clone(), old_holder.clone()];
        initial.forbidden_node_ids = vec![target.clone()];
        let seed_job = native
            .apply_placement_policy(&owner, &path, DfsPlacementScope::SelfOnly, initial)
            .unwrap();
        crate::tests::writes::finish_native_placement_fixture_apply(&native, &seed_job);
        let mut source = native.metadata_primary_service(&path).unwrap();
        let receiver = native.client(&target).unwrap();
        let bytes = (0..GAP_BYTES).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let reply = native
            .rpc_request(
                &owner,
                &owner,
                ObjectRpcRequest::encode_body(
                    crate::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
                    sinomo_api::DAEMON_RPC_CATEGORY,
                    &crate::DaemonDfsBrowserWriteFileRequest {
                        path: path.clone(),
                        bytes: bytes.clone(),
                    },
                )
                .unwrap(),
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap();
        let _: crate::DaemonDfsBrowserWriteFileResponse = reply.decode_body().unwrap();
        let stat = source.posix_stat_distributed(&path).unwrap();
        let shard = DfsMetadataShardId::new(&config.shard_id);
        let seed_subject = GapSubject {
            stat: stat.clone(),
            target: old_holder.clone(),
            shard: shard.clone(),
        };
        let seed_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let seed = loop {
            if let Ok(cut) = gap_cut(native.data_dir(&owner), &seed_subject)
                && cut.receipts.len() == 1
                && cut.object.as_ref().is_some_and(|o| {
                    o.current_locations.contains(&owner)
                        && o.current_locations.contains(&old_holder)
                        && !o.current_locations.contains(&target)
                })
            {
                break cut;
            }
            assert!(
                Instant::now() < seed_deadline,
                "healthy seed must reach actual W1/N2 before managed plan"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            !receiver
                .content()
                .has_blob(stat.content_hash.as_deref().unwrap())
                .unwrap(),
            "C was not an old holder"
        );
        // Acquire ordinary handles/snapshot while healthy. No fresh startup
        // opener is introduced after the controlled generation/claim boundary.
        let snapshot_deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        while source.placement_snapshot_cache.snapshot().is_none() {
            assert!(
                Instant::now() < snapshot_deadline,
                "real Main placement snapshot missing"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let switches = Arc::new(GapSwitches::default());
        let subject = GapSubject {
            stat: stat.clone(),
            target: target.clone(),
            shard,
        };
        *switches.subject.lock().unwrap() = Some(subject.clone());
        switches.deny_receipt.store(deny_receipt, Ordering::SeqCst);
        let source_state = Arc::clone(&switches);
        let source_data = native.data_dir(&owner).clone();
        let source_hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            native.data_dir(&owner),
            Arc::new(move |point, command, _| {
                let subject = source_state.subject.lock().unwrap().clone().unwrap();
                if let DaemonDfsPreDispatchTestPoint::LocalPlacementAuthorityAcquired {
                    job_id,
                    path,
                    namespace_shards,
                } = point
                {
                    let mut entries = source_state.placement_entries.lock().unwrap();
                    if entries.len() < 32 {
                        entries.push((
                            job_id.to_owned(),
                            path.to_owned(),
                            namespace_shards.is_some(),
                        ));
                    } else {
                        source_state
                            .omitted_placement_entries
                            .fetch_add(1, Ordering::SeqCst);
                    }
                }
                if let DaemonDfsPreDispatchTestPoint::BeforeMetadataEffectPrepare { payload } =
                    point
                    && source_state.deny_receipt.load(Ordering::SeqCst)
                    && payload.metadata_effects.iter().any(|effect| {
                        matches!(effect,
                    DfsPreparedMetadataEffect::PutReceipt {receipt} if subject.receipt(receipt))
                    })
                {
                    // Observe the DEFAULT native birth at this exact negative
                    // boundary; a later observation cannot establish this order.
                    let original = (|| {
                        let queue = LocalDfsRepairQueueStore::new(
                            default_dfs_repair_attempt_snapshot_path(&source_data),
                        );
                        let birth = subject.actual_birth(&queue)?
                            .ok_or("receipt preparation had no actual Original matching filesystem/intent/object/version/hash/target")?;
                        if birth.closed_at_sequence.is_some() || !birth.observed.iter().any(|observed|
                        matches!(&observed.result,DfsBlobTransferResult::Committed {content_hash}
                            if Some(content_hash)==subject.stat.content_hash.as_ref())) {
                        return Err("actual Committed observation was not present before receipt preparation".to_string());
                    }
                        Ok(birth)
                    })();
                    if original.is_err() {
                        source_state
                            .receipt_premise_errors
                            .fetch_add(1, Ordering::SeqCst);
                    }
                    source_state.receipt_denials.fetch_add(1, Ordering::SeqCst);
                    let mut bodies = source_state.receipt_bodies.lock().unwrap();
                    if bodies.len() < 32 {
                        bodies.push(payload.clone());
                        source_state
                            .receipt_originals
                            .lock()
                            .unwrap()
                            .push(original);
                    } else {
                        source_state
                            .omitted_receipt_bodies
                            .fetch_add(1, Ordering::SeqCst);
                    }
                    return Err(DfsError::Cancelled {
                        operation: "deny exact default repair receipt before its first native preparation",
                    });
                }
                if source_state.old_probe.load(Ordering::SeqCst)
                    && command.is_some_and(|command| {
                        source_state.old_frame.lock().unwrap().as_ref()
                            == Some(&command.routed_message)
                    })
                {
                    match point {
                        DaemonDfsPreDispatchTestPoint::BeforeExecute { admission_held } => {
                            *source_state.old_entry.lock().unwrap() =
                                Some((admission_held, gap_cut(&source_data, &subject)));
                        }
                        DaemonDfsPreDispatchTestPoint::AfterReport { .. } => {
                            *source_state.old_exit.lock().unwrap() =
                                Some(gap_cut(&source_data, &subject));
                        }
                        _ => {}
                    }
                }
                Ok(())
            }),
        );
        let receiver_state = Arc::clone(&switches);
        let receiver_hook = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            native.data_dir(&target),
            Arc::new(move |point, command, _| {
                if let DaemonDfsPreDispatchTestPoint::AfterMessageResponse { response } = point {
                    let subject = receiver_state.subject.lock().unwrap().clone().unwrap();
                    if response
                        .outbound_messages
                        .iter()
                        .any(|frame| subject.reply(frame))
                    {
                        let command = command.expect("actual receiver retains its decoded command");
                        let mut returned = receiver_state.returned.lock().unwrap();
                        if returned.len() < 32 {
                            returned.push(GapReturned {
                                command: command.clone(),
                                response: response.clone(),
                            });
                        } else {
                            receiver_state
                                .omitted_returned
                                .fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                Ok(())
            }),
        );
        let initial_gate = Arc::new(tokio::sync::Semaphore::new(0));
        native.install_fault(FaultRule {
            source: Some(owner.clone()),
            target: Some(target.clone()),
            route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: FaultAction::PauseBeforeDispatch(Arc::clone(&initial_gate)),
        });
        let mut managed =
            DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 2).unwrap();
        managed.required_node_ids = vec![owner.clone(), target.clone()];
        managed.forbidden_node_ids = vec![old_holder.clone()];
        // Unlike the control-store-only fixture helper, canonical browser
        // Apply invokes the actual host maintenance notification after CAS.
        // Preview and Apply each retain their original DEFAULT5s RPC budget.
        let control = native.shared_control_plane().unwrap();
        let preview_request = crate::DaemonDfsBrowserPlacementPreviewRequest {
            path: path.clone(),
            policy: Some(managed.clone()),
            scope: DfsPlacementScope::SelfOnly,
        };
        let preview: crate::DaemonDfsBrowserPlacementPreviewResponse = native
            .rpc_request(
                &owner,
                &owner,
                ObjectRpcRequest::encode_body(
                    crate::DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
                    sinomo_api::DAEMON_RPC_CATEGORY,
                    &preview_request,
                )
                .unwrap(),
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .decode_body()
            .unwrap();
        let apply_request = crate::DaemonDfsBrowserPlacementUpdateV2Request {
            path: path.clone(),
            policy: Some(managed),
            scope: DfsPlacementScope::SelfOnly,
            expected_preview_token: preview.preview.preview_token.clone(),
        };
        let applied: crate::DaemonDfsBrowserPlacementUpdateResponse = native
            .rpc_request(
                &owner,
                &owner,
                ObjectRpcRequest::encode_body(
                    crate::DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
                    sinomo_api::DAEMON_RPC_CATEGORY,
                    &apply_request,
                )
                .unwrap(),
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            )
            .unwrap()
            .decode_body()
            .unwrap();
        let accepted = applied.reconciliation.clone();
        let stored_after_apply = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&accepted.file_system_id, &accepted.job_id)
            })
            .map_err(|e| e.to_string());
        let case = Self {
            native: Some(native),
            owner,
            old_holder,
            target,
            path,
            bytes,
            subject,
            accepted,
            initial_gate: Arc::clone(&initial_gate),
            gates: vec![initial_gate],
            switches,
            source_hook: Some(source_hook),
            receiver_hook: Some(receiver_hook),
            source: Some(source),
            receiver: Some(receiver),
            close_denial: None,
        };
        case.evidence(
            "gap-managed-before-claim.json",
            serde_json::json!({"actual_seed_cut":seed,
                "actual_preview_request":preview_request,"actual_preview_response":preview,
                "actual_apply_request":apply_request,"actual_apply_response":applied,
                "actual_stored_job_after_apply":stored_after_apply,
                "wake_owner":"canonical browser Apply called actual host.notify_maintenance after accepted native CAS; returned handle is not an execution-progress witness"}),
        );
        let stored = stored_after_apply
            .as_ref()
            .unwrap()
            .as_ref()
            .expect("actual accepted job is durably present after browser Apply");
        assert_eq!(stored.job_id, case.accepted.job_id);
        assert_eq!(stored.accepted_config, case.accepted.accepted_config);
        assert_eq!(stored.placement_snapshot, case.accepted.placement_snapshot);
        assert!(
            case.accepted.placement_snapshot.is_some(),
            "managed Some snapshot is mandatory"
        );
        assert_eq!(case.accepted.candidate_snapshot.len(), 3);
        assert_eq!(
            case.accepted.file_system_id,
            case.subject.stat.file_system_id
        );
        let found = case.wait(|| case.birth().is_some());
        let stored_after_wait = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .placement_reconciliation(&case.accepted.file_system_id, &case.accepted.job_id)
            })
            .map_err(|e| e.to_string());
        case.evidence(
            "gap-before-original-assertion.json",
            serde_json::json!({"original_found":found,"actual_stored_job_after_wait":stored_after_wait}),
        );
        assert!(
            found,
            "actual default Main must produce managed Original under the paused carrier"
        );
        assert!(
            case.switches
                .placement_entries
                .lock()
                .unwrap()
                .iter()
                .any(|(job, _, _)| job == &case.accepted.job_id),
            "actual managed job must enter the native placement owner"
        );
        let birth = case.birth().unwrap();
        let DfsNetworkMessage::RepairReplica(command) = &birth.original.message else {
            panic!("normal Original is RepairReplica")
        };
        assert_eq!(
            command.expected_base_version,
            Some(case.subject.stat.version.clone())
        );
        assert!(command.background_repair);
        assert_eq!(command.version, case.subject.stat.version);
        assert_eq!(
            Some(&command.content_hash),
            case.subject.stat.content_hash.as_ref()
        );
        assert_eq!(birth.original.source_node_id, case.owner);
        assert_eq!(birth.original.target_node_id, case.target);
        assert!(birth.observed.is_empty() && birth.closed_at_sequence.is_none());
        let intent = case
            .cut()
            .unwrap()
            .intents
            .into_iter()
            .find(|i| i.intent_id == command.intent_id)
            .unwrap();
        assert_eq!(
            intent.intent_id,
            format!(
                "placement:{}:{}:{}",
                case.accepted.job_id, case.subject.stat.object_id, case.subject.stat.version
            )
        );
        assert_eq!(
            (
                intent.policy.min_commit_replicas,
                intent.policy.desired_replicas
            ),
            (1, 2)
        );
        assert_eq!(
            intent
                .desired_replica_node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            [case.owner.clone(), case.target.clone()]
                .into_iter()
                .collect()
        );
        assert!(intent.object_metadata.is_some());
        assert!(
            !intent
                .object_metadata
                .as_ref()
                .unwrap()
                .current_locations
                .contains(&case.target)
        );
        (case, birth)
    }
}
impl Drop for DefaultGapCase {
    fn drop(&mut self) {
        self.switches.deny_receipt.store(false, Ordering::SeqCst);
        self.switches.old_probe.store(false, Ordering::SeqCst);
        for gate in &self.gates {
            gate.close();
        }
        drop(self.close_denial.take());
        drop(self.source.take());
        drop(self.receiver.take());
        drop(self.source_hook.take());
        drop(self.receiver_hook.take());
        if let Some(native) = self.native.take() {
            let result = native.close();
            if std::thread::panicking() {
                if let Err(e) = result {
                    eprintln!("default gap native teardown: {e}");
                }
            } else {
                result.unwrap();
            }
        }
    }
}

#[test]
fn default_managed_observed_receipt_gap_cold_recontacts_same_original() {
    let (mut case, original) = DefaultGapCase::new("default-observed-receipt-cold", true);
    case.initial_gate.add_permits(1);
    let observed = case.wait(|| {
        case.birth().is_some_and(|b| !b.observed.is_empty())
            && case.switches.receipt_denials.load(Ordering::SeqCst) > 0
    });
    case.evidence(
        "gap-before-cold-premise.json",
        serde_json::json!({"observed_and_denied":observed}),
    );
    assert!(
        observed,
        "actual Committed observation must precede exact BeforePrepare denial"
    );
    assert_eq!(
        case.switches.receipt_premise_errors.load(Ordering::SeqCst),
        0,
        "the exact BeforePrepare boundary must itself observe the real Committed Original"
    );
    let before = case
        .queue()
        .original_repair(&original.original)
        .unwrap()
        .unwrap();
    assert!(
        before
            .observed
            .iter()
            .any(|r| matches!(r.result, DfsBlobTransferResult::Committed { .. }))
    );
    assert!(before.closed_at_sequence.is_none());
    let cut = case.cut().unwrap();
    assert!(cut.receipts.is_empty() && cut.prepares.is_empty());
    assert_eq!(case.initial_return().source_node_id, case.target);
    case.remove_gate_fault(&case.initial_gate, false);
    let calls_before = case
        .runtime()
        .rpc_observations()
        .iter()
        .map(|r| r.call_id)
        .collect::<BTreeSet<_>>();
    drop(case.source.take());
    // Drain the complete old source owner before qualifying its real due.
    // Receipt denial remains armed throughout that stopped, immutable cut.
    let owner = case.owner.clone();
    case.native.as_mut().unwrap().stop_node(&owner).unwrap();
    let eligibility = case.qualify_recorded_due(&original, "cold-stopped");
    let stopped_cut = case.cut().unwrap();
    let stopped_birth = case
        .queue()
        .original_repair(&original.original)
        .unwrap()
        .unwrap();
    assert!(stopped_cut.receipts.is_empty() && stopped_cut.prepares.is_empty());
    assert!(stopped_birth.closed_at_sequence.is_none());
    // No actor exists now. Disarm before reopen so a genuine new startup
    // return is not denied again and cooled into a second lease window.
    case.switches.deny_receipt.store(false, Ordering::SeqCst);
    case.evidence(
        "gap-cold-before-reopen.json",
        serde_json::json!({
            "actual_eligibility":eligibility,"actual_stopped_gap":stopped_cut,
            "actual_stopped_original":stopped_birth,"source_fully_stopped":true,
        }),
    );
    let business_started = Instant::now();
    let business_deadline = business_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let reopened = case
        .native
        .as_mut()
        .unwrap()
        .reopen_node_with_startup_deadline(
            owner,
            tokio::time::Instant::from_std(business_deadline),
        );
    let repaired = reopened.is_ok() && case.wait_until(business_deadline, || case.goal(&original));
    let after = case
        .queue()
        .original_repair(&original.original)
        .unwrap()
        .unwrap();
    let fresh_calls = case
        .runtime()
        .rpc_observations()
        .into_iter()
        .filter(|r| {
            !calls_before.contains(&r.call_id)
                && gap_rpc_matches_original(r, &original)
                && r.phase == RpcPhase::BeforeDispatch
        })
        .collect::<Vec<_>>();
    let fresh_received = case
        .switches
        .returned
        .lock()
        .unwrap()
        .iter()
        .any(|returned| {
            gap_received_original_bytes(returned, &original, &case.bytes)
                && fresh_calls
                    .iter()
                    .any(|r| r.dfs_frame_id == Some(returned.command.routed_message.frame_id))
        });
    let fresh = !fresh_calls.is_empty() && fresh_received;
    case.evidence("gap-cold-business-outcome.json",serde_json::json!({"goal":repaired,"fresh_actual_rpc":fresh,
        "fresh_same_body_received_at_c":fresh_received,"fresh_actual_calls":format!("{fresh_calls:#?}"),
        "before":before,"after":after,"actual_reopen_result":reopened,
        "business_elapsed_us":business_started.elapsed().as_micros(),
        "business_window":"one ORIGINAL DEFAULT5s budget starts before canonical reopen and includes startup plus goal observation; no reset/caller resend/manualtick"}));
    assert!(
        repaired && fresh,
        "cold normal Main must contact actual C then publish/close this same observed Original"
    );
    assert!(after.attempts.len() > before.attempts.len());
    case.verify_goal(&original);
}

#[test]
fn default_old_return_enters_serving_while_fresh_contact_is_held_but_cannot_publish() {
    let (mut case, original) = DefaultGapCase::new("default-observed-old-return", false);
    let actual_owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(case.runtime().data_dir(&case.owner)),
    )
    .unwrap();
    case.close_denial = Some(
        case.queue()
            .deny_default_original_repair_close_for_test(&actual_owner, &original.original)
            .unwrap(),
    );
    drop(actual_owner);
    case.initial_gate.add_permits(1);
    let published = case.wait(|| {
        case.cut().is_ok_and(|c| {
            c.receipts.len() == 1
                && c.object
                    .as_ref()
                    .is_some_and(|o| o.current_locations.contains(&case.target))
        }) && case.close_denial.as_ref().unwrap().hits() > 0
    });
    case.evidence(
        "gap-before-withdrawal-premise.json",
        serde_json::json!({"actual_receipt_and_denied_terminal":published}),
    );
    assert!(
        published,
        "default post-receipt denial must preserve genuine pending custody"
    );
    let open = case
        .queue()
        .original_repair(&original.original)
        .unwrap()
        .unwrap();
    assert!(open.closed_at_sequence.is_none() && !open.observed.is_empty());
    let row = case
        .queue()
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|r| {
            r.intent_id == open.root.subject.intent_id && r.missing_replica_node_id == case.target
        })
        .expect("actual named pending producer row");
    assert!(row.transfer_request_id.is_some() && row.completed_transfer_request_id.is_none());
    case.remove_gate_fault(&case.initial_gate, false);
    let old = case.initial_return();
    let mut source = case.source.take().unwrap();
    let admitted = native_fixture_write_admission(case.runtime(), &source, &case.path);
    let actual_scope = source
        .metadata_backend()
        .expect("actual source metadata backend")
        .publication_owner_scope(&case.subject.stat.file_system_id, &case.subject.shard)
        .unwrap();
    let admission_before = source
        .publication_execution
        .current()
        .is_some_and(|owners| {
            owners.admits(
                &actual_scope,
                &case.subject.stat.file_system_id,
                &case.subject.shard,
            )
        });
    assert!(
        admission_before,
        "SETUP owns the actual source mutation admission before physical withdrawal"
    );
    let receiver = case.receiver.as_ref().unwrap();
    assert_eq!(
        receiver
            .content()
            .read_object_version(
                case.subject.stat.object_id.as_str(),
                case.subject.stat.version.as_str()
            )
            .unwrap()
            .1,
        case.bytes
    );
    receiver
        .content()
        .debug_remove_loose_blob(case.subject.stat.content_hash.as_deref().unwrap())
        .unwrap();
    assert!(
        !receiver
            .content()
            .has_blob(case.subject.stat.content_hash.as_deref().unwrap())
            .unwrap()
    );
    source
        .mark_remote_read_location_missing(
            &case.subject.stat.object_id,
            &case.subject.stat.version,
            &case.subject.stat.content_hash,
            &case.target,
        )
        .unwrap();
    // This direct, genuinely admitted Core action produced real committed work.
    // Complete its existing host-output boundary once, after releasing admission.
    let withdrawal_work = source.take_message_committed_work();
    assert!(
        withdrawal_work.publish_metadata && withdrawal_work.repair_maintenance,
        "actual withdrawal must report its real publication/repair maintenance work"
    );
    let withdrawn = case.cut().unwrap();
    assert!(withdrawn.receipts.is_empty());
    assert!(
        !withdrawn
            .object
            .as_ref()
            .unwrap()
            .current_locations
            .contains(&case.target)
    );
    case.close_denial.take().unwrap().release().unwrap();
    let fresh_gate = Arc::new(tokio::sync::Semaphore::new(0));
    case.gates.push(Arc::clone(&fresh_gate));
    case.runtime().install_fault(FaultRule {
        source: Some(case.owner.clone()),
        target: Some(case.target.clone()),
        route: Some(DFS_DATA_REPAIR_REPLICA_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::PauseAfterDispatch(Arc::clone(&fresh_gate)),
    });
    let eligibility = case.qualify_recorded_due(&original, "warm-admitted");
    let admission_after = source
        .publication_execution
        .current()
        .is_some_and(|owners| {
            owners.admits(
                &actual_scope,
                &case.subject.stat.file_system_id,
                &case.subject.shard,
            )
        });
    assert!(
        admission_after,
        "SETUP retains the SAME actual admission throughout eligibility"
    );
    case.evidence("gap-warm-before-normal-action.json",serde_json::json!({
        "actual_eligibility":eligibility,"actual_admission_before":admission_before,
        "actual_admission_after":admission_after,"actual_scope_debug":format!("{actual_scope:?}"),
        "qualification":"only scalar admission facts retained; temporary owners Arc dropped before releasing guard",
    }));
    let before_calls = case
        .runtime()
        .rpc_observations()
        .iter()
        .map(|r| r.call_id)
        .collect::<BTreeSet<_>>();
    case.source = Some(source);
    // SETUP only: actual wall-clock due is not the resident logical retry
    // clock. Allow the existing natural cadence to qualify a real held fresh
    // contact, without a manual tick/wake, row rewrite or product5s SLA claim.
    let setup_row = case.queue().view().unwrap().records.into_iter().find(|row|
        row.intent_id == original.root.subject.intent_id
            && row.missing_replica_node_id == case.target)
        .expect("actual named producer row before normal setup action");
    let actual_lease_nanos = setup_row.next_retry_unix_nanos
        .checked_sub(setup_row.last_attempt_unix_nanos)
        .expect("actual due must follow its native attempt");
    let tick_nanos = crate::transfer::repair_retry_delay_nanos_for_ticks(1);
    let encoded_lease_ticks = actual_lease_nanos.div_ceil(tick_nanos);
    let cadence_cycles = encoded_lease_ticks.max(
        crate::transfer::repair_transfer_inflight_timeout_ticks(GAP_BYTES as u64));
    let remaining_due_nanos = setup_row.next_retry_unix_nanos
        .saturating_sub(crate::metadata_records::dfs_now_unix_nanos());
    let cadence = crate::maintenance_schedule::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL;
    let setup_budget = cadence.checked_mul(u32::try_from(cadence_cycles)
        .expect("actual lease tick bound representable"))
        .and_then(|value| value.checked_add(Duration::from_nanos(remaining_due_nanos)))
        .and_then(|value| value.checked_add(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT))
        .expect("derived normal-cadence setup bound representable");
    let setup_started = Instant::now();
    let setup_deadline = setup_started.checked_add(setup_budget).unwrap();
    case.evidence("gap-warm-normal-contact-setup-bound.json", serde_json::json!({
        "actual_row":setup_row,"actual_remaining_due_nanos":remaining_due_nanos,
        "actual_encoded_lease_ticks":encoded_lease_ticks,
        "existing_transfer_lease_ticks":crate::transfer::repair_transfer_inflight_timeout_ticks(GAP_BYTES as u64),
        "normal_cadence_ms":cadence.as_millis(),"setup_budget_ms":setup_budget.as_millis(),
        "qualification":"one PASSIVE SETUP bound from actual recorded lease/due + existing cadence; conservative tick-gap upper bound, not an observed RAM tick or product contact SLA; no rolling extension",
    }));
    drop(admitted);
    let data = case.runtime().data_dir(&case.owner);
    let cluster_paths = sinomo_core::network::ClusterPaths::new(data);
    let execution_scope = crate::DfsMessageExecutionScope::ExactShard {
        file_system_id: case.subject.stat.file_system_id.clone(),
        shard_id: case.subject.shard.clone(),
    };
    let edge_before = crate::runtime_host::daemon_dfs_repair_maintenance_edge_request_count(data);
    let emitted = crate::runtime_host::emit_daemon_dfs_committed_edges(
        data,
        &cluster_paths,
        &execution_scope,
        crate::test_runtime::executor().handle(),
        None,
        withdrawal_work,
        &[],
    );
    let edge_after = crate::runtime_host::daemon_dfs_repair_maintenance_edge_request_count(data);
    case.evidence("gap-warm-actual-withdrawal-output.json",serde_json::json!({
        "actual_withdrawal_work_debug":format!("{withdrawal_work:?}"),
        "actual_emitted_edges_debug":format!("{emitted:?}"),
        "actual_scope_debug":format!("{execution_scope:?}"),
        "actual_repair_edge_before":edge_before,"actual_repair_edge_after":edge_after,
        "qualification":"complete ACTUAL Core committed output through the EXISTING real Main host after admission release; no fabricated work/tick/timer/semantic resend",
        "setup_elapsed_us":setup_started.elapsed().as_micros(),
    }));
    assert!(
        edge_after > edge_before,
        "actual Main host must receive the real withdrawal repair edge"
    );
    let held = case.wait_until(setup_deadline, || {
        let observations = case.runtime().rpc_observations();
        observations.iter().any(|r| {
            !before_calls.contains(&r.call_id)
                && gap_rpc_matches_original(r, &original)
                && r.phase == RpcPhase::AfterDispatch
                && !observations.iter().any(|end| end.call_id == r.call_id
                    && matches!(end.phase, RpcPhase::Completed | RpcPhase::EndedWithoutResponse))
        })
    });
    case.evidence(
        "gap-before-old-return-while-fresh-held.json",
        serde_json::json!({"fresh_actual_after_dispatch_held":held,"withdrawn":withdrawn,
            "setup_elapsed_us":setup_started.elapsed().as_micros(),
            "setup_budget_ms":setup_budget.as_millis(),
            "coverage_change":"24t/x/y failed5s background-contact windows remain preserved/unqualified. This qualifies actual held fresh contact as SETUP before a separate authority-action window; not a5s repair SLA."}),
    );
    assert!(held, "SETUP UNQUALIFIED: normal cadence did not reach actual held fresh RPC");
    let observations = case.runtime().rpc_observations();
    let held_call = observations.iter().find(|r| {
        !before_calls.contains(&r.call_id) && gap_rpc_matches_original(r, &original)
            && r.phase == RpcPhase::AfterDispatch
            && !observations.iter().any(|end| end.call_id == r.call_id
                && matches!(end.phase, RpcPhase::Completed | RpcPhase::EndedWithoutResponse))
    }).expect("the actual held fresh carrier has a live native call identity").clone();
    let held_call_id = held_call.call_id;
    // Actual DEFAULT background Repair has connect/idle5s but no overall
    // deadline. Never invent/restart an original5s whole-call budget. A future
    // Some budget needs its actual absolute-deadline evidence before this case.
    assert!(held_call.options.overall_timeout().is_none(),
        "SETUP UNQUALIFIED: actual fresh whole-call deadline needs original absolute evidence");
    case.evidence("gap-warm-held-fresh-budget.json", serde_json::json!({
        "actual_held_call_debug":format!("{held_call:#?}"),
        "actual_overall_timeout":null,
        "qualification":"actual fresh call remains under unchanged native None-overall budget; connect/idle timeouts are not an overall deadline",
    }));
    let action_started = Instant::now();
    let action_deadline = action_started + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    *case.switches.old_frame.lock().unwrap() = Some(old.clone());
    case.switches.old_probe.store(true, Ordering::SeqCst);
    let old_frame_id = old.frame_id;
    let old_calls_before = case
        .runtime()
        .rpc_observations()
        .iter()
        .map(|r| r.call_id)
        .collect::<BTreeSet<_>>();
    let before_birth = case
        .queue()
        .original_repair(&original.original)
        .unwrap()
        .unwrap();
    let before_row = case
        .queue()
        .view()
        .unwrap()
        .records
        .into_iter()
        .find(|row| {
            row.intent_id == before_birth.root.subject.intent_id
                && row.missing_replica_node_id == case.target
        })
        .expect("actual pending scheduling row must exist while the fresh call is held");
    assert!(
        before_row.transfer_request_id.is_some()
            && before_row.completed_transfer_request_id.is_none()
    );
    let mut receiver = case.receiver.take().unwrap();
    let source_id = case.owner.clone();
    let old_result = std::thread::scope(|scope| {
        let _release_on_unwind = GapGateCleanup(Arc::clone(&fresh_gate));
        let worker = scope.spawn(move || {
            let previous = receiver.set_transport_operation_deadline(Some(action_deadline));
            let result = receiver
                .transport
                .send_routed_call(&source_id, DfsRoutedCall::value(old));
            receiver.set_transport_operation_deadline(previous);
            (receiver, result)
        });
        let (receiver, result) = worker.join().unwrap();
        case.receiver = Some(receiver);
        let entry = case.switches.old_entry.lock().unwrap().clone();
        let exit = case.switches.old_exit.lock().unwrap().clone();
        let old_ingress = case.runtime().rpc_observations().iter().any(|r| {
            !old_calls_before.contains(&r.call_id)
                && r.source == case.target
                && r.target == case.owner
                && r.dfs_frame_id == Some(old_frame_id)
                && r.phase == RpcPhase::Decoded
        });
        let after_birth = case
            .queue()
            .original_repair(&original.original)
            .unwrap()
            .unwrap();
        let after_row = case
            .queue()
            .view()
            .unwrap()
            .records
            .into_iter()
            .find(|row| {
                row.intent_id == before_birth.root.subject.intent_id
                    && row.missing_replica_node_id == case.target
            })
            .expect("old DATA must retain the actual pending scheduling row");
        let fresh_still_pending = !case.runtime().rpc_observations().iter().any(|r| {
            r.call_id == held_call_id
                && matches!(
                    r.phase,
                    RpcPhase::Completed | RpcPhase::EndedWithoutResponse
                )
        });
        case.evidence("gap-old-serving-before-fresh-release.json",serde_json::json!({"actual_old_send_result":format!("{result:?}"),
            "before_birth":before_birth,"after_birth":after_birth,"serving_entry":entry,"serving_exit":exit,
            "actual_old_ingress":old_ingress,"actual_old_frame_id":old_frame_id,
            "before_scheduling_row":before_row,"after_scheduling_row":after_row,
            "held_fresh_call_id":held_call_id,"fresh_still_pending_before_gate_release":fresh_still_pending,
            "authority_action_elapsed_us":action_started.elapsed().as_micros(),
            "authority_action_deadline_remaining_us":action_deadline.saturating_duration_since(Instant::now()).as_micros(),
            "action_scope":"separateDEFAULT5s old-frame authority action after actual fresh contact qualified asSETUP; no fresh RPCbudget reset"}));
        assert!(Instant::now() < action_deadline,
            "old-frame authority action exceeded its original local5s bound");
        assert!(
            fresh_still_pending,
            "the actual fresh carrier must remain nonterminal under its unchanged native budget throughout the old callback control"
        );
        assert!(
            old_ingress,
            "the real preowned C transport must reach actual native decoding of the exact captured old frame at A"
        );
        let (admission_held, before) = entry
            .expect("actual old frame MUST enter serving; lock blocking is not a negative PASS");
        assert!(
            admission_held,
            "old raw callback must own real runtime admission"
        );
        let before = before.unwrap();
        let after = exit
            .expect("actual old callback completed its source execution report")
            .unwrap();
        assert_eq!(
            before, after,
            "old raw DATA changed source metadata while fresh native call was still held"
        );
        assert!(
            after.receipts.is_empty()
                && !after
                    .object
                    .unwrap()
                    .current_locations
                    .contains(&case.target)
        );
        assert_eq!(
            after_birth, before_birth,
            "old DATA changed Original/attempt/observation/closure custody"
        );
        assert_eq!(
            after_row, before_row,
            "old DATA changed pending scheduling/delivery/completion custody"
        );
        assert!(after_birth.closed_at_sequence.is_none());
        assert!(!fresh_gate.is_closed(), "actual held fresh gate must remain open before release");
        fresh_gate.add_permits(1);
        assert!(!fresh_gate.is_closed(), "normal release is a permit, never gate cancellation");
        result
    });
    assert!(!fresh_gate.is_closed(), "normal scope exit must not abort the same native reply");
    case.switches.old_probe.store(false, Ordering::SeqCst);
    case.remove_gate_fault(&fresh_gate, true);
    let repaired = case.wait(|| case.goal(&original));
    let held_call_completed = case
        .runtime()
        .rpc_observations()
        .iter()
        .any(|r| r.call_id == held_call_id && r.phase == RpcPhase::Completed);
    case.evidence(
        "gap-fresh-native-final-outcome.json",
        serde_json::json!({"actual_old_send_result":format!("{old_result:?}"),"goal":repaired,
            "held_fresh_call_id":held_call_id,"held_fresh_call_completed":held_call_completed}),
    );
    assert!(
        repaired && held_call_completed,
        "only the same real fresh return may finish receipt and exact Original closure"
    );
    case.verify_goal(&original);
}
