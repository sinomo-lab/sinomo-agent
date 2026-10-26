//! Actual direct-return provenance only; no serving or durable attestation.
use super::*;
use crate::node_transport::{
    DfsMetadataPrefixEvidence, DfsMetadataPrefixProbeAttempt, DfsMetadataPrefixProbeReturn,
};
use crate::test_runtime::{
    DfsTestRuntime, FaultAction, FaultRule, RpcObservation, RpcOrigin, RpcPhase,
};
use crate::{AsyncDfsPeerTransport, DfsPeerTransport, DfsPeerTransportError};
use sinomo_core::network::{NetworkError, ObjectRpcCall, ObjectRpcResponse, ObjectRpcResult};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const CAP: Duration = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;

struct Fixture {
    native: DfsTestRuntime,
    nodes: Vec<NodeId>,
    shard: DfsMetadataShardConfig,
    command: crate::DfsMetadataJournalBatchCommand,
}
impl Fixture {
    fn new(label: &str) -> TestResult<Self> {
        let (native, nodes) = DfsTestRuntime::start_cluster(label, 3)?;
        let shard = native.root_shard()?;
        assert_eq!(shard.replica_node_ids.len(), 2);
        let path = "/native-prefix-return-seed";
        let mut main = native.client(&shard.primary_node_id)?;
        let deadline = Instant::now() + CAP;
        let old = main.set_transport_operation_deadline(Some(deadline));
        let seeded = main.posix_mkdir(path);
        main.set_transport_operation_deadline(old);
        seeded?;
        assert!(Instant::now() <= deadline);
        drop(main);
        let mut source = native.metadata_primary_service(path)?;
        assert!(source.native_publication_owner_for_observation().is_some());
        let object = source
            .metadata_backend()
            .unwrap()
            .get_object_by_path(&shard.file_system_id, path)?
            .ok_or("real Main seed unavailable")?;
        let target = source.capture_metadata_quorum_sync_target(&object)?;
        let complete = source.sync_metadata_shard_to_quorum(&target, CAP)?;
        assert_eq!(complete.acknowledged_node_ids.len(), 3);
        let target = source.capture_metadata_quorum_sync_target(&object)?;
        assert_eq!(target.expected_shard.as_ref(), Some(&shard));
        let command = crate::DfsMetadataJournalBatchCommand {
            request_id: format!("native-prefix-return-{}", sinomo_core::Id::generate()),
            source_node_id: Some(shard.primary_node_id.clone()),
            file_system_id: Some(shard.file_system_id.clone()),
            authority_epoch: Some(shard.epoch),
            verify_prefix_only: true,
            prefix_probe: Some(crate::DfsMetadataPrefixProbe {
                barrier_id: target.barrier_id,
                file_system_id: Some(shard.file_system_id.clone()),
                authority_epoch: Some(shard.epoch),
                shard_id: target.shard_id.clone(),
                journal_sequence: target.journal_sequence,
                state_fingerprint: target.state_fingerprint,
                primary_handoff_id: None,
                repair_custody: None,
            }),
            authority_probe: None,
            durable_replication: None,
            batch: crate::DfsMetadataJournalFrameBatch {
                shard_id: target.shard_id,
                next_after_sequence: target.journal_sequence,
                journal_frames: Vec::new(),
            },
        };
        drop(source); // No metadata lease is held across the capture RPC.
        Ok(Self {
            native,
            nodes,
            shard,
            command,
        })
    }
    fn peer(&self) -> NodeId {
        self.shard.replica_node_ids[1].clone()
    }
    fn source_root(&self) -> PathBuf {
        self.native.data_dir(&self.shard.primary_node_id).root()
    }
    fn transport(&self) -> crate::DfsNodeTransport {
        daemon_dfs_node_transport_on_runtime(
            self.shard.primary_node_id.clone(),
            self.nodes.clone(),
            self.native.data_dir(&self.shard.primary_node_id),
            &self.native.runtime_handle(),
        )
    }
    fn markers(&self) -> TestResult<BTreeMap<PathBuf, Option<Vec<u8>>>> {
        self.nodes
            .iter()
            .map(|node| {
                let path = crate::local_dfs_metadata_shard_path(
                    self.native.data_dir(node),
                    &DfsMetadataShardId::new(&self.shard.shard_id),
                )
                .join("journal/membership-handoff-write-freeze.sobj");
                let bytes = match fs::read(&path) {
                    Ok(bytes) => Some(bytes),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(e.into()),
                };
                Ok((path, bytes))
            })
            .collect()
    }
}
fn projection(cap: &OwnedNativeMetadataPrefixReturn) -> serde_json::Value {
    let p = cap.local_physical_owner();
    serde_json::json!({"attempt":cap.attempt_id(),"peer":cap.peer(),"command":cap.command(),
        "response":cap.response(),"assignment":cap.source_assignment(),"prefix":cap.source_prefix(),
        "local_physical":{"data_root":p.data_root,"data_identity":p.data_identity,
        "owner_identity":p.owner_identity,"shard_root":p.shard_root,"shard_identity":p.shard_identity},
        "remote_physical_generation":"not supplied by authenticated protocol; unavailable",
        "authority":"historical native return, not current effect or serving authority"})
}
fn wire(events: &[RpcObservation]) -> serde_json::Value {
    serde_json::json!(
        events
            .iter()
            .map(|e| serde_json::json!({"call":e.call_id,"source":e.source,
        "target":e.target,"route":e.route,"phase":format!("{:?}",e.phase),
        "origin":format!("{:?}",e.origin),"error":e.outcome_error}))
            .collect::<Vec<_>>()
    )
}
fn own_calls(events: &[RpcObservation], source: &NodeId, peer: &NodeId) -> BTreeSet<u64> {
    events
        .iter()
        .filter(|e| {
            e.source == *source
                && e.target == *peer
                && e.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                && e.origin == RpcOrigin::RegisteredHost
                && e.phase == RpcPhase::Constructed
        })
        .map(|e| e.call_id)
        .collect()
}
fn invoke(
    transport: &mut crate::DfsNodeTransport,
    peer: &NodeId,
    command: &crate::DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataPrefixProbeReturn, NetworkError> {
    transport.set_operation_deadline(Some(Instant::now() + CAP));
    transport.capture_metadata_prefix_probe_direct(peer, command)
}
fn native_value(
    returned: DfsMetadataPrefixProbeReturn,
) -> TestResult<OwnedNativeMetadataPrefixReturn> {
    let response = returned.response;
    let DfsMetadataPrefixEvidence::Native(cap) = returned.evidence else {
        return Err("actual protected native return did not produce private evidence".into());
    };
    assert_eq!(cap.response(), &response);
    Ok(cap)
}
fn run(
    label: &str,
    case: impl FnOnce(
        &mut Fixture,
        &mut serde_json::Value,
    ) -> TestResult<Vec<OwnedNativeMetadataPrefixReturn>>,
) {
    let artifact = sinomo_testdir::TestDir::new(label);
    let mut fixture = Fixture::new(label).unwrap();
    let source_root = fixture.source_root();
    let owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(
        fixture.native.data_dir(&fixture.shard.primary_node_id),
    ))
    .unwrap();
    let weak = Arc::downgrade(&owner);
    drop(owner);
    let mut report = serde_json::json!({"scope":"actual native direct PREFIX return on existing initialized full3",
        "limits":"capture only; no durable attestation/serving/C1/remote physical generation/quietness claim",
        "request":fixture.command});
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        case(&mut fixture, &mut report)
    }));
    report["all_rpc"] = wire(&fixture.native.rpc_observations());
    let close = fixture.native.close();
    report["normal_close"] = serde_json::json!(close);
    report["source_owner_released"] = serde_json::json!(weak.upgrade().is_none());
    let after = if let Ok(Ok(values)) = &outcome {
        values
            .iter()
            .map(|cap| {
                serde_json::json!({"historical_data":projection(cap),
            "live_binding_after_close":cap.validate_binding(cap.peer(),cap.command(),&source_root)
                .map_err(|e|e.to_string())})
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    report["held_values_after_close"] = serde_json::json!(after);
    let saved = (|| -> TestResult<()> {
        let bytes = serde_json::to_vec_pretty(&report)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("bounded native prefix-return evidence overflow".into());
        }
        let path = artifact.join("native-prefix-return-report.json");
        let mut f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        eprintln!("NATIVE_PREFIX_RETURN_REPORT {}", path.display());
        Ok(())
    })();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    let values = outcome.unwrap().unwrap();
    saved.unwrap();
    close.unwrap();
    assert!(
        weak.upgrade().is_none(),
        "completed evidence must not pin the source DATA owner"
    );
    for cap in values {
        assert!(
            cap.validate_binding(cap.peer(), cap.command(), &source_root)
                .is_err(),
            "historical getters after close cannot mint a live owner binding"
        );
    }
}

#[test]
fn native_prefix_return_captures_actual_direct_call_without_pinning_owner() {
    run("native-prefix-return-owned", |fixture, report| {
        let peer = fixture.peer();
        let command = fixture.command.clone();
        let markers = fixture.markers()?;
        assert!(markers.values().any(Option::is_some));
        let cursor = fixture.native.rpc_observations().len();
        let mut transport = fixture.transport();
        let returned = invoke(&mut transport, &peer, &command);
        report["result"] = serde_json::json!(
            returned
                .as_ref()
                .map(|r| &r.response)
                .map_err(ToString::to_string)
        );
        report["rpc"] = wire(&fixture.native.rpc_observations()[cursor..]);
        let cap = native_value(returned?)?;
        assert_eq!(cap.command(), &command);
        assert_eq!(cap.peer(), &peer);
        assert_eq!(cap.source_assignment(), &fixture.shard);
        assert_eq!(
            cap.source_prefix().journal_sequence,
            command.batch.next_after_sequence
        );
        assert_eq!(
            cap.source_prefix().digest,
            command.prefix_probe.as_ref().unwrap().state_fingerprint
        );
        cap.validate_binding(&peer, &command, &fixture.source_root())?;
        let owner = require_daemon_dfs_data_owner_lock(&DfsRuntimePaths::new(
            fixture.native.data_dir(&fixture.shard.primary_node_id),
        ))?;
        assert_eq!(
            cap.local_physical_owner().owner_identity,
            owner.storage_owner_file_identity()?
        );
        assert_eq!(
            cap.local_physical_owner().data_root,
            owner.physical_data_root()
        );
        drop(owner);
        let receiver_root = crate::local_dfs_metadata_shard_path(
            fixture.native.data_dir(&peer),
            &command.batch.shard_id,
        );
        let floor = crate::metadata_cursor::metadata_prefix_ack_floor(
            &receiver_root,
            &command.batch.shard_id,
        )?
        .ok_or("actual receiver has no prefix floor after its verified response")?;
        report["receiver_floor_data"] = serde_json::json!(floor);
        assert_eq!(floor.journal_sequence, command.batch.next_after_sequence);
        assert_eq!(
            floor.state_fingerprint,
            command.prefix_probe.as_ref().unwrap().state_fingerprint
        );
        assert_eq!(
            own_calls(
                &fixture.native.rpc_observations()[cursor..],
                &fixture.shard.primary_node_id,
                &peer
            )
            .len(),
            1,
            "capture must own exactly one RPC, not repeat a prior call for provenance"
        );
        assert_eq!(fixture.markers()?, markers);
        report["captured"] = projection(&cap);
        assert_eq!(fixture.native.observation_dropped_count(), 0);
        drop(transport);
        Ok(vec![cap])
    });
}

struct RecordedPeer {
    response: crate::DfsMetadataJournalBatchResponse,
    calls: AtomicUsize,
}
impl DfsPeerTransport for RecordedPeer {
    fn call_object_rpc(
        &self,
        _peer: &NodeId,
        call: ObjectRpcCall,
        _options: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ObjectRpcResponse::encode_body(&call.request.route, &self.response)
            .map(ObjectRpcResult::value)
            .map_err(|e| DfsPeerTransportError::new(e.to_string()))
    }
}
impl AsyncDfsPeerTransport for RecordedPeer {
    fn call_object_rpc_async<'a>(
        &'a self,
        peer: &'a NodeId,
        call: ObjectRpcCall,
        options: crate::DfsObjectRpcSendOptions,
    ) -> crate::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async move { DfsPeerTransport::call_object_rpc(self, peer, call, options) })
    }
}
struct OldEvidencePeer {
    value: Mutex<Option<DfsMetadataPrefixProbeReturn>>,
}
impl DfsPeerTransport for OldEvidencePeer {
    fn call_object_rpc(
        &self,
        _: &NodeId,
        _: ObjectRpcCall,
        _: crate::DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        Err(DfsPeerTransportError::new("raw replay path must not run"))
    }
    fn call_metadata_prefix_probe(
        &self,
        _: &DfsMetadataPrefixProbeAttempt,
        _: crate::DfsObjectRpcSendOptions,
    ) -> Result<DfsMetadataPrefixProbeReturn, DfsPeerTransportError> {
        self.value
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| DfsPeerTransportError::new("single old-value replay spent"))
    }
}
impl AsyncDfsPeerTransport for OldEvidencePeer {
    fn call_object_rpc_async<'a>(
        &'a self,
        _: &'a NodeId,
        _: ObjectRpcCall,
        _: crate::DfsObjectRpcSendOptions,
    ) -> crate::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
        Box::pin(async {
            Err(DfsPeerTransportError::new(
                "no asynchronous native replay path",
            ))
        })
    }
}
#[test]
fn native_prefix_return_refuses_generic_data_and_old_attempt_peer_owner_reuse() {
    run("native-prefix-return-bindings", |fixture, report| {
        let peer = fixture.peer();
        let command = fixture.command.clone();
        let root = fixture.source_root();
        let mut transport = fixture.transport();
        let original = native_value(invoke(&mut transport, &peer, &command)?)?;
        let second = native_value(invoke(&mut transport, &peer, &command)?)?;
        assert_ne!(
            original.attempt_id(),
            second.attempt_id(),
            "same logical command has separate actual calls"
        );
        let wrong_peer = fixture.shard.replica_node_ids[0].clone();
        assert!(
            original
                .validate_binding(&wrong_peer, &command, &root)
                .is_err()
        );
        let mut changed = command.clone();
        changed.request_id.push_str("-foreign");
        assert!(original.validate_binding(&peer, &changed, &root).is_err());
        assert!(
            original
                .validate_binding(
                    &peer,
                    &command,
                    &fixture.native.data_dir(&wrong_peer).root()
                )
                .is_err()
        );
        let data = original.response().clone();
        report["actual_original"] = projection(&original);
        report["actual_second"] = projection(&second);
        let before = fixture.native.rpc_observations().len();
        let fake = Arc::new(RecordedPeer {
            response: data.clone(),
            calls: AtomicUsize::new(0),
        });
        let mut generic = crate::DfsNodeTransport::with_peer_transports_for_nodes(
            fixture.shard.primary_node_id.clone(),
            fixture.nodes.clone(),
            CAP,
            fake.clone(),
            fake.clone(),
        );
        let decoded = invoke(&mut generic, &peer, &command)?;
        assert_eq!(decoded.response, data);
        assert!(matches!(
            decoded.evidence,
            DfsMetadataPrefixEvidence::NoNativeProof
        ));
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        let replay = Arc::new(OldEvidencePeer {
            value: Mutex::new(Some(DfsMetadataPrefixProbeReturn {
                response: data,
                evidence: DfsMetadataPrefixEvidence::Native(original),
            })),
        });
        let mut replaying = crate::DfsNodeTransport::with_peer_transports_for_nodes(
            fixture.shard.primary_node_id.clone(),
            fixture.nodes.clone(),
            CAP,
            replay.clone(),
            replay,
        );
        let stale = invoke(&mut replaying, &peer, &command);
        report["reused_actual_old_value"] = serde_json::json!(
            stale
                .as_ref()
                .map(|r| &r.response)
                .map_err(ToString::to_string)
        );
        assert!(
            stale.is_err(),
            "generic peer cannot rebind an actual old value to this fresh producer attempt"
        );
        assert_eq!(
            fixture.native.rpc_observations().len(),
            before,
            "generic DATA/replay adds no native RPC"
        );
        let (other_sync, other_async) = daemon_dfs_peer_transports_on_runtime(
            fixture.native.data_dir(&wrong_peer),
            &fixture.native.runtime_handle(),
        );
        let mut foreign = crate::DfsNodeTransport::with_peer_transports_for_nodes(
            fixture.shard.primary_node_id.clone(),
            fixture.nodes.clone(),
            CAP,
            other_sync,
            other_async,
        );
        let rejected = invoke(&mut foreign, &peer, &command);
        report["actual_foreign_native_owner"] = serde_json::json!(
            rejected
                .as_ref()
                .map(|r| &r.response)
                .map_err(ToString::to_string)
        );
        assert!(rejected.is_err());
        assert_eq!(
            fixture.native.rpc_observations().len(),
            before,
            "foreign native owner must refuse before dispatch"
        );
        drop(foreign);
        drop(replaying);
        drop(generic);
        drop(transport);
        Ok(vec![second])
    });
}

struct Hold<'a> {
    native: &'a DfsTestRuntime,
    gate: Arc<Semaphore>,
}
impl Drop for Hold<'_> {
    fn drop(&mut self) {
        self.native.remove_faults_matching(
            |r| matches!(&r.action,FaultAction::PauseAfterDispatch(g) if Arc::ptr_eq(g,&self.gate)),
        );
        self.gate.add_permits(1);
    }
}
fn hold<'a>(fixture: &'a Fixture, peer: &NodeId) -> Hold<'a> {
    let gate = Arc::new(Semaphore::new(0));
    fixture.native.install_fault(FaultRule {
        source: Some(fixture.shard.primary_node_id.clone()),
        target: Some(peer.clone()),
        route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
        call_id: None,
        origin: Some(RpcOrigin::RegisteredHost),
        remaining_hits: 1,
        action: FaultAction::PauseAfterDispatch(Arc::clone(&gate)),
    });
    Hold {
        native: &fixture.native,
        gate,
    }
}
fn drain_calls(native: &DfsTestRuntime, calls: &BTreeSet<u64>) -> TestResult<()> {
    let deadline = Instant::now() + CAP;
    loop {
        let events = native.rpc_observations();
        if calls.iter().all(|id| {
            events.iter().any(|e| {
                e.call_id == *id
                    && matches!(
                        e.phase,
                        RpcPhase::Completed | RpcPhase::EndedWithoutResponse
                    )
            })
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("owned direct call lacks terminal evidence within5s drain".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn native_prefix_return_loss_cannot_mint_completion_and_same_query_retries() {
    run("native-prefix-return-loss", |fixture, report| {
        let peer = fixture.peer();
        let command = fixture.command.clone();
        let mut transport = fixture.transport();
        let cursor = fixture.native.rpc_observations().len();
        let guard = hold(fixture, &peer);
        let failed = invoke(&mut transport, &peer, &command);
        let events = fixture.native.rpc_observations();
        let window = &events[cursor..];
        report["failed_result"] = serde_json::json!(
            failed
                .as_ref()
                .map(|r| &r.response)
                .map_err(ToString::to_string)
        );
        report["held_rpc"] = wire(window);
        let calls = own_calls(window, &fixture.shard.primary_node_id, &peer);
        assert_eq!(calls.len(), 1);
        assert!(
            window
                .iter()
                .any(|e| calls.contains(&e.call_id) && e.phase == RpcPhase::AfterDispatch)
        );
        assert!(
            failed.is_err(),
            "receiver-side completion or a held/lost response is not a captured native return"
        );
        drop(guard);
        drain_calls(&fixture.native, &calls)?;
        fixture.native.assert_fault_budgets()?;
        let retry_cursor = fixture.native.rpc_observations().len();
        let cap = native_value(invoke(&mut transport, &peer, &command)?)?;
        report["fresh_actual_return"] = projection(&cap);
        let retry = fixture.native.rpc_observations();
        let retry = &retry[retry_cursor..];
        report["fresh_rpc"] = wire(retry);
        let fresh = own_calls(retry, &fixture.shard.primary_node_id, &peer);
        assert_eq!(fresh.len(), 1);
        assert!(fresh.is_disjoint(&calls));
        assert_eq!(cap.command(), &command);
        drop(transport);
        Ok(vec![cap])
    });
}

fn identity(path: &Path) -> TestResult<(u64, u64)> {
    Ok(storage_file_identity(&fs::File::open(path)?)?)
}
struct OwnerNameReplacement {
    path: PathBuf,
    old: PathBuf,
    retained_new: PathBuf,
    old_identity: (u64, u64),
    replacement_identity: (u64, u64),
    restored: bool,
}
impl OwnerNameReplacement {
    fn install(path: PathBuf) -> TestResult<Self> {
        let old = path.with_file_name("prefix-capture-owned-original.lock");
        let retained_new = path.with_file_name("prefix-capture-owned-replacement.lock");
        if old.exists() || retained_new.exists() {
            return Err("owned replacement artifact already exists".into());
        }
        let old_identity = identity(&path)?;
        fs::rename(&path, &old)?;
        let created = (|| -> TestResult<(u64, u64)> {
            let f = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            f.sync_all()?;
            Ok(storage_file_identity(&f)?)
        })();
        let replacement_identity = match created {
            Ok(value) => value,
            Err(error) => {
                if !path.exists() {
                    let _ = fs::rename(&old, &path);
                }
                return Err(error);
            }
        };
        assert_ne!(old_identity, replacement_identity);
        Ok(Self {
            path,
            old,
            retained_new,
            old_identity,
            replacement_identity,
            restored: false,
        })
    }
    fn restore(&mut self) -> TestResult<()> {
        if self.restored {
            return Ok(());
        }
        if identity(&self.path)? != self.replacement_identity
            || identity(&self.old)? != self.old_identity
            || self.retained_new.exists()
        {
            return Err(
                "refuse to overwrite an unexpected owner-file identity during cleanup".into(),
            );
        }
        fs::rename(&self.path, &self.retained_new)?;
        fs::rename(&self.old, &self.path)?;
        self.restored = true;
        assert_eq!(identity(&self.path)?, self.old_identity);
        Ok(())
    }
}
impl Drop for OwnerNameReplacement {
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            eprintln!("NATIVE_PREFIX_OWNER_RESTORE_FAILED {error}");
        }
    }
}
#[test]
fn native_prefix_return_refuses_replaced_local_owner_without_rebinding_old_evidence() {
    run("native-prefix-return-physical-owner", |fixture, report| {
        let peer = fixture.peer();
        let command = fixture.command.clone();
        let root = fixture.source_root();
        let mut transport = fixture.transport();
        let cap = native_value(invoke(&mut transport, &peer, &command)?)?;
        let original = projection(&cap);
        let mut replacement = OwnerNameReplacement::install(root.join("daemon/data-owner.lock"))?;
        report["physical_fault"] = serde_json::json!({"old":replacement.old_identity,
            "replacement":replacement.replacement_identity,"scope":"owned fixture named lock inode only; original FD/file retained"});
        let cursor = fixture.native.rpc_observations().len();
        let stale = cap.validate_binding(&peer, &command, &root);
        let rejected = invoke(&mut transport, &peer, &command);
        report["old_value_current_binding"] =
            serde_json::json!(stale.as_ref().map_err(ToString::to_string));
        report["new_capture_with_foreign_named_owner"] = serde_json::json!(
            rejected
                .as_ref()
                .map(|r| &r.response)
                .map_err(ToString::to_string)
        );
        report["rpc_during_refusal"] = wire(&fixture.native.rpc_observations()[cursor..]);
        // Restore only our exact two files for ordinary close, before assertions.
        // Restoration is cleanup, never a claim of renewed native authority.
        let restored = replacement.restore();
        report["owned_restore"] = serde_json::json!(restored.as_ref().map_err(ToString::to_string));
        restored?;
        assert!(stale.is_err());
        assert!(rejected.is_err());
        assert_eq!(
            fixture.native.rpc_observations().len(),
            cursor,
            "physical-owner mismatch must refuse before sending another RPC"
        );
        assert_eq!(
            projection(&cap),
            original,
            "historical original binding must not be rewritten"
        );
        report["historical_value"] = original;
        drop(transport);
        Ok(vec![cap])
    });
}

mod round;
