//! A caller-only reply-discard probe around supported routed History RPCs.

use super::*;
use sinomo_runtime::api::{DaemonApiError, DaemonObjectRpcDestinationProcessAffinity};
use sinomo_runtime::dfs::{
    DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE, DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE, DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE,
    DfsFileHistoryListRequest, DfsFileHistoryListResponse, DfsFileHistoryPinOperation,
    DfsFileHistoryPinRequest, DfsFileHistoryPinResponse, DfsFileHistoryReadOutcome,
    DfsFileHistoryReadRequest, DfsFileHistoryRestoreRequest, DfsFileHistoryRestoreResponse,
    DfsFileVersionIdentity,
};
use sinomo_runtime::network::NetworkDeliveryState;

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_semantic_lost_reply(ctx) {
        id: "cluster.dfs.semantic-lost-reply",
        summary: "Verify remote capture, conditional restore and release retries after successful replies are discarded by the test caller",
        tags: ["cluster-dfs", "dfs-file-history"],
        requirements: [SinomoTestRequirement::ClusterRuntime, SinomoTestRequirement::ClusterKvSnapshot],
        run: { run_semantic_lost_reply(ctx) }
    }
}

struct RemoteHistoryCaller {
    address: String,
    primary: NodeId,
    deadline: Instant,
}

impl RemoteHistoryCaller {
    fn rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        route: &str,
        body: &Req,
    ) -> Result<Resp, SinomoTestError> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| {
                SinomoTestError::failed("remote semantic retry probe deadline elapsed")
            })?;
        let request =
            ObjectRpcRequest::encode_body(route, sinomo_runtime::daemon::DAEMON_RPC_CATEGORY, body)
                .map_err(|error| SinomoTestError::failed(error.to_string()))?;
        // Construct a new caller for every RPC. The replay must depend on the
        // remote durable receipt, rather than state in the original client.
        let response = DaemonApiClient::at_address(&self.address)
            .with_timeout(remaining)
            .object_rpc_to_target_with_deadline_and_affinity(
                &self.primary,
                &request,
                remaining.min(Duration::from_secs(10)),
                Some(remaining),
                true,
                DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            )
            .map_err(|error| {
                SinomoTestError::failed(format!(
                    "remote History RPC {route} to {} failed: {error}",
                    self.primary
                ))
            })?;
        if response
            .responder_node_id()
            .is_some_and(|responder| responder != self.primary.to_string())
        {
            return Err(SinomoTestError::failed(
                "remote History RPC identified an unexpected responder",
            ));
        }
        response
            .decode_body()
            .map_err(|error| SinomoTestError::failed(error.to_string()))
    }

    fn discard_reply<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        route: &str,
        body: &Req,
    ) -> Result<DiscardedReply<Resp>, SinomoTestError> {
        let audit_response = self.rpc(route, body)?;
        // Only this maintained test caller introduces ambiguity, after a real
        // authenticated remote RPC completed and its response decoded. No
        // server fault switch or production transport behavior is changed.
        Ok(DiscardedReply {
            audit_response,
            caller_result: Err(DaemonApiError::NetworkDelivery {
                detail: "test caller discarded a successful remote History reply".into(),
                delivery_state: NetworkDeliveryState::PossiblyApplied,
            }),
        })
    }
}

struct DiscardedReply<T> {
    // The verifier keeps this for equality evidence. The simulated caller
    // receives only caller_result and cannot use the successful outcome.
    audit_response: T,
    caller_result: Result<(), DaemonApiError>,
}

impl<T> DiscardedReply<T> {
    fn require_ambiguous(&self) -> Result<(), SinomoTestError> {
        if matches!(
            &self.caller_result,
            Err(error) if error.delivery_state() == Some(NetworkDeliveryState::PossiblyApplied)
        ) {
            Ok(())
        } else {
            Err(SinomoTestError::failed(
                "discarded successful reply was not presented as PossiblyApplied",
            ))
        }
    }
}

struct ScopedHistoryPin<'a> {
    caller: &'a RemoteHistoryCaller,
    release: DfsFileHistoryPinRequest,
    released: bool,
}

impl Drop for ScopedHistoryPin<'_> {
    fn drop(&mut self) {
        if !self.released {
            // Release before the outer namespace guard removes the subtree.
            // Cleanup remains bounded even when the main probe timed out.
            let cleanup = RemoteHistoryCaller {
                address: self.caller.address.clone(),
                primary: self.caller.primary.clone(),
                deadline: Instant::now() + Duration::from_secs(10),
            };
            let _ = cleanup.rpc::<_, DfsFileHistoryPinResponse>(
                DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
                &self.release,
            );
        }
    }
}

fn current_identity(
    browser: &SifsBrowser,
    path: &str,
) -> Result<DfsFileVersionIdentity, SinomoTestError> {
    let stat = browser.stat(path).map_err(SinomoTestError::failed)?;
    Ok(DfsFileVersionIdentity {
        file_system_id: DfsFileSystemId::new(&stat.file_system_id)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?,
        object_id: DfsObjectId::new(&stat.object_id)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?,
        version: DfsVersion::new(&stat.version)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?,
    })
}

fn require_current(
    browser: &SifsBrowser,
    path: &str,
    identity: &DfsFileVersionIdentity,
    bytes: &[u8],
) -> Result<(), SinomoTestError> {
    if current_identity(browser, path)? != *identity
        || browser.read_file(path).map_err(SinomoTestError::failed)? != bytes
    {
        return Err(SinomoTestError::failed(format!(
            "an exact semantic retry changed newer current content at {path}"
        )));
    }
    Ok(())
}

fn source_entries(
    caller: &RemoteHistoryCaller,
    path: &str,
    identity: &DfsFileVersionIdentity,
) -> Result<DfsFileHistoryListResponse, SinomoTestError> {
    let mut entries = Vec::new();
    let mut after = None;
    let mut seen = Vec::new();
    // A cursor can advance authorities even when the previous page was empty.
    // Bound both that traversal and this isolated fixture's total entries.
    for _ in 0..16 {
        let page: DfsFileHistoryListResponse = caller.rpc(
            DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE,
            &DfsFileHistoryListRequest {
                file_system_id: identity.file_system_id.clone(),
                path: Some(path.into()),
                object_id: Some(identity.object_id.clone()),
                after: after.clone(),
                limit: 16,
            },
        )?;
        entries.extend(page.entries);
        if entries.len() > 16 {
            return Err(SinomoTestError::failed(
                "isolated semantic retry fixture unexpectedly exceeded its bounded history entries",
            ));
        }
        let Some(next) = page.next_cursor else {
            return Ok(DfsFileHistoryListResponse {
                entries,
                next_cursor: None,
            });
        };
        if seen.contains(&next) {
            return Err(SinomoTestError::failed(
                "semantic retry history authority returned a repeated cursor",
            ));
        }
        seen.push(next.clone());
        after = Some(next);
    }
    Err(SinomoTestError::failed(
        "semantic retry history authority traversal exceeded its bounded page budget",
    ))
}

fn run_semantic_lost_reply(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let started = Instant::now();
    let deadline = started + expect_timeout(ctx)?;
    let authority = ctx.authoritative_cluster_snapshot().ok_or_else(|| {
        SinomoTestError::failed("remote semantic retry requires an authoritative cluster snapshot")
    })?;
    // Validate any explicit base before probing it; otherwise choose a guarded
    // lane whose primary is remote from this invocation's node.
    let bases = if let Some(base) = ctx.parameter("base") {
        scoped_dfs_test_base(ctx, "semantic-lost-reply")?;
        vec![base.trim_end_matches('/')]
    } else {
        vec![
            "/services/cluster-tests",
            "/users/cluster-tests",
            "/cluster-tests",
        ]
    };
    let observation = open_observation_with_deadline(ctx, deadline)?;
    let local = NodeId::new(observation.node_label())
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let mut selected = None;
    for base in bases {
        let root = format!("{base}/runs/{}/semantic-lost-reply", ctx.run_id());
        validate_scoped_sifs_test_namespace(&root)?;
        let shard = observation
            .metadata_shard_for_path_at_authority(&format!("{root}/source.txt"), &authority)
            .map_err(SinomoTestError::failed)?
            .ok_or_else(|| {
                SinomoTestError::failed("semantic retry fixture has no metadata authority")
            })?;
        if shard.primary_node_id != local {
            selected = Some((root, shard));
            break;
        }
    }
    let (base, shard) = selected.ok_or_else(|| {
        SinomoTestError::failed(
            "semantic retry probe requires a non-local metadata primary; no guarded lane qualifies",
        )
    })?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [base.clone()])?;
    let browser = open_local_daemon_sifs_browser(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let status = sinomo_runtime::daemon::read_daemon_status(
        &sinomo_runtime::daemon::DaemonPaths::new(&data_dir),
    )
    .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let caller = RemoteHistoryCaller {
        address: status.address,
        primary: shard.primary_node_id.clone(),
        deadline,
    };
    ctx.record(
        "fault_model",
        "client-side successful remote reply discarded",
    );
    ctx.record("local_node_id", local.to_string());
    ctx.record("target_primary_node_id", shard.primary_node_id.to_string());
    ctx.record("metadata_shard_id", shard.shard_id.to_string());
    ctx.record("metadata_shard_epoch", shard.epoch);
    ctx.record("base", &base);

    let source = format!("{base}/source.txt");
    let destination = format!("{base}/destination.txt");
    let pin_id = "semantic-reply-discard".to_string();
    browser
        .write_file(&source, b"retained-original")
        .map_err(SinomoTestError::failed)?;
    let identity = current_identity(&browser, &source)?;
    let capture = DfsFileHistoryPinRequest {
        path: source.clone(),
        identity: identity.clone(),
        operation: DfsFileHistoryPinOperation::Capture {
            pin_id: pin_id.clone(),
        },
        operation_id: sinomo_runtime::core::Id::generate().to_string(),
    };
    let mut pin_guard = ScopedHistoryPin {
        caller: &caller,
        release: DfsFileHistoryPinRequest {
            path: source.clone(),
            identity: identity.clone(),
            operation: DfsFileHistoryPinOperation::Unpin {
                pin_id: pin_id.clone(),
            },
            operation_id: sinomo_runtime::core::Id::generate().to_string(),
        },
        released: false,
    };
    let captured: DiscardedReply<DfsFileHistoryPinResponse> =
        caller.discard_reply(DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE, &capture)?;
    captured.require_ambiguous()?;
    browser
        .write_file(&source, b"newer-source-head")
        .map_err(SinomoTestError::failed)?;
    let newer_source = current_identity(&browser, &source)?;
    let capture_retry: DfsFileHistoryPinResponse =
        caller.rpc(DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE, &capture)?;
    if capture_retry != captured.audit_response || capture_retry.identity != identity {
        return Err(SinomoTestError::failed(
            "capture retry changed its durable outcome",
        ));
    }
    require_current(&browser, &source, &newer_source, b"newer-source-head")?;
    let page = source_entries(&caller, &source, &identity)?;
    let matching = page
        .entries
        .iter()
        .filter(|entry| entry.identity == identity)
        .collect::<Vec<_>>();
    if matching.len() != 1
        || matching[0]
            .pins
            .iter()
            .filter(|pin| **pin == pin_id)
            .count()
            != 1
    {
        return Err(SinomoTestError::failed(
            "capture retry duplicated or lost its retained revision/reference",
        ));
    }
    let read: DfsFileHistoryReadOutcome = caller.rpc(
        DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
        &DfsFileHistoryReadRequest {
            identity: identity.clone(),
            offset: 0,
            max_bytes: 64,
        },
    )?;
    if !matches!(read, DfsFileHistoryReadOutcome::Found { data } if data.bytes == b"retained-original")
    {
        return Err(SinomoTestError::failed(
            "captured remote content differs after the intervening edit",
        ));
    }

    browser
        .write_file(&destination, b"conditional-destination")
        .map_err(SinomoTestError::failed)?;
    let restore = DfsFileHistoryRestoreRequest {
        source: identity.clone(),
        destination_path: destination.clone(),
        expected_destination: Some(current_identity(&browser, &destination)?),
        operation_id: sinomo_runtime::core::Id::generate().to_string(),
    };
    let restored: DiscardedReply<DfsFileHistoryRestoreResponse> =
        caller.discard_reply(DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE, &restore)?;
    restored.require_ambiguous()?;
    require_current(
        &browser,
        &destination,
        &restored.audit_response.committed,
        b"retained-original",
    )?;
    browser
        .write_file(&destination, b"newer-destination-head")
        .map_err(SinomoTestError::failed)?;
    let newer_destination = current_identity(&browser, &destination)?;
    let restore_retry: DfsFileHistoryRestoreResponse =
        caller.rpc(DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE, &restore)?;
    if restore_retry != restored.audit_response {
        return Err(SinomoTestError::failed(
            "conditional restore retry changed its durable outcome",
        ));
    }
    require_current(
        &browser,
        &destination,
        &newer_destination,
        b"newer-destination-head",
    )?;

    let released: DiscardedReply<DfsFileHistoryPinResponse> = caller.discard_reply(
        DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
        &pin_guard.release,
    )?;
    released.require_ambiguous()?;
    browser
        .delete(&source, false)
        .map_err(SinomoTestError::failed)?;
    browser
        .write_file(&source, b"path-reused-new-object")
        .map_err(SinomoTestError::failed)?;
    let reused = current_identity(&browser, &source)?;
    if reused.object_id == identity.object_id {
        return Err(SinomoTestError::failed(
            "unlink/path reuse did not create a new object",
        ));
    }
    let release_retry: DfsFileHistoryPinResponse = caller.rpc(
        DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
        &pin_guard.release,
    )?;
    if release_retry != released.audit_response {
        return Err(SinomoTestError::failed(
            "release retry changed its durable outcome",
        ));
    }
    require_current(&browser, &source, &reused, b"path-reused-new-object")?;
    if source_entries(&caller, &source, &identity)?
        .entries
        .iter()
        .any(|entry| entry.pins.contains(&pin_id))
    {
        return Err(SinomoTestError::failed(
            "release retry resurrected the old reference",
        ));
    }
    pin_guard.released = true;
    ctx.record("discarded_reply_delivery_state", "PossiblyApplied");
    ctx.record("capture_operation_id", &capture.operation_id);
    ctx.record("capture_identity", &identity);
    ctx.record("source_after_capture_retry", &newer_source);
    ctx.record("restore_operation_id", &restore.operation_id);
    ctx.record("restore_original_response", &restored.audit_response);
    ctx.record("destination_after_restore_retry", &newer_destination);
    ctx.record("release_operation_id", &pin_guard.release.operation_id);
    ctx.record("source_after_release_retry", &reused);
    ctx.record("discarded_successful_replies", 3);
    ctx.record("capture_single_reference", true);
    ctx.record("restore_preserved_newer_head", true);
    ctx.record("release_preserved_reused_path", true);
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
    Ok(SinomoTestOutcome::passed(
        "remote capture, conditional restore and release replayed their original receipts after successful replies were discarded, without rewinding newer heads or resurrecting references",
    ))
}
