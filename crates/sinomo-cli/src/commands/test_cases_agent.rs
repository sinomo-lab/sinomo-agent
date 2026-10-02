//! Agent cluster runtime test cases.
//!
//! These run on a live cluster node (`sinomo test run <id> --require-cluster`)
//! and probe the node's daemon-hosted agent RPC surface without creating a
//! model session. Agent command cancellation is verified through the supported
//! local-workspace simple-sandbox path on the cluster node.

use std::time::{Duration, Instant};

use sinomo_agent::{
    AGENT_CHANNEL_APP_USER_ID, AGENT_OBJECT_RPC_ROUTES, AGENT_SESSION_INSPECT_ROUTE,
    AGENT_SESSION_PROGRESS_ROUTE, AGENT_SESSION_TERMINAL_RESULTS_ROUTE, AgentChannel,
    AgentChannelSource, AgentChannelStore, AgentEvent, AgentEventPayload,
    AgentReadinessCheckStatus, AgentRole, AgentSession, AgentSessionControlEventCursor,
    AgentSessionControlSnapshot, AgentSessionInspectRequest, AgentSessionInspectResponse,
    AgentSessionProgressRequest, AgentSessionProgressResponse, AgentSessionStore,
    AgentSessionStoreRecord, AgentSessionStoreRecordKind, AgentSessionTerminalResultsRequest,
    AgentSessionTerminalResultsResponse, AgentTaskLifecycleStatus, AgentTaskRef, AgentTaskSnapshot,
    FileAgentSessionStore, TaskId, run_agent_simple_sandbox_active_cancellation_readiness,
};
use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::cluster;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::module::{MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY};
use sinomo_runtime::network::{
    ClusterPaths, ObjectRpcCancelRequest, ObjectRpcCancelResponse, ObjectRpcRequest,
    ObjectRpcResponse,
};
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};

use crate::app_api::SifsAgentChannelStore;

const DAEMON_OBJECT_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const AGENT_CHANNEL_EXPECT_DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const AGENT_CHANNEL_EXPECT_MAX_TIMEOUT_SECONDS: u64 = 300;
const AGENT_CHANNEL_EXPECT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const AGENT_CHANNEL_PROBE_DEFAULT_TITLE: &str = "Cluster sync probe";
const AGENT_CHANNEL_PROBE_SOURCE_ADAPTER: &str = "sinomo-cli.cluster-test";
const AGENT_CHANNEL_TEST_RUNS_ROOT: &str = "/services/cluster-tests/runs";

pub(super) fn native_dylib_runtime_capability(
    module_id: &sinomo_runtime::module::ModuleId,
) -> String {
    format!("module.native-dylib:{module_id}")
}

pub fn register_agent_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(agent_runtime_routes())
        .register(agent_runtime_persisted_session_routes())
        .register(agent_channel_write())
        .register(agent_channel_expect())
        .register(module_native_dylib_cancel_route())
        .register(agent_simple_sandbox_active_cancel())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_channel_write(ctx) {
        id: "cluster.agent.channel-write",
        summary: "Append one immutable SIFS-backed channel message with local-node provenance",
        tags: ["cluster-agent", "chat-channel", "multi-device"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_channel_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_channel_expect(ctx) {
        id: "cluster.agent.channel-expect",
        summary: "Wait for exactly one matching immutable SIFS-backed channel message",
        tags: ["cluster-agent", "chat-channel", "multi-device"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_channel_expect(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_runtime_routes(ctx) {
        id: "cluster.agent.runtime-routes",
        summary: "Read-only agent session RPC routes are hosted by the live daemon on this cluster node",
        tags: ["cluster-agent", "agent-readiness"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_runtime_routes(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_runtime_persisted_session_routes(ctx) {
        id: "cluster.agent.runtime-persisted-session-routes",
        summary: "Daemon-hosted agent session RPC routes read persisted session state on this cluster node",
        tags: ["cluster-agent", "agent-readiness", "session-resume"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_runtime_persisted_session_routes(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn module_native_dylib_cancel_route(ctx) {
        id: "cluster.agent.module-native-dylib-cancel-route",
        summary: "Daemon-hosted native-dylib module cancellation route is reachable on this cluster node",
        tags: ["cluster-agent", "agent-readiness", "module-cancellation"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_module_native_dylib_cancel_route(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_simple_sandbox_active_cancel(ctx) {
        id: "cluster.agent.simple-sandbox-active-cancel",
        summary: "Agent local-workspace simple-sandbox execution observes active cancellation",
        tags: ["cluster-agent", "agent-readiness", "sandbox-cancellation"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_simple_sandbox_active_cancel(ctx)
        }
    }
}

fn run_agent_runtime_routes(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let session_id = SessionId::from_legacy_named("agent_cluster_runtime_routes_probe")
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    ctx.record("session_id", &session_id);
    ctx.record("expected_routes", AGENT_OBJECT_RPC_ROUTES);

    let inspect: AgentSessionInspectResponse = probe_agent_session_route(
        AGENT_SESSION_INSPECT_ROUTE,
        &AgentSessionInspectRequest {
            session_id: session_id.clone(),
        },
    )?;
    if inspect.session_id != session_id
        || inspect.session.is_some()
        || inspect.control_snapshot.is_some()
        || !inspect.terminal_results.is_empty()
        || inspect.persisted_event_count != 0
    {
        ctx.record("inspect_response", &inspect);
        return Ok(SinomoTestOutcome::failed(
            "agent inspect route returned unexpected state for synthetic session",
        ));
    }

    let progress: AgentSessionProgressResponse = probe_agent_session_route(
        AGENT_SESSION_PROGRESS_ROUTE,
        &AgentSessionProgressRequest {
            session_id: session_id.clone(),
            cursor: None,
        },
    )?;
    if progress.session_id != session_id
        || !progress.batch.events.is_empty()
        || !progress.batch.task_updates.is_empty()
        || !progress.batch.terminal_results.is_empty()
    {
        ctx.record("progress_response", &progress);
        return Ok(SinomoTestOutcome::failed(
            "agent progress route returned unexpected state for synthetic session",
        ));
    }

    let terminal_results: AgentSessionTerminalResultsResponse = probe_agent_session_route(
        AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
        &AgentSessionTerminalResultsRequest {
            session_id: session_id.clone(),
            task_id: None,
        },
    )?;
    if terminal_results.session_id != session_id || !terminal_results.terminal_results.is_empty() {
        ctx.record("terminal_results_response", &terminal_results);
        return Ok(SinomoTestOutcome::failed(
            "agent terminal-results route returned unexpected state for synthetic session",
        ));
    }

    ctx.record("probed_routes", AGENT_OBJECT_RPC_ROUTES);
    Ok(SinomoTestOutcome::passed(format!(
        "agent read-only session RPC routes are reachable on {}",
        ctx.cluster_config_source()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "cluster runtime".to_string())
    )))
}

fn run_agent_runtime_persisted_session_routes(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let store = FileAgentSessionStore::for_data_dir(&data_dir);
    let session_id = SessionId::generate();
    let task = AgentTaskRef::new("coding", "cluster persisted session route probe")
        .with_owner_role(AgentRole::Main);
    let unrelated_task_id = TaskId::generate();
    let op_id = TaskId::generate();
    let mut task_snapshot =
        AgentTaskSnapshot::new(task.clone(), AgentTaskLifecycleStatus::Completed);
    task_snapshot.progress_summary = Some("persisted session route probe completed".to_string());
    task_snapshot.active_op_id = Some(op_id.clone());
    task_snapshot.result_ref = Some("agent-readiness://persisted-session-route".to_string());
    let event = AgentEvent::new(
        op_id,
        session_id.clone(),
        AgentEventPayload::TaskStatusChanged {
            snapshot: task_snapshot.clone(),
        },
    );
    let control_snapshot = AgentSessionControlSnapshot {
        session_id: session_id.clone(),
        next_sequence: 1,
        active_turn: None,
        tasks: vec![task_snapshot.clone()],
        buffered_event_count: 0,
        paused: false,
    };
    store
        .save_session(&AgentSession::new(session_id.clone(), AgentRole::Main))
        .map_err(|source| {
            SinomoTestError::failed(format!("seed persisted agent session: {source}"))
        })?;
    store
        .append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::ControlEventPublished {
                event: event.clone(),
            },
        ))
        .map_err(|source| {
            SinomoTestError::failed(format!("seed persisted agent control event: {source}"))
        })?;
    store
        .append_record(&AgentSessionStoreRecord::new(
            session_id.clone(),
            AgentSessionStoreRecordKind::ControlSnapshotSaved {
                snapshot: control_snapshot,
            },
        ))
        .map_err(|source| {
            SinomoTestError::failed(format!("seed persisted agent control snapshot: {source}"))
        })?;

    ctx.record("session_id", &session_id);
    ctx.record("task_id", &task.task_id);
    ctx.record("unrelated_task_id", &unrelated_task_id);

    let inspect: AgentSessionInspectResponse = probe_agent_session_route(
        AGENT_SESSION_INSPECT_ROUTE,
        &AgentSessionInspectRequest {
            session_id: session_id.clone(),
        },
    )?;
    if inspect.session_id != session_id
        || inspect.session.is_none()
        || inspect.control_snapshot.is_none()
        || inspect.persisted_event_count != 1
        || inspect.event_cursor.offset != 1
        || inspect.event_cursor.last_event_id.as_ref() != Some(&event.event_id)
        || inspect.terminal_results.len() != 1
        || inspect.terminal_results[0].task.task_id != task.task_id
    {
        ctx.record("inspect_response", &inspect);
        return Ok(SinomoTestOutcome::failed(
            "agent inspect route did not return the seeded persisted session state",
        ));
    }

    let progress: AgentSessionProgressResponse = probe_agent_session_route(
        AGENT_SESSION_PROGRESS_ROUTE,
        &AgentSessionProgressRequest {
            session_id: session_id.clone(),
            cursor: None,
        },
    )?;
    if progress.session_id != session_id
        || progress.batch.events.len() != 1
        || progress.batch.events[0].event_id != event.event_id
        || progress.batch.terminal_results.len() != 1
        || progress.batch.terminal_results[0].task.task_id != task.task_id
    {
        ctx.record("progress_response", &progress);
        return Ok(SinomoTestOutcome::failed(
            "agent progress route did not return the seeded persisted control event",
        ));
    }

    let progress_after_cursor: AgentSessionProgressResponse = probe_agent_session_route(
        AGENT_SESSION_PROGRESS_ROUTE,
        &AgentSessionProgressRequest {
            session_id: session_id.clone(),
            cursor: Some(AgentSessionControlEventCursor {
                offset: 1,
                last_event_id: Some(event.event_id.clone()),
            }),
        },
    )?;
    if progress_after_cursor.session_id != session_id
        || !progress_after_cursor.batch.events.is_empty()
        || !progress_after_cursor.batch.task_updates.is_empty()
        || !progress_after_cursor.batch.terminal_results.is_empty()
        || progress_after_cursor.batch.next_cursor.offset != 1
        || progress_after_cursor
            .batch
            .next_cursor
            .last_event_id
            .as_ref()
            != Some(&event.event_id)
    {
        ctx.record("progress_after_cursor_response", &progress_after_cursor);
        return Ok(SinomoTestOutcome::failed(
            "agent progress route replayed already observed persisted control events",
        ));
    }

    let terminal_results_all: AgentSessionTerminalResultsResponse = probe_agent_session_route(
        AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
        &AgentSessionTerminalResultsRequest {
            session_id: session_id.clone(),
            task_id: None,
        },
    )?;
    if terminal_results_all.session_id != session_id
        || terminal_results_all.terminal_results.len() != 1
        || terminal_results_all.terminal_results[0].task.task_id != task.task_id
    {
        ctx.record("terminal_results_all_response", &terminal_results_all);
        return Ok(SinomoTestOutcome::failed(
            "agent terminal-results route did not return all seeded terminal tasks",
        ));
    }

    let terminal_results: AgentSessionTerminalResultsResponse = probe_agent_session_route(
        AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
        &AgentSessionTerminalResultsRequest {
            session_id: session_id.clone(),
            task_id: Some(task.task_id.clone()),
        },
    )?;
    if terminal_results.session_id != session_id
        || terminal_results.terminal_results.len() != 1
        || terminal_results.terminal_results[0].task.task_id != task.task_id
    {
        ctx.record("terminal_results_response", &terminal_results);
        return Ok(SinomoTestOutcome::failed(
            "agent terminal-results route did not return the seeded terminal task",
        ));
    }

    let unrelated_terminal_results: AgentSessionTerminalResultsResponse =
        probe_agent_session_route(
            AGENT_SESSION_TERMINAL_RESULTS_ROUTE,
            &AgentSessionTerminalResultsRequest {
                session_id: session_id.clone(),
                task_id: Some(unrelated_task_id.clone()),
            },
        )?;
    if unrelated_terminal_results.session_id != session_id
        || !unrelated_terminal_results.terminal_results.is_empty()
    {
        ctx.record(
            "unrelated_terminal_results_response",
            &unrelated_terminal_results,
        );
        return Ok(SinomoTestOutcome::failed(
            "agent terminal-results route ignored the requested task filter",
        ));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "agent session RPC routes read persisted state with cursor and task filters on {}",
        ctx.cluster_config_source()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "cluster runtime".to_string())
    ))
    .with_diagnostics(serde_json::json!({
        "session_id": session_id,
        "task_id": task.task_id,
        "unrelated_task_id": unrelated_task_id,
        "event_id": event.event_id,
        "progress_after_cursor_event_count": progress_after_cursor.batch.events.len(),
        "progress_after_cursor_task_update_count": progress_after_cursor.batch.task_updates.len(),
        "progress_after_cursor_terminal_result_count": progress_after_cursor.batch.terminal_results.len(),
        "progress_after_cursor_next_cursor_last_event_id": progress_after_cursor.batch.next_cursor.last_event_id,
        "terminal_results_all_count": terminal_results_all.terminal_results.len(),
        "terminal_results_filtered_count": terminal_results.terminal_results.len(),
        "unrelated_terminal_result_count": unrelated_terminal_results.terminal_results.len(),
    })))
}

fn run_agent_channel_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let channel_parameter = ctx.required_parameter("channel")?.trim().to_string();
    let channel_id = parse_agent_channel_probe_id(&channel_parameter)?;
    let content = ctx.required_parameter("message")?.to_string();
    let requested_title = optional_agent_channel_title(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    let cluster_state = cluster::read_cluster_state(&cluster_paths).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot read local cluster state for channel provenance: {source}"
        ))
    })?;
    let source_node_id = cluster_state.local_node_id.to_string();
    let test_root = agent_channel_probe_root(&channel_id);
    let (_runtime, store) = open_daemon_agent_channel_store(&test_root)?;
    let (mut channel, channel_created) =
        match store.load_channel(&channel_id).map_err(|source| {
            SinomoTestError::failed(format!("cannot load agent channel {channel_id}: {source}"))
        })? {
            Some(channel) => (channel, false),
            None => (
                AgentChannel::new_channel(
                    channel_id.clone(),
                    requested_title
                        .clone()
                        .unwrap_or_else(|| AGENT_CHANNEL_PROBE_DEFAULT_TITLE.to_string()),
                ),
                true,
            ),
        };
    if channel.is_deleted() {
        return Ok(SinomoTestOutcome::failed(format!(
            "agent channel {channel_id} is deleted and cannot accept a probe message"
        )));
    }

    let source = AgentChannelSource {
        adapter: AGENT_CHANNEL_PROBE_SOURCE_ADAPTER.to_string(),
        external_channel_id: Some(channel_parameter.clone()),
        metadata: serde_json::json!({
            "probe": "cluster.agent.channel-write",
            "source_node_id": source_node_id,
        }),
        ..AgentChannelSource::default()
    };
    let message =
        channel.append_user_message(AGENT_CHANNEL_APP_USER_ID, "Cluster probe", content, source);
    store
        .append_message(&channel_id, &message)
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot append immutable agent channel message {}: {source}",
                message.message_id
            ))
        })?;
    store.save_channel(&channel).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot save agent channel snapshot {channel_id}: {source}"
        ))
    })?;

    ctx.record("channel", &channel_parameter);
    ctx.record("channel_id", channel_id.to_string());
    ctx.record("test_root", &test_root);
    ctx.record("channel_title", &channel.title);
    ctx.record("channel_created", channel_created);
    ctx.record("message_id", message.message_id.to_string());
    ctx.record("message_sequence", message.sequence);
    ctx.record("source_node_id", &source_node_id);
    ctx.record("message_source", &message.source);
    Ok(SinomoTestOutcome::passed(format!(
        "appended channel message {} to {channel_id} from node {source_node_id}",
        message.message_id
    )))
}

fn run_agent_channel_expect(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let channel_parameter = ctx.required_parameter("channel")?.trim().to_string();
    let channel_id = parse_agent_channel_probe_id(&channel_parameter)?;
    let expected = ctx.required_parameter("message")?.to_string();
    let timeout = agent_channel_expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let test_root = agent_channel_probe_root(&channel_id);
    let (_runtime, store) = open_daemon_agent_channel_store(&test_root)?;
    let mut last_state: String;

    ctx.record("channel", &channel_parameter);
    ctx.record("channel_id", channel_id.to_string());
    ctx.record("test_root", &test_root);
    ctx.record("timeout_seconds", timeout.as_secs());
    loop {
        match store.load_recent_messages(&channel_id, usize::MAX) {
            Ok(messages) => {
                let matching = messages
                    .iter()
                    .filter(|message| message.content == expected)
                    .collect::<Vec<_>>();
                let matching_message_ids = matching
                    .iter()
                    .map(|message| message.message_id.to_string())
                    .collect::<Vec<_>>();
                if matching.len() > 1 {
                    ctx.record("matching_message_ids", &matching_message_ids);
                    ctx.record("matching_message_count", matching.len());
                    ctx.record("observed_message_count", messages.len());
                    cleanup_agent_channel_probe(ctx, &store)?;
                    return Ok(SinomoTestOutcome::failed(format!(
                        "agent channel {channel_id} contains {} immutable records matching the expected message; expected exactly one",
                        matching.len()
                    )));
                }
                if let Some(message) = matching.first() {
                    ctx.record("message_id", message.message_id.to_string());
                    ctx.record("matching_message_ids", &matching_message_ids);
                    ctx.record("matching_message_count", 1);
                    ctx.record("message_sequence", message.sequence);
                    ctx.record("message_source", &message.source);
                    ctx.record("observed_message_count", messages.len());
                    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                    cleanup_agent_channel_probe(ctx, &store)?;
                    return Ok(SinomoTestOutcome::passed(format!(
                        "observed exactly one matching immutable channel record {} in {channel_id} after {}ms",
                        message.message_id,
                        started.elapsed().as_millis()
                    )));
                }
                last_state = format!(
                    "observed {} immutable records, none matching the expected message",
                    messages.len()
                );
            }
            Err(source) => {
                last_state = format!("cannot read immutable channel records: {source}");
            }
        }

        let now = Instant::now();
        if now >= deadline {
            ctx.record("matching_message_count", 0);
            ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
            ctx.record("last_state", &last_state);
            cleanup_agent_channel_probe(ctx, &store)?;
            return Ok(SinomoTestOutcome::failed(format!(
                "agent channel {channel_id} did not converge to exactly one matching immutable record within {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(AGENT_CHANNEL_EXPECT_POLL_INTERVAL.min(deadline.duration_since(now)));
    }
}

fn cleanup_agent_channel_probe(
    ctx: &mut SinomoTestContext,
    store: &SifsAgentChannelStore,
) -> Result<(), SinomoTestError> {
    store.delete_isolated_test_run().map_err(|detail| {
        SinomoTestError::failed(format!(
            "cannot remove isolated agent channel test run after expectation: {detail}"
        ))
    })?;
    ctx.record("test_root_removed", true);
    Ok(())
}

fn open_daemon_agent_channel_store(
    test_root: &str,
) -> Result<(tokio::runtime::Runtime, SifsAgentChannelStore), SinomoTestError> {
    let daemon_client = DaemonApiClient::connect().map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot connect to the local daemon for agent channel storage: {source}"
        ))
    })?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|source| {
            SinomoTestError::failed(format!("cannot create agent channel RPC runtime: {source}"))
        })?;
    let store = SifsAgentChannelStore::open_daemon_on_runtime_at_root(
        daemon_client.address().to_string(),
        runtime.handle(),
        test_root,
    )
    .map_err(|detail| {
        SinomoTestError::failed(format!(
            "cannot open daemon-backed SIFS agent channel store: {detail}"
        ))
    })?;
    Ok((runtime, store))
}

fn agent_channel_probe_root(channel_id: &SessionId) -> String {
    format!("{AGENT_CHANNEL_TEST_RUNS_ROOT}/{channel_id}/agent-channels")
}

fn parse_agent_channel_probe_id(raw: &str) -> Result<SessionId, SinomoTestError> {
    let raw = raw.trim();
    raw.parse::<SessionId>()
        .or_else(|_| SessionId::from_legacy_named(raw))
        .map_err(|source| SinomoTestError::InvalidParameter {
            name: "channel".to_string(),
            detail: format!("must be a lowercase ULID or a valid stable channel label: {source}"),
        })
}

fn optional_agent_channel_title(
    ctx: &SinomoTestContext,
) -> Result<Option<String>, SinomoTestError> {
    let Some(raw) = ctx.parameter("title") else {
        return Ok(None);
    };
    let title = raw.trim();
    if title.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "title".to_string(),
            detail: "must not be empty when supplied".to_string(),
        });
    }
    Ok(Some(title.to_string()))
}

fn agent_channel_expect_timeout(ctx: &SinomoTestContext) -> Result<Duration, SinomoTestError> {
    let configured = match (ctx.parameter("timeout-seconds"), ctx.parameter("timeout")) {
        (Some(_), Some(_)) => {
            return Err(SinomoTestError::InvalidParameter {
                name: "timeout".to_string(),
                detail: "supply only one of timeout or timeout-seconds".to_string(),
            });
        }
        (Some(raw), None) => Some(("timeout-seconds", raw)),
        (None, Some(raw)) => Some(("timeout", raw)),
        (None, None) => None,
    };
    let (key, seconds) = match configured {
        Some((key, raw)) => (
            key,
            raw.trim()
                .parse::<u64>()
                .map_err(|source| SinomoTestError::InvalidParameter {
                    name: key.to_string(),
                    detail: format!("must be an integer number of seconds: {source}"),
                })?,
        ),
        None => (
            "timeout-seconds",
            AGENT_CHANNEL_EXPECT_DEFAULT_TIMEOUT_SECONDS,
        ),
    };
    if !(1..=AGENT_CHANNEL_EXPECT_MAX_TIMEOUT_SECONDS).contains(&seconds) {
        return Err(SinomoTestError::InvalidParameter {
            name: key.to_string(),
            detail: format!(
                "must be between 1 and {AGENT_CHANNEL_EXPECT_MAX_TIMEOUT_SECONDS} seconds"
            ),
        });
    }
    Ok(Duration::from_secs(seconds))
}

fn run_module_native_dylib_cancel_route(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let operation_id = "agent_cluster_module_native_dylib_cancel_route_probe";
    ctx.record("operation_id", operation_id);
    let request = ObjectRpcRequest::encode_body(
        MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
        MODULE_RPC_CATEGORY,
        &ObjectRpcCancelRequest::new(operation_id, "cluster readiness route probe"),
    )
    .map_err(|source| SinomoTestError::failed(source.to_string()))?
    .with_cancels_operation_id(operation_id);
    let response: ObjectRpcCancelResponse =
        probe_daemon_object_rpc(&request).map_err(|source| {
            SinomoTestError::failed(format!(
                "native dylib module cancel RPC route failed: {source}"
            ))
        })?;
    ctx.record("cancel_response", &response);

    if response.operation_id != operation_id {
        return Ok(SinomoTestOutcome::failed(
            "native dylib module cancel route returned the wrong operation id",
        ));
    }
    if response.cancelled {
        return Ok(SinomoTestOutcome::failed(
            "synthetic native dylib module cancel probe unexpectedly cancelled active work",
        ));
    }
    if !response
        .detail
        .contains("native dylib module operation is not active")
    {
        return Ok(SinomoTestOutcome::failed(
            "native dylib module cancel route returned an unexpected inactive-operation detail",
        ));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "native dylib module cancel route is reachable on {}",
        ctx.cluster_config_source()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "cluster runtime".to_string())
    )))
}

fn run_agent_simple_sandbox_active_cancel(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let check = run_agent_simple_sandbox_active_cancellation_readiness();
    ctx.record("agent_readiness_check", &check);
    Ok(match check.status {
        AgentReadinessCheckStatus::Passed => {
            SinomoTestOutcome::passed(check.summary).with_diagnostics(check.evidence)
        }
        AgentReadinessCheckStatus::Failed => {
            SinomoTestOutcome::failed(check.summary).with_diagnostics(check.evidence)
        }
    })
}

fn probe_agent_session_route<Req, Resp>(route: &str, body: &Req) -> Result<Resp, SinomoTestError>
where
    Req: serde::Serialize + ?Sized,
    Resp: serde::de::DeserializeOwned,
{
    crate::send_agent_session_rpc(route, body).map_err(|source| {
        SinomoTestError::failed(format!("agent session RPC route {route} failed: {source}"))
    })
}

fn probe_daemon_object_rpc<Resp>(request: &ObjectRpcRequest) -> Result<Resp, crate::CliError>
where
    Resp: serde::de::DeserializeOwned,
{
    probe_daemon_object_rpc_with_timeout(request, DAEMON_OBJECT_RPC_TIMEOUT)
}

fn probe_daemon_object_rpc_with_timeout<Resp>(
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<Resp, crate::CliError>
where
    Resp: serde::de::DeserializeOwned,
{
    let response = send_daemon_object_rpc_with_timeout(request, timeout)?;
    response.decode_body().map_err(crate::CliError::from)
}

fn send_daemon_object_rpc_with_timeout(
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> Result<ObjectRpcResponse, crate::CliError> {
    let Some(status) = crate::default_main_daemon_status()? else {
        return Err(crate::CliError::DaemonNotRunning {
            detail: "cluster runtime tests require the local daemon RPC service".to_string(),
        });
    };
    crate::daemon_send_daemon_rpc(&status.address, request, timeout).map_err(crate::CliError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_channel_probe_id_accepts_ulids_and_stable_labels() {
        let generated = SessionId::generate();
        assert_eq!(
            parse_agent_channel_probe_id(generated.as_str()).expect("generated channel id"),
            generated
        );

        let label = "cluster-channel-sync-probe";
        assert_eq!(
            parse_agent_channel_probe_id(label).expect("stable channel label"),
            SessionId::from_legacy_named(label).expect("derived channel id")
        );
        assert!(parse_agent_channel_probe_id("invalid channel label").is_err());
    }

    #[test]
    fn agent_channel_probe_root_is_isolated_from_product_channels() {
        let channel_id = SessionId::generate();
        let root = agent_channel_probe_root(&channel_id);

        assert_eq!(
            root,
            format!("/services/cluster-tests/runs/{channel_id}/agent-channels")
        );
        assert!(!root.starts_with("/services/sinomo-agent/channels"));
    }

    #[test]
    fn agent_channel_expect_timeout_is_defaulted_and_bounded() {
        let default = SinomoTestContext::new("test-data");
        assert_eq!(
            agent_channel_expect_timeout(&default).expect("default timeout"),
            Duration::from_secs(AGENT_CHANNEL_EXPECT_DEFAULT_TIMEOUT_SECONDS)
        );

        let configured = SinomoTestContext::new("test-data").with_parameter("timeout", "45");
        assert_eq!(
            agent_channel_expect_timeout(&configured).expect("configured timeout"),
            Duration::from_secs(45)
        );

        let zero = SinomoTestContext::new("test-data").with_parameter("timeout-seconds", "0");
        assert!(matches!(
            agent_channel_expect_timeout(&zero),
            Err(SinomoTestError::InvalidParameter { ref name, .. })
                if name == "timeout-seconds"
        ));

        let excessive = SinomoTestContext::new("test-data").with_parameter(
            "timeout-seconds",
            (AGENT_CHANNEL_EXPECT_MAX_TIMEOUT_SECONDS + 1).to_string(),
        );
        assert!(agent_channel_expect_timeout(&excessive).is_err());

        let ambiguous = SinomoTestContext::new("test-data")
            .with_parameter("timeout", "10")
            .with_parameter("timeout-seconds", "10");
        assert!(agent_channel_expect_timeout(&ambiguous).is_err());
    }
}
