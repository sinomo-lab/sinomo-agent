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

use crate::app_api::{AgentChatSifsStorage, SifsAgentChannelStore};

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
        .register(agent_channel_cleanup())
        .register(agent_channel_provider_reopen())
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
    pub fn agent_channel_cleanup(ctx) {
        id: "cluster.agent.channel-cleanup",
        summary: "Remove an explicitly selected isolated SIFS channel probe namespace",
        tags: ["cluster-agent", "chat-channel", "multi-device"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_channel_cleanup(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_channel_provider_reopen(ctx) {
        id: "cluster.agent.channel-provider-reopen",
        summary: "Reopen one chat storage instance after its selected SIFS provider is withdrawn",
        tags: ["cluster-agent", "chat-channel", "multi-device", "provider-recovery"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_agent_channel_provider_reopen(ctx)
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
    let cleanup = agent_channel_cleanup_on_success(ctx)?;
    let expected_title = optional_agent_channel_title(ctx)?;
    let expected_count = optional_agent_channel_u64(ctx, "message-count")?;
    let expected_sequence = optional_agent_channel_u64(ctx, "message-sequence")?;
    let expected_message_id = ctx.parameter("message-id").map(str::to_string);
    let expected_source_node = ctx.parameter("source-node-id").map(str::to_string);
    let started = Instant::now();
    let deadline = started + timeout;
    let test_root = agent_channel_probe_root(&channel_id);
    let (_runtime, store) = open_daemon_agent_channel_store(&test_root)?;
    let mut last_state: String;

    ctx.record("channel", &channel_parameter);
    ctx.record("channel_id", channel_id.to_string());
    ctx.record("test_root", &test_root);
    ctx.record("timeout_seconds", timeout.as_secs());
    ctx.record("cleanup_on_success", cleanup);
    // Failed expectations retain their original records for inspection and a
    // later exact-ID check. A successful reader can opt out of cleanup so the
    // same channel can be verified on several nodes and after daemon restart.
    ctx.record("test_root_removed", false);
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
                ctx.record("matching_message_count", matching.len());
                if matching.len() > 1 {
                    ctx.record("matching_message_ids", &matching_message_ids);
                    ctx.record("matching_message_count", matching.len());
                    ctx.record("observed_message_count", messages.len());
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
                    let metadata = store
                        .load_channel(&channel_id)
                        .map_err(|source| format!("cannot load channel metadata: {source}"));
                    let validation = match metadata {
                        Ok(Some(channel)) => {
                            ctx.record("channel_title", &channel.title);
                            ctx.record("channel_next_sequence", channel.next_sequence);
                            if channel.is_deleted() {
                                Err("channel metadata is deleted".to_string())
                            } else if expected_title
                                .as_ref()
                                .is_some_and(|title| title != &channel.title)
                            {
                                Err(format!(
                                    "channel title {:?} differs from {:?}",
                                    channel.title, expected_title
                                ))
                            } else if expected_count
                                .is_some_and(|count| count != messages.len() as u64)
                            {
                                Err(format!(
                                    "observed {} messages instead of {:?}",
                                    messages.len(),
                                    expected_count
                                ))
                            } else if expected_sequence
                                .is_some_and(|sequence| sequence != message.sequence)
                            {
                                Err(format!(
                                    "message sequence {} differs from {:?}",
                                    message.sequence, expected_sequence
                                ))
                            } else if expected_message_id
                                .as_ref()
                                .is_some_and(|id| id != message.message_id.as_str())
                            {
                                Err(format!(
                                    "matching content belongs to message {} instead of {:?}",
                                    message.message_id, expected_message_id
                                ))
                            } else if expected_source_node.as_ref().is_some_and(|node| {
                                message
                                    .source
                                    .metadata
                                    .get("source_node_id")
                                    .and_then(serde_json::Value::as_str)
                                    != Some(node.as_str())
                            }) {
                                Err(format!(
                                    "message provenance {:?} differs from {:?}",
                                    message.source, expected_source_node
                                ))
                            } else if messages
                                .iter()
                                .enumerate()
                                .any(|(index, item)| item.sequence != index as u64 + 1)
                            {
                                Err("channel message sequence is not contiguous".to_string())
                            } else if channel.next_sequence < messages.len() as u64 + 1 {
                                Err("channel metadata sequence trails its immutable messages"
                                    .to_string())
                            } else {
                                Ok(())
                            }
                        }
                        Ok(None) => Err("channel metadata is absent".to_string()),
                        Err(detail) => Err(detail),
                    };
                    match validation {
                        Ok(()) => {
                            ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                            if cleanup {
                                cleanup_agent_channel_probe(ctx, &store)?;
                            }
                            return Ok(SinomoTestOutcome::passed(format!(
                                "observed exactly one matching immutable channel record {} and channel metadata in {channel_id} after {}ms",
                                message.message_id,
                                started.elapsed().as_millis()
                            )));
                        }
                        Err(detail) => last_state = detail,
                    }
                } else {
                    last_state = format!(
                        "observed {} immutable records, none matching the expected message",
                        messages.len()
                    );
                }
            }
            Err(source) => {
                last_state = format!("cannot read immutable channel records: {source}");
            }
        }

        let now = Instant::now();
        if now >= deadline {
            ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
            ctx.record("last_state", &last_state);
            return Ok(SinomoTestOutcome::failed(format!(
                "agent channel {channel_id} did not converge to exactly one matching immutable record within {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(AGENT_CHANNEL_EXPECT_POLL_INTERVAL.min(deadline.duration_since(now)));
    }
}

fn run_agent_channel_cleanup(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let channel = ctx.required_parameter("channel")?.trim().to_string();
    let channel_id = parse_agent_channel_probe_id(&channel)?;
    let test_root = agent_channel_probe_root(&channel_id);
    let (_runtime, store) = open_daemon_agent_channel_store(&test_root)?;
    ctx.record("channel", channel);
    ctx.record("test_root", test_root);
    cleanup_agent_channel_probe(ctx, &store)?;
    Ok(SinomoTestOutcome::passed(format!(
        "removed isolated channel probe namespace for {channel_id}"
    )))
}

fn run_agent_channel_provider_reopen(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let control_id = ctx.required_parameter("control-id")?.to_string();
    if control_id.is_empty()
        || control_id.len() > 128
        || !control_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(SinomoTestError::InvalidParameter {
            name: "control-id".to_string(),
            detail:
                "must be a new lowercase alphanumeric/hyphen fixture identity of at most 128 bytes"
                    .to_string(),
        });
    }
    let expected_original = ctx.required_parameter("original-provider")?.to_string();
    expected_original
        .parse::<sinomo_runtime::node::NodeId>()
        .map_err(|source| SinomoTestError::InvalidParameter {
            name: "original-provider".to_string(),
            detail: source.to_string(),
        })?;
    let timeout = agent_channel_expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    // This is an explicit native Docker-controller handshake, never SIFS
    // product state or distributed effect authority. A fresh fixture prevents
    // a prior run's release from satisfying this observation.
    let base = std::path::Path::new("/shared/consumer-tests");
    std::fs::create_dir_all(base).map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let control = base.join(&control_id);
    std::fs::create_dir(&control).map_err(|source| {
        SinomoTestError::failed(format!(
            "provider probe requires a fresh control directory {}: {source}",
            control.display()
        ))
    })?;
    let daemon =
        DaemonApiClient::connect().map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let storage = AgentChatSifsStorage::new(
        SinomoDataDir::new(ctx.data_dir()),
        daemon.address(),
        runtime.handle(),
    );
    let clone = storage.clone();
    let initial = storage
        .open_channel_store()
        .map_err(SinomoTestError::failed)?;
    let original = initial.provider_node_id().to_string();
    if original != expected_original {
        return Ok(SinomoTestOutcome::failed(format!(
            "provider probe opened {original}, expected original {expected_original}"
        )));
    }
    drop(initial);
    let ready = serde_json::json!({
        "control_id": control_id,
        "original_provider": original,
        "phase": "initial-provider-opened",
    });
    std::fs::write(
        control.join("ready.pending.json"),
        serde_json::to_vec_pretty(&ready).unwrap(),
    )
    .and_then(|_| {
        std::fs::rename(
            control.join("ready.pending.json"),
            control.join("ready.json"),
        )
    })
    .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    ctx.record("control_root", control.display().to_string());
    ctx.record("original_provider", &original);
    let release_path = control.join("release.json");
    let release = loop {
        if Instant::now() >= deadline {
            return Ok(SinomoTestOutcome::failed(
                "provider controller did not release the same storage instance before its deadline; fixture retained",
            ));
        }
        match std::fs::read(&release_path) {
            Ok(bytes) => {
                break serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|source| {
                    SinomoTestError::failed(format!("invalid controller release: {source}"))
                })?;
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(SinomoTestError::failed(source.to_string())),
        }
        std::thread::sleep(
            Duration::from_millis(25).min(deadline.saturating_duration_since(Instant::now())),
        );
    };
    if release
        .get("control_id")
        .and_then(serde_json::Value::as_str)
        != Some(control_id.as_str())
        || release
            .get("original_provider")
            .and_then(serde_json::Value::as_str)
            != Some(original.as_str())
        || release.get("phase").and_then(serde_json::Value::as_str) != Some("provider-withdrawn")
    {
        return Ok(SinomoTestOutcome::failed(
            "controller release does not match this exact provider fixture; data retained",
        ));
    }
    let request = ObjectRpcRequest::encode_body(
        sinomo_runtime::kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
        sinomo_runtime::kv::CLUSTER_KV_RPC_CATEGORY,
        &sinomo_runtime::service::CLUSTER_SERVICES_KV_PREFIX.to_string(),
    )
    .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let (read, providers) = loop {
        // The controller may have restarted Main while keeping this CLI and
        // its storage clone alive. Read-only catalog propagation and local
        // control reconnection retain the original fixture deadline.
        let observation: Result<_, String> = (|| {
            let response = daemon
                .call_request(&request)
                .map_err(|source| source.to_string())?;
            let read: sinomo_runtime::kv::KvLocalRead<Vec<sinomo_runtime::kv::KvEntry>> = response
                .decode_body()
                .map_err(|source| source.to_string())?;
            let instances = sinomo_runtime::service::service_instances_from_entries(&read.value)
                .map_err(|source| source.to_string())?;
            let providers = sinomo_runtime::service::service_instances_providing_capability(
                instances,
                sinomo_runtime::dfs::SIFS_PATH_ACCESS_CAPABILITY,
            );
            Ok((read, providers))
        })();
        let last_state = match observation {
            Ok((read, providers))
                if !providers
                    .iter()
                    .any(|provider| provider.node_id.as_str() == original.as_str()) =>
            {
                break (read, providers);
            }
            Ok((read, _)) => format!(
                "original provider remains advertised at catalog revision {}",
                read.revision.0
            ),
            Err(detail) => detail,
        };
        ctx.record("last_withdrawal_state", &last_state);
        if Instant::now() >= deadline {
            return Ok(SinomoTestOutcome::failed(format!(
                "actual provider withdrawal was not observed within the original deadline: {last_state}"
            )));
        }
        std::thread::sleep(
            Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
        );
    };
    ctx.record("catalog_revision_after_withdrawal", read.revision.0);
    ctx.record(
        "providers_after_withdrawal",
        providers
            .iter()
            .map(|provider| provider.node_id.to_string())
            .collect::<Vec<_>>(),
    );
    let reopened = clone
        .open_channel_store()
        .map_err(SinomoTestError::failed)?;
    let replacement = reopened.provider_node_id().to_string();
    ctx.record("replacement_provider", &replacement);
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
    if replacement == original || Instant::now() > deadline {
        return Ok(SinomoTestOutcome::failed(
            "reopened storage did not select a different live provider within its original deadline",
        ));
    }
    Ok(SinomoTestOutcome::passed(format!(
        "same chat storage instance reopened from withdrawn provider {original} on live provider {replacement}; no mutation was replayed"
    )))
}

fn agent_channel_cleanup_on_success(ctx: &SinomoTestContext) -> Result<bool, SinomoTestError> {
    match ctx.parameter("cleanup") {
        None | Some("true") | Some("1") => Ok(true),
        Some("false") | Some("0") => Ok(false),
        Some(_) => Err(SinomoTestError::InvalidParameter {
            name: "cleanup".to_string(),
            detail: "must be true, false, 1, or 0".to_string(),
        }),
    }
}

fn optional_agent_channel_u64(
    ctx: &SinomoTestContext,
    key: &str,
) -> Result<Option<u64>, SinomoTestError> {
    ctx.parameter(key)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|source| SinomoTestError::InvalidParameter {
                    name: key.to_string(),
                    detail: format!("must be a nonnegative integer: {source}"),
                })
        })
        .transpose()
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
    fn agent_channel_expect_cleanup_defaults_to_legacy_success_cleanup() {
        let default = SinomoTestContext::new("test-data");
        assert!(agent_channel_cleanup_on_success(&default).unwrap());
        for value in ["false", "0"] {
            let retained = SinomoTestContext::new("test-data").with_parameter("cleanup", value);
            assert!(!agent_channel_cleanup_on_success(&retained).unwrap());
        }
        for value in ["true", "1"] {
            let cleanup = SinomoTestContext::new("test-data").with_parameter("cleanup", value);
            assert!(agent_channel_cleanup_on_success(&cleanup).unwrap());
        }
        let invalid = SinomoTestContext::new("test-data").with_parameter("cleanup", "sometimes");
        assert!(matches!(
            agent_channel_cleanup_on_success(&invalid),
            Err(SinomoTestError::InvalidParameter { ref name, .. }) if name == "cleanup"
        ));
    }

    #[test]
    fn agent_channel_expect_integer_assertions_do_not_ignore_invalid_values() {
        let absent = SinomoTestContext::new("test-data");
        assert_eq!(
            optional_agent_channel_u64(&absent, "message-count").unwrap(),
            None
        );
        let zero = SinomoTestContext::new("test-data").with_parameter("message-count", "0");
        assert_eq!(
            optional_agent_channel_u64(&zero, "message-count").unwrap(),
            Some(0)
        );
        let count = SinomoTestContext::new("test-data").with_parameter("message-count", "2");
        assert_eq!(
            optional_agent_channel_u64(&count, "message-count").unwrap(),
            Some(2)
        );
        for value in ["-1", "many", "18446744073709551616"] {
            let invalid =
                SinomoTestContext::new("test-data").with_parameter("message-count", value);
            assert!(matches!(
                optional_agent_channel_u64(&invalid, "message-count"),
                Err(SinomoTestError::InvalidParameter { ref name, .. }) if name == "message-count"
            ));
        }
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
