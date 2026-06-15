//! Agent cluster runtime test cases.
//!
//! These run on a live cluster node (`sinomo test run <id> --require-cluster`)
//! and probe the node's daemon-hosted agent RPC surface without creating a
//! model session. The sandbox active-cancel probe creates and destroys a
//! temporary sandbox instance under the daemon-hosted sandbox service.

use std::sync::mpsc;
use std::time::Duration;

use sinomo_agent::{
    AGENT_OBJECT_RPC_ROUTES, AGENT_SESSION_INSPECT_ROUTE, AGENT_SESSION_PROGRESS_ROUTE,
    AGENT_SESSION_TERMINAL_RESULTS_ROUTE, AgentEvent, AgentEventPayload, AgentRole, AgentSession,
    AgentSessionControlEventCursor, AgentSessionControlSnapshot, AgentSessionInspectRequest,
    AgentSessionInspectResponse, AgentSessionProgressRequest, AgentSessionProgressResponse,
    AgentSessionStore, AgentSessionStoreRecord, AgentSessionStoreRecordKind,
    AgentSessionTerminalResultsRequest, AgentSessionTerminalResultsResponse,
    AgentTaskLifecycleStatus, AgentTaskRef, AgentTaskSnapshot, FileAgentSessionStore, TaskId,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{
    MODULE_NATIVE_DYLIB_CANCEL_ROUTE, MODULE_RPC_CATEGORY, ObjectRpcCancelRequest,
    ObjectRpcCancelResponse, ObjectRpcRequest, ObjectRpcResponse, SANDBOX_CREATE_ROUTE,
    SANDBOX_DESTROY_ROUTE, SANDBOX_RPC_CATEGORY, SANDBOX_RUN_CANCEL_ROUTE,
    SANDBOX_RUN_TOOLCHAIN_ROUTE,
};
use sinomo_runtime::sandbox::{
    BUSYBOX_TOOLCHAIN_LEGACY_ID, SANDBOX_UNLIMITED_WALL_TIME_MS, SandboxCreateInstanceRequest,
    SandboxCreateInstanceResponse, SandboxDestroyRequest, SandboxId, SandboxLocalCommandOutcome,
    SandboxRunCancelResponse, SandboxRunToolchainRequest, SandboxTempPolicy, SandboxToolchainId,
};
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};

const BENCHMARK_CANCEL_PROBE_ROUTE: &str = "/benchmarks/cancel-probe";
const BENCHMARK_RPC_CATEGORY: &str = "benchmarks";

#[derive(Clone, Debug, serde::Serialize)]
struct BenchmarkCancelProbeRequest {
    max_runtime_ms: u64,
    poll_interval_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct BenchmarkCancelProbeResponse {
    cancelled: bool,
    elapsed_ms: u64,
    polls: u64,
}

pub fn register_agent_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(agent_runtime_routes())
        .register(agent_runtime_persisted_session_routes())
        .register(module_native_dylib_cancel_route())
        .register(module_native_dylib_active_cancel())
        .register(sandbox_run_cancel_route())
        .register(sandbox_active_cancel())
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
    pub fn module_native_dylib_active_cancel(ctx) {
        id: "cluster.agent.module-native-dylib-active-cancel",
        summary: "Daemon-hosted native-dylib module work observes operation-id cancellation",
        tags: ["cluster-agent", "agent-readiness", "module-cancellation"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_module_native_dylib_active_cancel(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn sandbox_run_cancel_route(ctx) {
        id: "cluster.agent.sandbox-cancel-route",
        summary: "Daemon-hosted sandbox run cancellation route is reachable on this cluster node",
        tags: ["cluster-agent", "agent-readiness", "sandbox-cancellation"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_sandbox_run_cancel_route(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn sandbox_active_cancel(ctx) {
        id: "cluster.agent.sandbox-active-cancel",
        summary: "Daemon-hosted sandbox run cancellation interrupts active sandbox work by operation id",
        tags: ["cluster-agent", "agent-readiness", "sandbox-cancellation"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_sandbox_active_cancel(ctx)
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

fn run_module_native_dylib_active_cancel(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let operation_id = "agent_cluster_module_native_dylib_active_cancel_probe";
    ctx.record("operation_id", operation_id);
    ctx.record("route", BENCHMARK_CANCEL_PROBE_ROUTE);
    let run = BenchmarkCancelProbeRequest {
        max_runtime_ms: 30_000,
        poll_interval_ms: 25,
    };
    let run_request =
        ObjectRpcRequest::encode_body(BENCHMARK_CANCEL_PROBE_ROUTE, BENCHMARK_RPC_CATEGORY, &run)
            .map_err(|source| SinomoTestError::failed(source.to_string()))?
            .with_operation_id(operation_id);

    let (run_sender, run_receiver) =
        mpsc::channel::<Result<BenchmarkCancelProbeResponse, crate::CliError>>();
    std::thread::spawn(move || {
        let result = probe_daemon_object_rpc::<BenchmarkCancelProbeResponse>(&run_request);
        let _ = run_sender.send(result);
    });

    let mut inactive_responses = 0_u64;
    let cancel_response = loop {
        match run_receiver.try_recv() {
            Ok(result) => return Ok(module_active_cancel_early_run_outcome(result)),
            Err(mpsc::TryRecvError::Disconnected) => {
                return Ok(SinomoTestOutcome::failed(
                    "native dylib module cancel-probe worker disconnected before cancellation",
                ));
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }

        let cancel_request = ObjectRpcRequest::encode_body(
            MODULE_NATIVE_DYLIB_CANCEL_ROUTE,
            MODULE_RPC_CATEGORY,
            &ObjectRpcCancelRequest::new(
                operation_id,
                "active native dylib module cancellation probe",
            ),
        )
        .map_err(|source| SinomoTestError::failed(source.to_string()))?
        .with_cancels_operation_id(operation_id);
        let response: ObjectRpcCancelResponse =
            match probe_daemon_object_rpc::<ObjectRpcCancelResponse>(&cancel_request) {
                Ok(response) => response,
                Err(error) => {
                    return Ok(SinomoTestOutcome::failed(format!(
                        "native dylib module cancel RPC failed while run was active: {error}"
                    )));
                }
            };
        ctx.record("last_cancel_response", &response);
        if response.cancelled {
            break response;
        }
        inactive_responses = inactive_responses.saturating_add(1);
        if inactive_responses > 100 {
            return Ok(SinomoTestOutcome::failed(
                "native dylib module run did not become cancellable before the probe deadline",
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    ctx.record("cancel_response", &cancel_response);
    match run_receiver.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(response)) if response.cancelled => Ok(SinomoTestOutcome::passed(
            "active native dylib module work observed operation-id cancellation",
        )
        .with_diagnostics(serde_json::json!({
            "operation_id": operation_id,
            "cancel_response": cancel_response,
            "run_response": response,
            "inactive_cancel_responses_before_active": inactive_responses,
        }))),
        Ok(Ok(response)) => Ok(SinomoTestOutcome::failed(format!(
            "native dylib module run completed without observing cancellation after {}ms and {} polls",
            response.elapsed_ms, response.polls
        ))),
        Ok(Err(error)) => Ok(SinomoTestOutcome::failed(format!(
            "native dylib module run ended with an error after cancel was accepted: {error}"
        ))),
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(SinomoTestOutcome::failed(
            "native dylib module run did not finish within 10s after cancel was accepted",
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => Ok(SinomoTestOutcome::failed(
            "native dylib module cancel-probe worker disconnected after cancel was accepted",
        )),
    }
}

fn run_sandbox_run_cancel_route(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let operation_id = "agent_cluster_sandbox_cancel_route_probe";
    ctx.record("operation_id", operation_id);
    let request = ObjectRpcRequest::encode_body(
        SANDBOX_RUN_CANCEL_ROUTE,
        SANDBOX_RPC_CATEGORY,
        &ObjectRpcCancelRequest::new(operation_id, "cluster readiness route probe"),
    )
    .map_err(|source| SinomoTestError::failed(source.to_string()))?
    .with_cancels_operation_id(operation_id);
    let response: SandboxRunCancelResponse =
        probe_daemon_object_rpc(&request).map_err(|source| {
            SinomoTestError::failed(format!("sandbox cancel RPC route failed: {source}"))
        })?;
    ctx.record("cancel_response", &response);

    if response.operation_id != operation_id {
        return Ok(SinomoTestOutcome::failed(
            "sandbox cancel route returned the wrong operation id",
        ));
    }
    if response.cancelled {
        return Ok(SinomoTestOutcome::failed(
            "synthetic sandbox cancel probe unexpectedly cancelled active work",
        ));
    }
    if !response.detail.contains("no active sandbox run") {
        return Ok(SinomoTestOutcome::failed(
            "sandbox cancel route returned an unexpected inactive-operation detail",
        ));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "sandbox run cancel route is reachable on {}",
        ctx.cluster_config_source()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "cluster runtime".to_string())
    )))
}

fn run_sandbox_active_cancel(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let toolchain_id = SandboxToolchainId::from_legacy_named(BUSYBOX_TOOLCHAIN_LEGACY_ID)
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let sandbox_id = SandboxId::generate();
    let operation_id = format!("agent_cluster_sandbox_active_cancel_{}", sandbox_id);
    ctx.record("sandbox_id", &sandbox_id);
    ctx.record("operation_id", &operation_id);
    ctx.record("toolchain_id", &toolchain_id);

    let create = SandboxCreateInstanceRequest {
        sandbox_id: Some(sandbox_id.clone()),
        toolchain_id,
        temp_policy: SandboxTempPolicy::RetainAcrossSessions,
        memory_mb: None,
        wall_time_ms: Some(30_000),
        max_input_bytes: None,
        max_output_bytes: None,
        max_workspace_bytes: None,
        max_written_bytes_per_command: None,
        max_file_count: None,
        mounts: Vec::new(),
    };
    let create_request =
        ObjectRpcRequest::encode_body(SANDBOX_CREATE_ROUTE, SANDBOX_RPC_CATEGORY, &create)
            .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    match probe_daemon_object_rpc::<SandboxCreateInstanceResponse>(&create_request) {
        Ok(response) => ctx.record("created_sandbox", &response.manifest.sandbox_id),
        Err(error) => {
            return Ok(sandbox_active_cancel_setup_outcome(
                "create temporary sandbox",
                &error.to_string(),
            ));
        }
    }

    let run = SandboxRunToolchainRequest {
        sandbox_id: sandbox_id.clone(),
        command_name: "sh".to_string(),
        argv: vec!["sh".to_string(), "-c".to_string(), "sleep 30".to_string()],
        stdin: None,
        timeout_ms: SANDBOX_UNLIMITED_WALL_TIME_MS,
    };
    let run_request =
        ObjectRpcRequest::encode_body(SANDBOX_RUN_TOOLCHAIN_ROUTE, SANDBOX_RPC_CATEGORY, &run)
            .map_err(|source| SinomoTestError::failed(source.to_string()))?
            .with_operation_id(operation_id.clone());

    let (run_sender, run_receiver) =
        mpsc::channel::<Result<SandboxLocalCommandOutcome, crate::CliError>>();
    std::thread::spawn(move || {
        let result = probe_daemon_object_rpc::<SandboxLocalCommandOutcome>(&run_request);
        let _ = run_sender.send(result);
    });

    let mut inactive_responses = 0_u64;
    let cancel_response = loop {
        match run_receiver.try_recv() {
            Ok(result) => {
                cleanup_sandbox(&sandbox_id);
                return Ok(sandbox_active_cancel_early_run_outcome(result));
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                cleanup_sandbox(&sandbox_id);
                return Ok(SinomoTestOutcome::failed(
                    "sandbox run worker disconnected before cancellation",
                ));
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }

        let cancel_request = ObjectRpcRequest::encode_body(
            SANDBOX_RUN_CANCEL_ROUTE,
            SANDBOX_RPC_CATEGORY,
            &ObjectRpcCancelRequest::new(&operation_id, "active sandbox cancellation probe"),
        )
        .map_err(|source| SinomoTestError::failed(source.to_string()))?
        .with_cancels_operation_id(&operation_id);
        let response: SandboxRunCancelResponse =
            match probe_daemon_object_rpc::<SandboxRunCancelResponse>(&cancel_request) {
                Ok(response) => response,
                Err(error) => {
                    cleanup_sandbox(&sandbox_id);
                    return Ok(SinomoTestOutcome::failed(format!(
                        "sandbox cancel RPC failed while run was active: {error}"
                    )));
                }
            };
        ctx.record("last_cancel_response", &response);
        if response.cancelled {
            break response;
        }
        inactive_responses = inactive_responses.saturating_add(1);
        if inactive_responses > 100 {
            cleanup_sandbox(&sandbox_id);
            return Ok(SinomoTestOutcome::failed(
                "sandbox run did not become cancellable before the probe deadline",
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    ctx.record("cancel_response", &cancel_response);
    match run_receiver.recv_timeout(Duration::from_secs(10)) {
        Ok(Err(error)) if is_sandbox_cancel_error(&error.to_string()) => {
            cleanup_sandbox(&sandbox_id);
            Ok(SinomoTestOutcome::passed(
                "active sandbox run was interrupted by operation-id cancellation",
            )
            .with_diagnostics(serde_json::json!({
                "operation_id": operation_id,
                "cancel_response": cancel_response,
                "inactive_cancel_responses_before_active": inactive_responses,
            })))
        }
        Ok(Err(error)) => {
            cleanup_sandbox(&sandbox_id);
            Ok(SinomoTestOutcome::failed(format!(
                "sandbox run ended with a non-cancel error after cancel was accepted: {error}"
            )))
        }
        Ok(Ok(outcome)) => {
            cleanup_sandbox(&sandbox_id);
            Ok(SinomoTestOutcome::failed(format!(
                "sandbox run completed instead of cancelling; exit status {}",
                outcome.response.exit_status
            )))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            cleanup_sandbox(&sandbox_id);
            Ok(SinomoTestOutcome::failed(
                "sandbox run did not finish within 10s after cancel was accepted",
            ))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            cleanup_sandbox(&sandbox_id);
            Ok(SinomoTestOutcome::failed(
                "sandbox run worker disconnected after cancel was accepted",
            ))
        }
    }
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
    let response = send_daemon_object_rpc(request)?;
    response.decode_body().map_err(crate::CliError::from)
}

fn send_daemon_object_rpc(
    request: &ObjectRpcRequest,
) -> Result<ObjectRpcResponse, crate::CliError> {
    let Some(status) = crate::default_main_daemon_status()? else {
        return Err(crate::CliError::DaemonNotRunning {
            detail: "cluster runtime tests require the local daemon RPC service".to_string(),
        });
    };
    crate::daemon_send_daemon_rpc(&status.address, request, Duration::from_secs(10))
        .map_err(crate::CliError::from)
}

fn module_active_cancel_early_run_outcome(
    result: Result<BenchmarkCancelProbeResponse, crate::CliError>,
) -> SinomoTestOutcome {
    match result {
        Ok(response) => SinomoTestOutcome::failed(format!(
            "native dylib module run completed before cancellation; cancelled={} elapsed_ms={} polls={}",
            response.cancelled, response.elapsed_ms, response.polls
        )),
        Err(error) => {
            let detail = error.to_string();
            if module_active_cancel_prerequisite_missing(&detail) {
                SinomoTestOutcome::skipped(format!(
                    "native dylib module active-cancel probe skipped before run became active: {detail}"
                ))
            } else {
                SinomoTestOutcome::failed(format!(
                    "native dylib module run failed before cancellation: {detail}"
                ))
            }
        }
    }
}

fn module_active_cancel_prerequisite_missing(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("unknown route")
        || detail.contains("/benchmarks/cancel-probe")
        || detail.contains("module.benchmarks")
        || detail.contains("native dylib module")
        || detail.contains("does not support operation")
        || detail.contains("unsupported benchmark module operation")
}

fn sandbox_active_cancel_setup_outcome(action: &str, detail: &str) -> SinomoTestOutcome {
    if sandbox_active_cancel_prerequisite_missing(detail) {
        return SinomoTestOutcome::skipped(format!(
            "sandbox active-cancel probe skipped during {action}: {detail}"
        ));
    }
    SinomoTestOutcome::failed(format!(
        "sandbox active-cancel probe failed during {action}: {detail}"
    ))
}

fn sandbox_active_cancel_early_run_outcome(
    result: Result<SandboxLocalCommandOutcome, crate::CliError>,
) -> SinomoTestOutcome {
    match result {
        Ok(outcome) => SinomoTestOutcome::failed(format!(
            "sandbox run completed before cancellation; exit status {}",
            outcome.response.exit_status
        )),
        Err(error) => {
            let detail = error.to_string();
            if sandbox_active_cancel_prerequisite_missing(&detail) {
                SinomoTestOutcome::skipped(format!(
                    "sandbox active-cancel probe skipped before run became active: {detail}"
                ))
            } else {
                SinomoTestOutcome::failed(format!(
                    "sandbox run failed before cancellation: {detail}"
                ))
            }
        }
    }
}

fn sandbox_active_cancel_prerequisite_missing(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("sandbox runtime not compiled")
        || detail.contains("sandbox toolchain not found")
        || detail.contains("sandbox toolchain command not found")
        || detail.contains("module does not support")
        || detail.contains("wasix module")
}

fn is_sandbox_cancel_error(detail: &str) -> bool {
    detail.to_ascii_lowercase().contains("cancelled")
}

fn cleanup_sandbox(sandbox_id: &SandboxId) {
    let request = match ObjectRpcRequest::encode_body(
        SANDBOX_DESTROY_ROUTE,
        SANDBOX_RPC_CATEGORY,
        &SandboxDestroyRequest {
            sandbox_id: sandbox_id.clone(),
        },
    ) {
        Ok(request) => request,
        Err(_) => return,
    };
    let _ = send_daemon_object_rpc(&request);
}
