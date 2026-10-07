//! Project agent session activity into client traces for chat history.
//!
//! The session's activity log is the record of what an agent did; a message
//! only refers to the operation that produced it. History asks for the
//! operations of the messages it shows, and gets each as a client trace.

use std::collections::BTreeMap;
use std::str::FromStr;

use sinomo_agent::{
    AgentItem, AgentSessionItemRecord, AgentSessionStore, AgentToolStatus, FileAgentSessionStore,
};
use sinomo_runtime::api::app::chat as wire;
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;

/// Each requested operation's activity. References whose session cannot be
/// read are left out rather than failing the page.
pub(crate) fn session_activity(
    data_dir: &SinomoDataDir,
    request: wire::ChatSessionActivityRequest,
) -> wire::ChatSessionActivityResponse {
    let store = FileAgentSessionStore::for_data_dir(data_dir);
    let mut records_by_session = BTreeMap::<String, Option<Vec<AgentSessionItemRecord>>>::new();
    let mut activities = Vec::new();
    for activity in request.refs {
        let records = records_by_session
            .entry(activity.session_id.clone())
            .or_insert_with(|| {
                let session_id = SessionId::from_str(&activity.session_id).ok()?;
                store.load_item_records(&session_id).ok()
            });
        let Some(records) = records else {
            continue;
        };
        let trace = work_trace_from_records(records.iter().filter(|record| {
            record
                .op_id
                .as_ref()
                .is_some_and(|op| op.to_string() == activity.op_id)
        }));
        activities.push(wire::ChatOpActivity { activity, trace });
    }
    wire::ChatSessionActivityResponse { activities }
}

/// One operation's journaled items as a client trace, in journal order.
pub(crate) fn work_trace_from_records<'a>(
    records: impl IntoIterator<Item = &'a AgentSessionItemRecord>,
) -> wire::ChatWorkTrace {
    let mut trace = wire::ChatWorkTrace::default();
    for record in records {
        match &record.item {
            AgentItem::AssistantReasoningSummary {
                step_index,
                summary,
                ..
            } => trace.reasoning_summary(*step_index, summary),
            // Text of a step that goes on to call tools becomes intermediate
            // output; text of the final step is the reply, which `bounded`
            // drops when it finishes the trace.
            AgentItem::AssistantText {
                step_index,
                content,
                ..
            } => trace.assistant_text_delta(*step_index, &visible_text(content)),
            AgentItem::ToolCall {
                tool_call_id,
                step_index,
                call_index,
                call,
                ..
            } => {
                trace.tool_call_completed(&wire::ChatToolCallSnapshot {
                    step_index: *step_index,
                    call_index: Some(*call_index),
                    call_id: call.call_id.clone(),
                    tool: call.tool.clone(),
                    input: (!call.input.is_null())
                        .then(|| serde_json::to_string_pretty(&call.input).ok())
                        .flatten(),
                    input_preview: None,
                });
                if let Some(recorded) = trace.entries.iter_mut().rev().find_map(|entry| match entry
                {
                    wire::ChatWorkTraceEntry::Tool(recorded)
                        if recorded.step_index == *step_index
                            && recorded.call_index == Some(*call_index)
                            && recorded.tool_call_id.is_none() =>
                    {
                        Some(recorded)
                    }
                    _ => None,
                }) {
                    recorded.tool_call_id = Some(tool_call_id.to_string());
                }
            }
            AgentItem::ToolResult {
                call_id,
                status,
                output,
                ..
            } => {
                if let Some(call) = trace.tool_call_mut(&call_id.to_string()) {
                    apply_tool_result(call, status, output);
                }
            }
            _ => {}
        }
    }
    trace.bounded()
}

fn apply_tool_result(
    call: &mut wire::ChatToolActivity,
    status: &AgentToolStatus,
    output: &serde_json::Value,
) {
    call.status = match status {
        AgentToolStatus::Succeeded => "completed",
        AgentToolStatus::Failed => "failed",
        AgentToolStatus::Blocked => "blocked",
        AgentToolStatus::Cancelled => "cancelled",
    }
    .to_string();
    // Command tools report their streams; other tools show their output.
    if let Some(stdout) = output.get("stdout").and_then(serde_json::Value::as_str) {
        call.stdout = stdout.to_string();
        call.stderr = output
            .get("stderr")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        call.exit_code = output
            .get("status")
            .and_then(serde_json::Value::as_i64)
            .and_then(|code| i32::try_from(code).ok());
        call.elapsed_ms = output
            .get("duration_ms")
            .and_then(serde_json::Value::as_u64);
        call.process_command = output
            .get("program")
            .and_then(serde_json::Value::as_str)
            .map(|program| {
                std::iter::once(program)
                    .chain(
                        output
                            .get("args")
                            .and_then(serde_json::Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(serde_json::Value::as_str),
                    )
                    .collect::<Vec<_>>()
                    .join(" ")
            });
        call.process_status = Some(call.status.clone());
    } else if !output.is_null() {
        call.stdout = serde_json::to_string_pretty(output).unwrap_or_default();
    }
}

/// The user-visible text of a model response, which may be a structured
/// decision with a `text` field.
fn visible_text(content: &str) -> String {
    serde_json::from_str::<serde_json::Value>(content)
        .ok()
        .and_then(|value| value.get("text")?.as_str().map(str::to_string))
        .unwrap_or_else(|| content.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_agent::{AgentItemOrigin, AgentToolCall, AgentToolEvidence, TaskId};

    fn tool_call(tool_call_id: &TaskId, step_index: usize) -> AgentItem {
        AgentItem::ToolCall {
            item_id: TaskId::generate(),
            tool_call_id: tool_call_id.clone(),
            step_index,
            call_index: 0,
            call: AgentToolCall {
                call_id: None,
                tool: "exec_command".to_string(),
                input: serde_json::json!({ "cmd": "cargo check" }),
                r#async: false,
                summary: "check".to_string(),
            },
        }
    }

    fn tool_result(tool_call_id: &TaskId, step_index: usize) -> AgentItem {
        AgentItem::ToolResult {
            item_id: TaskId::generate(),
            step_index,
            call_index: 0,
            call_id: tool_call_id.clone(),
            native_call_id: None,
            tool_name: Some("exec_command".to_string()),
            status: AgentToolStatus::Succeeded,
            summary: "checked".to_string(),
            output: serde_json::json!({
                "program": "cargo", "args": ["check"], "status": 0,
                "duration_ms": 1200, "stdout": "Finished", "stderr": "",
            }),
            evidence: AgentToolEvidence::default(),
        }
    }

    fn text(step_index: usize, content: &str) -> AgentItem {
        AgentItem::AssistantText {
            item_id: TaskId::generate(),
            step_index,
            content: content.to_string(),
            native_items: Vec::new(),
        }
    }

    #[test]
    fn session_activity_projects_each_requested_operation() {
        let root = sinomo_testdir::TestDir::new("chat-session-activity");
        let data_dir = SinomoDataDir::new(&root);
        let store = FileAgentSessionStore::for_data_dir(&data_dir);
        let session_id = SessionId::generate();
        let first_op = TaskId::generate();
        let second_op = TaskId::generate();
        let call = TaskId::generate();
        store
            .append_items(
                &session_id,
                &AgentItemOrigin::for_op(first_op.clone(), []),
                &[
                    AgentItem::AssistantReasoningSummary {
                        item_id: TaskId::generate(),
                        step_index: 0,
                        summary: "check the build first".to_string(),
                    },
                    text(0, r#"{"response_mode":"tool_calls","text":"Checking."}"#),
                    tool_call(&call, 0),
                    tool_result(&call, 0),
                    text(1, "It builds."),
                ],
            )
            .unwrap();
        store
            .append_items(
                &session_id,
                &AgentItemOrigin::for_op(second_op.clone(), []),
                &[text(0, "Unrelated.")],
            )
            .unwrap();
        let reference = |op_id: &TaskId| wire::ChatActivityRef {
            participant_id: "agent".to_string(),
            session_id: session_id.to_string(),
            op_id: op_id.to_string(),
        };

        let response = session_activity(
            &data_dir,
            wire::ChatSessionActivityRequest {
                chat_session_id: "chat".to_string(),
                refs: vec![
                    reference(&first_op),
                    reference(&second_op),
                    wire::ChatActivityRef {
                        session_id: "not a session".to_string(),
                        ..reference(&first_op)
                    },
                ],
            },
        );

        assert_eq!(
            response.activities.len(),
            2,
            "unreadable sessions are skipped"
        );
        let trace = &response.activities[0].trace;
        assert!(matches!(
            &trace.entries[0],
            wire::ChatWorkTraceEntry::Reasoning { text, .. } if text == "check the build first"
        ));
        assert!(matches!(
            &trace.entries[1],
            wire::ChatWorkTraceEntry::Intermediate { text, .. } if text == "Checking."
        ));
        let calls = trace.tool_calls().collect::<Vec<_>>();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].tool_call_id.as_deref(),
            Some(call.to_string().as_str())
        );
        assert_eq!(calls[0].status, "completed");
        assert_eq!(calls[0].stdout, "Finished");
        assert_eq!(calls[0].exit_code, Some(0));
        assert_eq!(calls[0].process_command.as_deref(), Some("cargo check"));
        assert_eq!(
            trace.entries.len(),
            3,
            "the reply text is not part of the trace"
        );
        assert!(
            response.activities[1].trace.is_empty(),
            "an operation without tools or reasoning has an empty trace"
        );
    }
}
