//! Bounded model-visible tool projections and retained evidence references.

use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentError, AgentLoopDecision, AgentToolCall, AgentToolEvidence,
    AgentToolOutcome, AgentToolStatus, LanguageModelRequestItem,
    MODEL_VISIBLE_COMMAND_AGGREGATED_BYTES, MODEL_VISIBLE_COMMAND_STDERR_BYTES,
    MODEL_VISIBLE_COMMAND_STDOUT_BYTES, MODEL_VISIBLE_READ_FILE_CONTENT_BYTES,
    MODEL_VISIBLE_TOOL_INPUT_STRING_BYTES, MODEL_VISIBLE_TOOL_RESULTS_AGGREGATE_BYTES,
    classify_command_failure,
};

pub(super) fn render_tool_results(
    step_index: usize,
    results: &[AgentToolOutcome],
) -> Result<String, AgentError> {
    let projection = model_visible_tool_results_projection(results)?;
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "tool_results",
        "step_index": step_index,
        "result_count": results.len(),
        "results": projection.results,
        "model_visible_budget": projection.budget,
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })
}

#[derive(Debug)]
pub(super) struct ModelVisibleToolResultsProjection {
    pub(super) results: Vec<serde_json::Value>,
    pub(super) budget: serde_json::Value,
}

pub(super) fn model_visible_tool_results_projection(
    outcomes: &[AgentToolOutcome],
) -> Result<ModelVisibleToolResultsProjection, AgentError> {
    let mut results = Vec::new();
    let mut used_bytes = 0usize;
    let mut omitted_full_result_count = 0usize;
    let mut omitted_summary_count = 0usize;
    let omitted_result_count = 0usize;
    for outcome in outcomes {
        let full = model_visible_tool_outcome(outcome);
        let full_bytes = serde_json::to_vec(&full)
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            })?
            .len();
        if used_bytes.saturating_add(full_bytes) <= MODEL_VISIBLE_TOOL_RESULTS_AGGREGATE_BYTES {
            used_bytes = used_bytes.saturating_add(full_bytes);
            results.push(full);
            continue;
        }

        omitted_full_result_count = omitted_full_result_count.saturating_add(1);
        let summary = model_visible_tool_outcome_summary_only(
            outcome,
            full_bytes,
            MODEL_VISIBLE_TOOL_RESULTS_AGGREGATE_BYTES,
        );
        let summary_bytes = serde_json::to_vec(&summary)
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            })?
            .len();
        used_bytes = used_bytes.saturating_add(summary_bytes);
        results.push(summary);
        omitted_summary_count = omitted_summary_count.saturating_add(1);
    }

    Ok(ModelVisibleToolResultsProjection {
        results,
        budget: serde_json::json!({
            "projection": "model_visible_aggregate_bounded",
            "max_result_bytes": MODEL_VISIBLE_TOOL_RESULTS_AGGREGATE_BYTES,
            "used_result_bytes": used_bytes,
            "full_result_count": outcomes.len().saturating_sub(omitted_full_result_count),
            "summary_only_result_count": omitted_summary_count,
            "omitted_full_result_count": omitted_full_result_count,
            "omitted_result_count": omitted_result_count,
        }),
    })
}

pub(super) fn model_visible_tool_outcome(outcome: &AgentToolOutcome) -> serde_json::Value {
    let mut value = serde_json::json!({
        "call_id": &outcome.call_id,
        "native_call_id": &outcome.call.call_id,
        "step_index": outcome.step_index,
        "call_index": outcome.call_index,
        "call": model_visible_tool_call(&outcome.call),
        "status": &outcome.status,
        "summary": &outcome.summary,
        "output": model_visible_tool_output(Some(outcome.call.tool.as_str()), &outcome.output),
        "evidence": model_visible_tool_evidence(&outcome.evidence),
    });
    if let Some(event_id) = &outcome.event_id
        && let Some(object) = value.as_object_mut()
    {
        object.insert("event_id".to_string(), serde_json::json!(event_id));
    }
    value
}

pub(super) fn model_visible_tool_call(call: &AgentToolCall) -> serde_json::Value {
    serde_json::json!({
        "call_id": &call.call_id,
        "tool": &call.tool,
        "input": model_visible_tool_input(&call.input),
        "async": call.r#async,
        "summary": &call.summary,
    })
}

pub(super) fn model_visible_tool_outcome_summary_only(
    outcome: &AgentToolOutcome,
    omitted_result_bytes: usize,
    aggregate_budget_bytes: usize,
) -> serde_json::Value {
    serde_json::json!({
        "call_id": &outcome.call_id,
        "native_call_id": &outcome.call.call_id,
        "step_index": outcome.step_index,
        "call_index": outcome.call_index,
        "tool": &outcome.call.tool,
        "status": &outcome.status,
        "summary": &outcome.summary,
        "output_omitted": {
            "reason": "model-visible tool result aggregate budget exceeded",
            "projection": "summary_only",
            "omitted_result_bytes": omitted_result_bytes,
            "aggregate_budget_bytes": aggregate_budget_bytes,
        },
        "evidence": {
            "availability": &outcome.evidence.availability,
            "output": &outcome.evidence.output,
            "artifact_count": outcome.evidence.artifacts.len(),
            "record_count": outcome.evidence.records.len(),
        },
    })
}

pub(super) fn native_tool_call_items_from_decision(
    decision: &AgentLoopDecision,
) -> Vec<LanguageModelRequestItem> {
    decision
        .tool_calls
        .iter()
        .filter_map(native_tool_call_item_from_call)
        .collect()
}

pub(super) fn native_tool_call_item_from_call(
    call: &AgentToolCall,
) -> Option<LanguageModelRequestItem> {
    Some(LanguageModelRequestItem::tool_call(
        call.call_id.as_ref()?.clone(),
        call.tool.clone(),
        model_visible_tool_input(&call.input),
    ))
}

pub(super) fn model_visible_assistant_content(content: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(content) else {
        return content.to_string();
    };
    let Some(tool_calls) = value
        .get_mut("tool_calls")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return content.to_string();
    };
    let mut changed = false;
    for tool_call in tool_calls {
        let Some(input) = tool_call.get_mut("input") else {
            continue;
        };
        let projected = model_visible_tool_input(input);
        if projected != *input {
            *input = projected;
            changed = true;
        }
    }
    if !changed {
        return content.to_string();
    }
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| content.to_string())
}

pub(super) fn native_tool_output_items_from_outcomes(
    outcomes: &[AgentToolOutcome],
) -> Result<Vec<LanguageModelRequestItem>, AgentError> {
    let projection = model_visible_tool_results_projection(outcomes)?;
    Ok(outcomes
        .iter()
        .zip(projection.results)
        .filter_map(|(outcome, output)| native_tool_output_item_from_outcome(outcome, output))
        .collect())
}

pub(super) fn native_tool_output_item_from_outcome(
    outcome: &AgentToolOutcome,
    output: serde_json::Value,
) -> Option<LanguageModelRequestItem> {
    Some(LanguageModelRequestItem::tool_call_output(
        outcome.call.call_id.as_ref()?.clone(),
        outcome.call.tool.clone(),
        output,
    ))
}

pub(super) fn model_visible_tool_input(input: &serde_json::Value) -> serde_json::Value {
    model_visible_json_value_with_string_bound(input, MODEL_VISIBLE_TOOL_INPUT_STRING_BYTES)
}

pub(super) fn model_visible_json_value_with_string_bound(
    value: &serde_json::Value,
    max_string_bytes: usize,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            serde_json::json!(model_visible_text_excerpt(text, max_string_bytes))
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| model_visible_json_value_with_string_bound(item, max_string_bytes))
                .collect(),
        ),
        serde_json::Value::Object(object) => {
            let mut projected = serde_json::Map::new();
            for (key, item) in object {
                let original_string_len = item.as_str().map(str::len);
                projected.insert(
                    key.clone(),
                    model_visible_json_value_with_string_bound(item, max_string_bytes),
                );
                if original_string_len.is_some_and(|len| len > max_string_bytes) {
                    projected.insert(
                        format!("{key}_model_visible_truncated"),
                        serde_json::json!(true),
                    );
                    projected.insert(
                        format!("{key}_original_bytes"),
                        serde_json::json!(original_string_len.unwrap_or_default()),
                    );
                }
            }
            serde_json::Value::Object(projected)
        }
        _ => value.clone(),
    }
}

pub(super) fn model_visible_tool_output(
    tool_name: Option<&str>,
    output: &serde_json::Value,
) -> serde_json::Value {
    let mut value = output.clone();
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    let looks_like_patch_output = tool_name == Some("apply_patch")
        || object.contains_key("diff") && object.contains_key("changed_files");
    if looks_like_file_read_output(tool_name, object) {
        bound_model_visible_string_field(
            object,
            "content",
            MODEL_VISIBLE_READ_FILE_CONTENT_BYTES,
            "content_model_visible_truncated",
        );
    }
    if looks_like_patch_output && let Some(diff) = object.get_mut("diff") {
        *diff = model_visible_patch_diff(diff);
    }
    let looks_like_command_output = looks_like_command_output(tool_name, object);
    if looks_like_command_output
        && let Some(diagnostics) = model_visible_command_output_diagnostics(object)
    {
        object.insert("command_output_diagnostics".to_string(), diagnostics);
    }
    if looks_like_command_output {
        sanitize_model_visible_command_output_refs(object);
        bound_model_visible_command_output_fields(object);
    }
    value
}

pub(super) fn looks_like_file_read_output(
    tool_name: Option<&str>,
    object: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    tool_name == Some("read_file")
        || object.contains_key("path")
            && object.contains_key("content")
            && object.contains_key("content_bytes")
            && object.contains_key("line_range")
}

pub(super) fn looks_like_command_output(
    tool_name: Option<&str>,
    object: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    tool_name == Some("run_command")
        || object.contains_key("program")
            && object.contains_key("args")
            && object.contains_key("status")
            && object.contains_key("stdout")
            && object.contains_key("stderr")
}

pub(super) fn model_visible_command_output_diagnostics(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let success = object
        .get("success")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let timed_out = object
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if success && !timed_out {
        return None;
    }
    let stderr = object
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let stdout = object
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if stderr.is_empty() && stdout.is_empty() && !timed_out {
        return None;
    }
    let aggregated_output = object
        .get("aggregated_output")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let output = serde_json::Value::Object(object.clone());
    let classification = classify_command_failure(&AgentToolStatus::Failed, "", &output);
    let mut diagnostics = serde_json::json!({
        "kind": if timed_out { "command_timed_out" } else { "command_failed" },
        "status": object.get("status").cloned().unwrap_or(serde_json::Value::Null),
        "timed_out": timed_out,
        "duration_ms": object.get("duration_ms").cloned().unwrap_or(serde_json::Value::Null),
        "timeout_ms": object.get("timeout_ms").cloned().unwrap_or(serde_json::Value::Null),
        "stdout_ref_available": command_output_ref_available(object, "stdout_ref"),
        "stderr_ref_available": command_output_ref_available(object, "stderr_ref"),
        "aggregated_output_excerpt": model_visible_text_excerpt(aggregated_output, 4096),
        "stderr_excerpt": model_visible_text_excerpt(stderr, 4096),
        "stdout_excerpt": model_visible_text_excerpt(stdout, 2048),
        "stderr_truncated": object.get("stderr_truncated").cloned().unwrap_or(serde_json::Value::Bool(false)),
        "stdout_truncated": object.get("stdout_truncated").cloned().unwrap_or(serde_json::Value::Bool(false)),
    });
    if let serde_json::Value::Object(ref mut map) = diagnostics {
        map.insert(
            "validation_status".to_string(),
            serde_json::to_value(&classification.status).unwrap_or(serde_json::Value::Null),
        );
        if let Some(failure_kind) = classification.failure_kind {
            map.insert(
                "failure_kind".to_string(),
                serde_json::to_value(failure_kind).unwrap_or(serde_json::Value::Null),
            );
        }
    }
    Some(diagnostics)
}

pub(super) fn command_output_ref_available(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> bool {
    object.get(key).is_some_and(|value| !value.is_null())
        || object
            .get("command_output")
            .and_then(serde_json::Value::as_object)
            .and_then(|command_output| command_output.get(key))
            .is_some_and(|value| !value.is_null())
}

pub(super) fn sanitize_model_visible_command_output_refs(
    object: &mut serde_json::Map<String, serde_json::Value>,
) {
    for key in ["stdout_ref", "stderr_ref"] {
        sanitize_model_visible_artifact_ref_field(object, key);
    }
    if let Some(command_output) = object
        .get_mut("command_output")
        .and_then(serde_json::Value::as_object_mut)
    {
        for key in ["stdout_ref", "stderr_ref"] {
            sanitize_model_visible_artifact_ref_field(command_output, key);
        }
    }
}

pub(super) fn bound_model_visible_command_output_fields(
    object: &mut serde_json::Map<String, serde_json::Value>,
) {
    bound_model_visible_command_string_field(object, "stdout", MODEL_VISIBLE_COMMAND_STDOUT_BYTES);
    bound_model_visible_command_string_field(object, "stderr", MODEL_VISIBLE_COMMAND_STDERR_BYTES);
    bound_model_visible_command_string_field(
        object,
        "aggregated_output",
        MODEL_VISIBLE_COMMAND_AGGREGATED_BYTES,
    );
    if let Some(command_output) = object
        .get_mut("command_output")
        .and_then(serde_json::Value::as_object_mut)
    {
        bound_model_visible_command_string_field(
            command_output,
            "stdout",
            MODEL_VISIBLE_COMMAND_STDOUT_BYTES,
        );
        bound_model_visible_command_string_field(
            command_output,
            "stderr",
            MODEL_VISIBLE_COMMAND_STDERR_BYTES,
        );
        bound_model_visible_command_string_field(
            command_output,
            "aggregated_output",
            MODEL_VISIBLE_COMMAND_AGGREGATED_BYTES,
        );
    }
}

pub(super) fn bound_model_visible_command_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    max_bytes: usize,
) {
    bound_model_visible_string_field(
        object,
        key,
        max_bytes,
        &format!("{key}_model_visible_truncated"),
    );
}

pub(super) fn bound_model_visible_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    max_bytes: usize,
    truncated_key: &str,
) {
    let Some(text) = object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    if text.len() <= max_bytes {
        return;
    }
    object.insert(
        key.to_string(),
        serde_json::json!(model_visible_text_excerpt(&text, max_bytes)),
    );
    object.insert(truncated_key.to_string(), serde_json::json!(true));
}

pub(super) fn sanitize_model_visible_artifact_ref_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
) {
    let Some(value) = object.get_mut(key) else {
        return;
    };
    if value.is_null() {
        return;
    }
    *value = model_visible_artifact_ref(value);
}

pub(super) fn model_visible_artifact_ref(ref_value: &serde_json::Value) -> serde_json::Value {
    let Some(object) = ref_value.as_object() else {
        return serde_json::json!({
            "uri_omitted": true,
        });
    };
    serde_json::json!({
        "kind": object.get("kind").cloned().unwrap_or(serde_json::Value::Null),
        "label": object.get("label").cloned().unwrap_or(serde_json::Value::Null),
        "media_type": object.get("media_type").cloned().unwrap_or(serde_json::Value::Null),
        "uri_omitted": true,
    })
}

pub(super) fn model_visible_text_excerpt(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    const TRUNCATED_MARKER: &str = "\n[truncated]";
    if max_bytes <= TRUNCATED_MARKER.len() {
        return String::new();
    }
    let mut end = max_bytes - TRUNCATED_MARKER.len();
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}{}", &text[..end], TRUNCATED_MARKER)
}

pub(super) fn model_visible_tool_evidence(evidence: &AgentToolEvidence) -> serde_json::Value {
    let artifacts = evidence
        .artifacts
        .iter()
        .map(|artifact| {
            serde_json::json!({
                "kind": artifact.kind,
                "label": artifact.label,
                "media_type": artifact.media_type,
                "uri_omitted": true,
            })
        })
        .collect::<Vec<_>>();
    let records = evidence
        .records
        .iter()
        .map(|record| {
            serde_json::json!({
                "kind": record.kind,
                "data": model_visible_evidence_record_data(&record.kind, &record.data),
            })
        })
        .collect::<Vec<_>>();
    let execution = evidence.execution.as_ref().map(|execution| {
        serde_json::json!({
            "environment": execution.environment,
            "exit_code": execution.exit_code,
            "cwd_omitted": execution.cwd.is_some(),
        })
    });
    let mut value = serde_json::json!({
        "availability": evidence.availability,
        "output": evidence.output,
        "artifacts": artifacts,
        "execution": execution,
        "records": records,
    });
    if !evidence.metadata.is_null()
        && let Some(object) = value.as_object_mut()
    {
        object.insert(
            "metadata_omitted".to_string(),
            serde_json::json!({
                "reason": "tool evidence metadata is stored in the run trace and omitted from model-visible context"
            }),
        );
    }
    value
}

pub(super) fn model_visible_evidence_record_data(
    kind: &str,
    data: &serde_json::Value,
) -> serde_json::Value {
    if kind == AGENT_CODING_EVIDENCE_KIND {
        let mut value = data.clone();
        if let Some(object) = value.as_object_mut() {
            let removed_patch_ref = object.remove("patch_ref").is_some();
            let removed_diff_ref = object.remove("diff_ref").is_some();
            let removed_refs = removed_patch_ref || removed_diff_ref;
            if removed_refs {
                object.insert("artifact_refs_omitted".to_string(), serde_json::json!(true));
            }
        }
        return value;
    }
    if kind == "patch_diff" {
        return serde_json::json!({
            "files": patch_diff_file_summaries(data),
            "content_omitted": true,
            "omitted_bytes": data.to_string().len(),
        });
    }
    data.clone()
}

pub(super) fn model_visible_patch_diff(diff: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "files": patch_diff_file_summaries(diff),
        "content_omitted": true,
        "omitted_bytes": diff.to_string().len(),
    })
}

pub(super) fn patch_diff_file_summaries(diff: &serde_json::Value) -> Vec<serde_json::Value> {
    diff.get("files")
        .and_then(serde_json::Value::as_array)
        .map(|files| {
            files
                .iter()
                .map(|file| {
                    serde_json::json!({
                        "status": file.get("status"),
                        "path": file.get("path"),
                        "added_lines": file.get("added_lines"),
                        "removed_lines": file.get("removed_lines"),
                        "diff_omitted_bytes": file
                            .get("diff")
                            .and_then(serde_json::Value::as_str)
                            .map(str::len)
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}
