use super::*;

#[derive(Clone)]
pub(super) struct RequestUserInputTool;

impl AgentTool for RequestUserInputTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        descriptor()
    }

    fn execute(&self, _request: super::super::AgentToolRequest<'_>) -> AgentToolResult {
        let payload = serde_json::json!({
            "error": "missing_user_input_bridge",
        });
        AgentToolResult::blocked(
            "request_user_input requires an attached interactive runtime bridge",
            payload.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(false, "missing interactive runtime bridge")
                .with_output_value(&payload, false),
        )
    }
}

pub(super) fn descriptor() -> AgentToolDescriptor {
    descriptor_for_kind(AgentToolKind::RequestUserInput)
        .with_capabilities([AgentToolCapability::PromptUser])
        .with_input_schema(serde_json::json!({
            "type": "object",
            "required": ["question"],
            "properties": {
                "question": { "type": "string" },
                "choices": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            }
        }))
}
