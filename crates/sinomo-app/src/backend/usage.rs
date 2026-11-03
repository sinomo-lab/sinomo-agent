use super::Backend;
use sinomo_client::{
    UsageClient,
    usage_contract::{UsageQuery, UsageQueryResponse},
};
impl Backend {
    pub fn usage_query(&self, query: &UsageQuery) -> Result<UsageQueryResponse, String> {
        let address = self
            .connection
            .daemon_address()
            .ok_or("usage requires a daemon connection")?;
        UsageClient::at_address(address)
            .query(query)
            .map_err(|e| e.to_string())
    }
}
impl Backend {
    pub fn usage_evidence(
        &self,
        record: &sinomo_client::usage_contract::UsageRecord,
    ) -> Result<String, String> {
        use sinomo_client::chat_contract::*;
        let a = &record.attribution;
        let chat = a
            .chat
            .as_ref()
            .ok_or("This record belongs to a background service; no conversation was reported.")?;
        let delegating = a.parent_session.is_some() && a.parent_operation.is_some();
        let session = if delegating {
            a.parent_session.as_ref()
        } else {
            a.session.as_ref()
        }
        .ok_or("The producer did not report a model session.")?;
        let operation = if delegating {
            a.parent_operation.as_ref()
        } else {
            a.operation.as_ref()
        }
        .ok_or("The producer did not report an activity operation.")?;
        let tool_call = if delegating {
            a.parent_tool_call.as_ref()
        } else {
            a.tool_call.as_ref()
        };
        let client = sinomo_client::ChatHistoryClient::at_address(
            self.connection
                .daemon_address()
                .ok_or("Reconnect to read activity evidence")?,
        );
        let participant = if let Some(id) = &a.participant {
            id.clone()
        } else {
            return Err(format!(
                "Conversation {chat}, session {session}, operation {operation}: no participant identity was reported."
            ));
        };
        let response = client
            .activity(ChatSessionActivityRequest {
                chat_session_id: chat.clone(),
                refs: vec![ChatActivityRef {
                    participant_id: participant,
                    session_id: session.clone(),
                    op_id: operation.clone(),
                }],
            })
            .map_err(|e| e.to_string())?;
        let mut text = format!(
            "Conversation {chat}\n{} session {session} · operation {operation}\n",
            if delegating { "Delegating" } else { "Agent" }
        );
        let mut found = false;
        for activity in response.activities {
            for entry in activity.trace.entries {
                match entry {
                    ChatWorkTraceEntry::Tool(tool) => {
                        if tool_call.is_none_or(|id| tool.tool_call_id.as_ref() == Some(id)) {
                            found = true;
                            text.push_str(&format!(
                                "\n{} · {}\nInput: {}\n{}\n{}\n",
                                tool.tool, tool.status, tool.input, tool.stdout, tool.stderr
                            ));
                        }
                    }
                    ChatWorkTraceEntry::Reasoning { text: reason, .. }
                    | ChatWorkTraceEntry::Intermediate { text: reason, .. }
                        if tool_call.is_none() =>
                    {
                        found = true;
                        text.push_str(&reason);
                        text.push('\n');
                    }
                    _ => {}
                }
            }
        }
        if !found {
            return Err(
                "The requested activity is not retained or has no visible evidence.".into(),
            );
        }
        Ok(text)
    }
}
