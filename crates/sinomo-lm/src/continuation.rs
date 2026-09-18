use crate::{
    LanguageModelRequest, LanguageModelRequestItem, LanguageModelResponse,
    LanguageModelResponseItem, LanguageModelRole, LanguageModelToolCall,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// Transport-private assistant items. The runtime persists these without
/// interpreting them. They can only replace the matching provider/model's
/// assistant text and calls; ordinary transcript items remain the fallback
/// when a session changes providers or models.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LanguageModelProviderContinuation {
    pub provider: String,
    pub model: String,
    pub items: Vec<Value>,
    #[serde(default)]
    pub tool_call_ids: Vec<String>,
}

impl LanguageModelResponse {
    pub fn continuation_items(&self) -> Vec<LanguageModelRequestItem> {
        self.items
            .iter()
            .filter_map(|item| match item {
                LanguageModelResponseItem::ProviderContinuation { continuation }
                    if continuation.provider == self.provider
                        && continuation.model == self.model =>
                {
                    Some(LanguageModelRequestItem::ProviderContinuation {
                        continuation: continuation.clone(),
                    })
                }
                _ => None,
            })
            .collect()
    }
}

pub(crate) fn response_continuation(
    provider: &str,
    model: &str,
    items: Vec<Value>,
    calls: &[LanguageModelToolCall],
) -> LanguageModelResponseItem {
    LanguageModelResponseItem::ProviderContinuation {
        continuation: LanguageModelProviderContinuation {
            provider: provider.into(),
            model: model.into(),
            items,
            tool_call_ids: calls
                .iter()
                .filter_map(|call| call.call_id.clone())
                .collect(),
        },
    }
}

/// Select opaque replay only for an exact protocol identity. Remove its normal
/// assistant/call fallback once, keeping tool outputs and other turns intact.
pub(crate) fn provider_request_items(
    request: &LanguageModelRequest,
    provider: &str,
    model: &str,
) -> Vec<LanguageModelRequestItem> {
    let mut result = Vec::new();
    let mut replaced_calls = BTreeSet::new();
    for item in &request.items {
        match item {
            LanguageModelRequestItem::ProviderContinuation { continuation } => {
                if continuation.provider != provider || continuation.model != model {
                    continue;
                }
                if matches!(
                    result.last(),
                    Some(LanguageModelRequestItem::Message {
                        role: LanguageModelRole::Assistant,
                        ..
                    })
                ) {
                    result.pop();
                }
                replaced_calls.extend(continuation.tool_call_ids.iter().cloned());
                result.push(item.clone());
            }
            LanguageModelRequestItem::ToolCall { call_id, .. }
                if replaced_calls.contains(call_id) => {}
            _ => result.push(item.clone()),
        }
    }
    result
}
