//! Deterministic provider that replays a script of model exchanges.
//!
//! The scripted provider sits behind the same `LanguageModelProvider` contract
//! as real providers, so the agent's request rendering, streaming, tool
//! orchestration, and cancellation run unchanged. Each exchange checks the
//! request it receives against its expectations before replying. Unexpected
//! requests, failed expectations, and replies calling tools the request did
//! not offer are returned to the caller as provider errors and recorded as
//! violations for `ScriptedProvider::assert_finished`.

use crate::{
    LanguageModelError, LanguageModelHostAccess, LanguageModelInvocation, LanguageModelProvider,
    LanguageModelProviderCapabilities, LanguageModelProviderInfo, LanguageModelRequest,
    LanguageModelRequestItem, LanguageModelResponse, LanguageModelRole, LanguageModelStreamEvent,
    LanguageModelToolCall, LanguageModelUsage, cancelled_language_model_error,
    current_request_cancelled,
};
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub const SCRIPTED_PROVIDER_ID: &str = "scripted";
pub const SCRIPTED_DEFAULT_MODEL: &str = "scripted";

/// How long a reply waits for a gate or cancellation before the script is
/// considered stuck. Bounded so a broken test fails instead of hanging.
const SCRIPTED_WAIT_LIMIT: Duration = Duration::from_secs(30);
const SCRIPTED_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// An ordered list of exchanges. Expectations added with `expect` apply to
/// the exchange closed by the next `reply`.
#[derive(Debug, Default)]
pub struct ModelScript {
    exchanges: Vec<ScriptedExchange>,
    pending: Vec<ScriptedExpectation>,
}

impl ModelScript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expect(mut self, expectation: ScriptedExpectation) -> Self {
        self.pending.push(expectation);
        self
    }

    pub fn reply(mut self, reply: ScriptedReply) -> Self {
        self.exchanges.push(ScriptedExchange {
            expectations: std::mem::take(&mut self.pending),
            reply,
        });
        self
    }
}

#[derive(Debug)]
struct ScriptedExchange {
    expectations: Vec<ScriptedExpectation>,
    reply: ScriptedReply,
}

/// A check on the request an exchange receives.
#[derive(Clone)]
pub enum ScriptedExpectation {
    /// The request offers a tool with this name.
    ToolOffered(String),
    /// The request carries a native output item for a call to this tool.
    ToolOutputFor(String),
    /// Some message, item, or context in the request contains this text.
    Contains(String),
    /// The latest user message contains this text.
    LastUserContains(String),
    Custom {
        label: String,
        check: Arc<dyn Fn(&LanguageModelRequest) -> Result<(), String> + Send + Sync>,
    },
}

impl ScriptedExpectation {
    pub fn tool_offered(name: impl Into<String>) -> Self {
        Self::ToolOffered(name.into())
    }

    pub fn tool_output_for(name: impl Into<String>) -> Self {
        Self::ToolOutputFor(name.into())
    }

    pub fn contains(text: impl Into<String>) -> Self {
        Self::Contains(text.into())
    }

    pub fn last_user_contains(text: impl Into<String>) -> Self {
        Self::LastUserContains(text.into())
    }

    pub fn custom(
        label: impl Into<String>,
        check: impl Fn(&LanguageModelRequest) -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self::Custom {
            label: label.into(),
            check: Arc::new(check),
        }
    }

    fn check(&self, request: &LanguageModelRequest) -> Result<(), String> {
        match self {
            Self::ToolOffered(name) => {
                if request.tools.iter().any(|tool| tool.name == *name) {
                    Ok(())
                } else {
                    Err(format!("tool `{name}` is not offered"))
                }
            }
            Self::ToolOutputFor(name) => {
                let found = request.items.iter().any(|item| {
                    matches!(item, LanguageModelRequestItem::ToolCallOutput { name: tool, .. }
                        if tool == name)
                });
                if found {
                    Ok(())
                } else {
                    Err(format!("no tool output for `{name}`"))
                }
            }
            Self::Contains(text) => {
                if request_texts(request).any(|content| content.contains(text.as_str())) {
                    Ok(())
                } else {
                    Err(format!("request does not contain {text:?}"))
                }
            }
            Self::LastUserContains(text) => match last_user_content(request) {
                Some(content) if content.contains(text.as_str()) => Ok(()),
                Some(content) => Err(format!(
                    "latest user message {:?} does not contain {text:?}",
                    excerpt(content)
                )),
                None => Err("request has no user message".to_string()),
            },
            Self::Custom { check, .. } => check(request),
        }
    }
}

impl fmt::Debug for ScriptedExpectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ToolOffered(name) => write!(f, "tool_offered({name:?})"),
            Self::ToolOutputFor(name) => write!(f, "tool_output_for({name:?})"),
            Self::Contains(text) => write!(f, "contains({text:?})"),
            Self::LastUserContains(text) => write!(f, "last_user_contains({text:?})"),
            Self::Custom { label, .. } => write!(f, "custom({label:?})"),
        }
    }
}

#[derive(Clone, Debug)]
enum ScriptedChunk {
    Text(String),
    Reasoning(String),
    ToolCall(LanguageModelToolCall),
}

#[derive(Clone, Debug, Default)]
enum ReplyEnding {
    #[default]
    Complete,
    /// Streams the chunks, then fails as if the connection dropped.
    Fail(String),
    /// Streams the chunks, then waits until the request is cancelled.
    AwaitCancellation,
}

/// What the provider streams back for one exchange.
#[derive(Clone, Debug, Default)]
pub struct ScriptedReply {
    chunks: Vec<ScriptedChunk>,
    usage: Option<LanguageModelUsage>,
    finish_reason: Option<String>,
    gate: Option<ScriptedGate>,
    ending: ReplyEnding,
}

impl ScriptedReply {
    pub fn new() -> Self {
        Self::default()
    }

    /// A reply that fails before streaming anything.
    pub fn error(detail: impl Into<String>) -> Self {
        Self::new().then_fail(detail)
    }

    /// A reply that streams nothing and returns only once the request is
    /// cancelled. It needs a caller that passes cancellation to the provider.
    pub fn await_cancellation() -> Self {
        Self {
            ending: ReplyEnding::AwaitCancellation,
            ..Self::default()
        }
    }

    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.chunks.push(ScriptedChunk::Text(text.into()));
        self
    }

    pub fn reasoning(mut self, text: impl Into<String>) -> Self {
        self.chunks.push(ScriptedChunk::Reasoning(text.into()));
        self
    }

    /// A native tool call. Its call id is assigned when the reply is served.
    pub fn tool_call(self, name: impl Into<String>, input: serde_json::Value) -> Self {
        self.push_tool_call(None, name.into(), input)
    }

    pub fn tool_call_with_id(
        self,
        call_id: impl Into<String>,
        name: impl Into<String>,
        input: serde_json::Value,
    ) -> Self {
        self.push_tool_call(Some(call_id.into()), name.into(), input)
    }

    fn push_tool_call(
        mut self,
        call_id: Option<String>,
        name: String,
        input: serde_json::Value,
    ) -> Self {
        self.chunks
            .push(ScriptedChunk::ToolCall(LanguageModelToolCall {
                call_id,
                name,
                input,
                summary: None,
            }));
        self
    }

    pub fn usage(mut self, input_tokens: u64, output_tokens: u64) -> Self {
        self.usage = Some(LanguageModelUsage {
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
        });
        self
    }

    pub fn finish_reason(mut self, reason: impl Into<String>) -> Self {
        self.finish_reason = Some(reason.into());
        self
    }

    /// Holds the reply until the gate opens or the request is cancelled.
    pub fn after(mut self, gate: &ScriptedGate) -> Self {
        self.gate = Some(gate.clone());
        self
    }

    /// Streams the chunks so far, then fails with `detail`.
    pub fn then_fail(mut self, detail: impl Into<String>) -> Self {
        self.ending = ReplyEnding::Fail(detail.into());
        self
    }
}

/// A test-controlled latch for holding a reply.
#[derive(Clone, Debug, Default)]
pub struct ScriptedGate(Arc<(Mutex<bool>, Condvar)>);

impl ScriptedGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&self) {
        let (open, changed) = &*self.0;
        *open.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        changed.notify_all();
    }

    fn is_open(&self) -> bool {
        *self
            .0
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Default)]
struct ScriptState {
    exchanges: VecDeque<ScriptedExchange>,
    served: usize,
    requests: Vec<LanguageModelRequest>,
    violations: Vec<String>,
}

/// Replays a `ModelScript`. Clones share the script, so a test can keep a
/// handle to inspect requests after moving the provider into an agent.
#[derive(Clone)]
pub struct ScriptedProvider {
    state: Arc<(Mutex<ScriptState>, Condvar)>,
}

impl ScriptedProvider {
    pub const PROVIDER_ID: &'static str = SCRIPTED_PROVIDER_ID;

    pub fn new(script: ModelScript) -> Self {
        assert!(
            script.pending.is_empty(),
            "scripted provider: expectations {:?} have no reply",
            script.pending
        );
        Self {
            state: Arc::new((
                Mutex::new(ScriptState {
                    exchanges: script.exchanges.into(),
                    ..ScriptState::default()
                }),
                Condvar::new(),
            )),
        }
    }

    pub fn requests(&self) -> Vec<LanguageModelRequest> {
        self.lock().requests.clone()
    }

    pub fn request_count(&self) -> usize {
        self.lock().requests.len()
    }

    pub fn remaining(&self) -> usize {
        self.lock().exchanges.len()
    }

    pub fn violations(&self) -> Vec<String> {
        self.lock().violations.clone()
    }

    /// Waits until at least `count` requests have arrived.
    pub fn wait_for_requests(&self, count: usize, timeout: Duration) -> bool {
        let (state, arrived) = &*self.state;
        let guard = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (guard, _) = arrived
            .wait_timeout_while(guard, timeout, |state| state.requests.len() < count)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.requests.len() >= count
    }

    /// Panics if any request violated the script or exchanges were left
    /// unserved.
    pub fn assert_finished(&self) {
        let state = self.lock();
        let mut problems = state.violations.clone();
        if !state.exchanges.is_empty() {
            problems.push(format!(
                "{} scripted exchange(s) were never requested",
                state.exchanges.len()
            ));
        }
        assert!(
            problems.is_empty(),
            "scripted provider:\n  {}",
            problems.join("\n  ")
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ScriptState> {
        self.state
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Records the request and takes the exchange that answers it.
    fn next_exchange(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<(usize, ScriptedExchange), String> {
        let (state, arrived) = &*self.state;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.requests.push(request.clone());
        arrived.notify_all();
        let index = state.served;
        let Some(exchange) = state.exchanges.pop_front() else {
            let violation = format!(
                "request #{} arrived after the script ended: {}",
                index + 1,
                describe_request(request)
            );
            state.violations.push(violation.clone());
            return Err(violation);
        };
        state.served += 1;
        let mut failures = exchange
            .expectations
            .iter()
            .filter_map(|expectation| {
                expectation
                    .check(request)
                    .err()
                    .map(|detail| format!("{expectation:?}: {detail}"))
            })
            .collect::<Vec<_>>();
        failures.extend(
            exchange
                .reply
                .chunks
                .iter()
                .filter_map(|chunk| match chunk {
                    ScriptedChunk::ToolCall(call)
                        if !request.tools.iter().any(|t| t.name == call.name) =>
                    {
                        Some(format!("reply calls `{}`, which is not offered", call.name))
                    }
                    _ => None,
                }),
        );
        if failures.is_empty() {
            return Ok((index, exchange));
        }
        let violation = format!(
            "exchange #{} does not match its request ({}):\n    {}",
            index + 1,
            describe_request(request),
            failures.join("\n    ")
        );
        state.violations.push(violation.clone());
        Err(violation)
    }

    fn record_violation(&self, violation: String) {
        self.lock().violations.push(violation);
    }

    /// Waits for `ready`, stopping early on cancellation. Returns whether the
    /// request was cancelled.
    fn wait(&self, ready: impl Fn() -> bool, what: &str) -> Result<bool, String> {
        let deadline = Instant::now() + SCRIPTED_WAIT_LIMIT;
        loop {
            if current_request_cancelled() {
                return Ok(true);
            }
            if ready() {
                return Ok(false);
            }
            if Instant::now() >= deadline {
                let violation = format!("{what} did not happen within {SCRIPTED_WAIT_LIMIT:?}");
                self.record_violation(violation.clone());
                return Err(violation);
            }
            std::thread::sleep(SCRIPTED_POLL_INTERVAL);
        }
    }
}

impl LanguageModelProvider for ScriptedProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: SCRIPTED_PROVIDER_ID.to_string(),
            default_model: SCRIPTED_DEFAULT_MODEL.to_string(),
            invocation: LanguageModelInvocation::InProcess,
            host_access: LanguageModelHostAccess::Bounded,
            capabilities: LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        Ok(vec![SCRIPTED_DEFAULT_MODEL.to_string()])
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming_events(request, &mut |_| {})
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let failed = |detail: String| LanguageModelError::ProviderInvocationFailed {
            provider: request.provider.clone(),
            model: request.model.clone(),
            detail: format!("scripted provider: {detail}"),
        };
        let (index, exchange) = self.next_exchange(request).map_err(failed)?;
        let reply = exchange.reply;
        if let Some(gate) = &reply.gate {
            let what = format!("gate for exchange #{}", index + 1);
            if self.wait(|| gate.is_open(), &what).map_err(failed)? {
                return Err(cancelled_language_model_error(request, "before response"));
            }
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let mut content = String::new();
        let mut tool_calls = Vec::new();
        for chunk in reply.chunks {
            match chunk {
                ScriptedChunk::Text(delta) => {
                    content.push_str(&delta);
                    on_event(LanguageModelStreamEvent::TextDelta { delta });
                }
                ScriptedChunk::Reasoning(delta) => {
                    on_event(LanguageModelStreamEvent::ReasoningDelta { delta });
                }
                ScriptedChunk::ToolCall(mut call) => {
                    let call_id = call.call_id.get_or_insert_with(|| {
                        format!("scripted-{}-{}", index + 1, tool_calls.len() + 1)
                    });
                    let call_id = Some(call_id.clone());
                    on_event(LanguageModelStreamEvent::ToolCallStarted {
                        call_id: call_id.clone(),
                        name: call.name.clone(),
                    });
                    on_event(LanguageModelStreamEvent::ToolCallDelta {
                        call_id: call_id.clone(),
                        name: Some(call.name.clone()),
                        arguments_delta: call.input.to_string(),
                    });
                    on_event(LanguageModelStreamEvent::ToolCallCompleted {
                        call_id,
                        name: call.name.clone(),
                        input: call.input.clone(),
                    });
                    tool_calls.push(call);
                }
            }
        }

        match reply.ending {
            ReplyEnding::Complete => {}
            ReplyEnding::Fail(detail) => return Err(failed(detail)),
            ReplyEnding::AwaitCancellation => {
                let what = format!("cancellation of exchange #{}", index + 1);
                self.wait(|| false, &what).map_err(failed)?;
                return Err(cancelled_language_model_error(request, "during response"));
            }
        }
        if let Some(usage) = &reply.usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: reply.finish_reason,
        });
        Ok(LanguageModelResponse {
            provider: request.provider.clone(),
            model: request.model.clone(),
            content,
            items: Vec::new(),
            tool_calls,
            usage: reply.usage,
        })
    }
}

fn request_texts(request: &LanguageModelRequest) -> impl Iterator<Item = String> + '_ {
    let messages = request
        .messages
        .iter()
        .map(|message| message.content.clone());
    let items = request.items.iter().filter_map(|item| match item {
        LanguageModelRequestItem::Message { content, .. }
        | LanguageModelRequestItem::Context { content, .. } => Some(content.clone()),
        LanguageModelRequestItem::ToolCall { input, .. } => Some(input.to_string()),
        LanguageModelRequestItem::ToolCallOutput { output, .. } => Some(match output {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        }),
        LanguageModelRequestItem::ProviderContinuation { .. } => None,
    });
    messages.chain(items)
}

fn last_user_content(request: &LanguageModelRequest) -> Option<&str> {
    let from_items = request.items.iter().rev().find_map(|item| match item {
        LanguageModelRequestItem::Message {
            role: LanguageModelRole::User,
            content,
        } => Some(content.as_str()),
        _ => None,
    });
    from_items.or_else(|| {
        request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == LanguageModelRole::User)
            .map(|message| message.content.as_str())
    })
}

fn describe_request(request: &LanguageModelRequest) -> String {
    let tools = request
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    format!(
        "{} message(s), {} item(s), tools [{}], latest user {:?}",
        request.messages.len(),
        request.items.len(),
        tools.join(", "),
        last_user_content(request).map(excerpt).unwrap_or_default()
    )
}

fn excerpt(text: &str) -> String {
    const LIMIT: usize = 160;
    match text.char_indices().nth(LIMIT) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LanguageModelCancellation, LanguageModelMessage, LanguageModelResponseFormat,
        LanguageModelToolSpec,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    fn request(user: &str, tools: &[&str]) -> LanguageModelRequest {
        LanguageModelRequest {
            provider: "fixture".into(),
            model: "fixture-model".into(),
            messages: vec![LanguageModelMessage {
                role: LanguageModelRole::User,
                content: user.into(),
            }],
            items: Vec::new(),
            tools: tools
                .iter()
                .map(|name| LanguageModelToolSpec {
                    name: (*name).into(),
                    description: String::new(),
                    input_schema: serde_json::Value::Null,
                    output_schema: serde_json::Value::Null,
                    supports_parallel: false,
                    provider_specs: Vec::new(),
                })
                .collect(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        }
    }

    #[test]
    fn replays_exchanges_in_order_and_streams_them() {
        let provider = ScriptedProvider::new(
            ModelScript::new()
                .expect(ScriptedExpectation::last_user_contains("build"))
                .expect(ScriptedExpectation::tool_offered("shell"))
                .reply(
                    ScriptedReply::new()
                        .text("Building.")
                        .tool_call("shell", serde_json::json!({ "command": "make" }))
                        .usage(10, 2),
                )
                .reply(ScriptedReply::new().text("Done.").finish_reason("stop")),
        );

        let mut events = Vec::new();
        let first = provider
            .complete_streaming_events(&request("please build", &["shell"]), &mut |event| {
                events.push(event)
            })
            .unwrap();
        assert_eq!(first.content, "Building.");
        assert_eq!(first.provider, "fixture", "responses echo the request");
        assert_eq!(first.tool_calls[0].call_id.as_deref(), Some("scripted-1-1"));
        assert!(matches!(
            events.as_slice(),
            [
                LanguageModelStreamEvent::Started { .. },
                LanguageModelStreamEvent::TextDelta { .. },
                LanguageModelStreamEvent::ToolCallStarted { .. },
                LanguageModelStreamEvent::ToolCallDelta { .. },
                LanguageModelStreamEvent::ToolCallCompleted { .. },
                LanguageModelStreamEvent::Usage { .. },
                LanguageModelStreamEvent::Finished {
                    finish_reason: None
                },
            ]
        ));

        let second = provider.complete(&request("next", &["shell"])).unwrap();
        assert_eq!(second.content, "Done.");
        assert_eq!(provider.request_count(), 2);
        provider.assert_finished();
    }

    #[test]
    fn mismatches_fail_the_request_and_are_reported() {
        let provider = ScriptedProvider::new(
            ModelScript::new()
                .expect(ScriptedExpectation::contains("needle"))
                .reply(ScriptedReply::new().text("found")),
        );
        let error = provider.complete(&request("haystack", &[])).unwrap_err();
        assert!(
            error.to_string().contains("contains(\"needle\")"),
            "{error}"
        );

        let extra = provider.complete(&request("again", &[])).unwrap_err();
        assert!(
            extra.to_string().contains("after the script ended"),
            "{extra}"
        );
        assert_eq!(provider.violations().len(), 2);
    }

    #[test]
    fn calling_a_tool_that_was_not_offered_is_a_violation() {
        let provider = ScriptedProvider::new(
            ModelScript::new()
                .reply(ScriptedReply::new().tool_call("shell", serde_json::json!({}))),
        );
        let error = provider
            .complete(&request("go", &["read_file"]))
            .unwrap_err();
        assert!(error.to_string().contains("not offered"), "{error}");
    }

    #[test]
    #[should_panic(expected = "never requested")]
    fn unserved_exchanges_fail_the_finish_check() {
        ScriptedProvider::new(ModelScript::new().reply(ScriptedReply::new().text("unused")))
            .assert_finished();
    }

    #[test]
    fn a_reply_can_fail_mid_stream() {
        let provider = ScriptedProvider::new(
            ModelScript::new().reply(ScriptedReply::new().text("partial").then_fail("reset")),
        );
        let mut deltas = Vec::new();
        let error = provider
            .complete_streaming(&request("go", &[]), &mut |delta| {
                deltas.push(delta.to_string())
            })
            .unwrap_err();
        assert_eq!(deltas, ["partial"]);
        assert!(error.to_string().contains("reset"));
        provider.assert_finished();
    }

    #[test]
    fn gated_replies_wait_and_waiting_replies_end_on_cancellation() {
        let gate = ScriptedGate::new();
        let provider = ScriptedProvider::new(
            ModelScript::new()
                .reply(ScriptedReply::new().text("released").after(&gate))
                .reply(ScriptedReply::await_cancellation()),
        );
        let worker = {
            let provider = provider.clone();
            std::thread::spawn(move || provider.complete(&request("go", &[])))
        };
        assert!(provider.wait_for_requests(1, Duration::from_secs(5)));
        assert!(!worker.is_finished());
        gate.open();
        assert_eq!(worker.join().unwrap().unwrap().content, "released");

        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = {
            let cancelled = Arc::clone(&cancelled);
            LanguageModelCancellation::new(move || cancelled.load(Ordering::SeqCst))
        };
        let worker = {
            let provider = provider.clone();
            std::thread::spawn(move || {
                provider.complete_streaming_events_with_cancellation(
                    &request("wait", &[]),
                    &cancellation,
                    &mut |_| {},
                )
            })
        };
        assert!(provider.wait_for_requests(2, Duration::from_secs(5)));
        cancelled.store(true, Ordering::SeqCst);
        let error = worker.join().unwrap().unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
        provider.assert_finished();
    }
}
