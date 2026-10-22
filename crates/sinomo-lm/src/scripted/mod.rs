//! Deterministic provider that replays a script of model exchanges.
//!
//! The scripted provider sits behind the same `LanguageModelProvider` contract
//! as real providers, so the agent's request rendering, streaming, tool
//! orchestration, and cancellation run unchanged. Each exchange checks the
//! request it receives against its expectations before replying. Unexpected
//! requests, failed expectations, and replies calling tools the request did
//! not offer are returned to the caller as provider errors and recorded as
//! violations for `ScriptedProvider::assert_finished`.
//!
//! Scripts are built in Rust with `ModelScript` or loaded from JSON:
//!
//! ```json
//! {
//!   "description": "reads a file, then answers",
//!   "exchanges": [
//!     {
//!       "expect": [{ "last_user_contains": "Cargo.toml" }, { "tool_offered": "read_file" }],
//!       "reply": { "stream": [{ "tool_call": { "name": "read_file", "input": { "path": "Cargo.toml" } } }] }
//!     },
//!     { "reply": { "stream": [{ "text": "It is a workspace manifest." }], "usage": { "input_tokens": 900, "output_tokens": 12 } } }
//!   ]
//! }
//! ```

use crate::{
    LanguageModelError, LanguageModelHostAccess, LanguageModelInvocation, LanguageModelProvider,
    LanguageModelProviderCapabilities, LanguageModelProviderInfo, LanguageModelRequest,
    LanguageModelRequestItem, LanguageModelResponse, LanguageModelRole, LanguageModelStreamEvent,
    LanguageModelToolCall, LanguageModelUsage, cancelled_language_model_error,
    current_request_cancelled,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

mod recording;

pub(crate) use recording::record_configured_provider;
pub use recording::{RECORD_MODEL_SCRIPT_ENV, RecordingProvider, ScriptRecorder, ScriptRedactor};

pub const SCRIPTED_PROVIDER_ID: &str = "scripted";
pub const SCRIPTED_DEFAULT_MODEL: &str = "scripted";
/// The script a configured `scripted` provider replays, for harnesses that
/// select the provider by id (such as a daemon a test starts). The provider
/// opens gates from files in `<script>.gates/`, keeps `<script>.report.json`
/// current, and appends each request to `<script>.requests.jsonl`.
pub const SCRIPTED_PROVIDER_SCRIPT_ENV: &str = "SINOMO_SCRIPTED_PROVIDER_SCRIPT";

/// How long a reply waits for a gate or cancellation before the script is
/// considered stuck. Bounded so a broken test fails instead of hanging.
const SCRIPTED_WAIT_LIMIT: Duration = Duration::from_secs(30);
const SCRIPTED_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// An ordered list of exchanges. Expectations added with `expect` apply to
/// the exchange closed by the next `reply`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelScript {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default)]
    exchanges: Vec<ScriptedExchange>,
    #[serde(skip)]
    pending: Vec<ScriptedExpectation>,
}

impl ModelScript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn describe(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
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

    pub fn len(&self) -> usize {
        self.exchanges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.exchanges.is_empty()
    }

    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|error| error.to_string())
    }

    pub fn to_json(&self) -> Result<String, String> {
        if !self.pending.is_empty() {
            return Err(format!("expectations {:?} have no reply", self.pending));
        }
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    pub fn load(path: &Path) -> Result<Self, LanguageModelError> {
        let json = std::fs::read_to_string(path)
            .map_err(|error| script_store_error(path, error.to_string()))?;
        Self::from_json(&json).map_err(|detail| script_store_error(path, detail))
    }

    pub fn save(&self, path: &Path) -> Result<(), LanguageModelError> {
        let json = self
            .to_json()
            .map_err(|detail| script_store_error(path, detail))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| script_store_error(path, error.to_string()))?;
        }
        std::fs::write(path, json + "\n")
            .map_err(|error| script_store_error(path, error.to_string()))
    }
}

fn script_store_error(path: &Path, detail: String) -> LanguageModelError {
    LanguageModelError::ConfigStore {
        path: path.display().to_string(),
        detail,
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptedExchange {
    #[serde(rename = "expect", default, skip_serializing_if = "Vec::is_empty")]
    expectations: Vec<ScriptedExpectation>,
    reply: ScriptedReply,
}

/// A check on the request an exchange receives.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptedExpectation {
    /// The request offers a tool with this name.
    ToolOffered(String),
    /// The request carries a native output item for a call to this tool.
    ToolOutputFor(String),
    /// Some message, item, or context in the request contains this text.
    Contains(String),
    /// The latest user-role message contains this text. With the JSON text
    /// tool protocol, tool results arrive as user messages too.
    LastUserContains(String),
    /// A check written in Rust; a script holding one cannot be saved.
    #[serde(skip)]
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ScriptedChunk {
    Text(String),
    Reasoning(String),
    ToolCall(LanguageModelToolCall),
}

/// What the provider streams back for one exchange.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ScriptedReply {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    stream: Vec<ScriptedChunk>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    usage: Option<LanguageModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finish_reason: Option<String>,
    /// Holds the reply until the named gate opens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    after_gate: Option<String>,
    /// Streams the chunks, then fails as if the connection dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fail: Option<String>,
    /// Streams the chunks, then waits until the request is cancelled.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    await_cancellation: bool,
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
            await_cancellation: true,
            ..Self::default()
        }
    }

    /// The reply a provider gave, as a script replays it. Provider
    /// continuation state is dropped: it is opaque and bound to the provider.
    pub fn from_response(response: &LanguageModelResponse) -> Self {
        let mut reply = Self::new();
        if !response.content.is_empty() {
            reply = reply.text(response.content.clone());
        }
        reply.stream.extend(
            response
                .tool_calls
                .iter()
                .cloned()
                .map(ScriptedChunk::ToolCall),
        );
        reply.usage = response.usage.clone();
        reply
    }

    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.stream.push(ScriptedChunk::Text(text.into()));
        self
    }

    pub fn reasoning(mut self, text: impl Into<String>) -> Self {
        self.stream.push(ScriptedChunk::Reasoning(text.into()));
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
        self.stream
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

    /// Holds the reply until the provider's gate `name` opens or the request
    /// is cancelled.
    pub fn after_gate(mut self, name: impl Into<String>) -> Self {
        self.after_gate = Some(name.into());
        self
    }

    /// Streams the chunks so far, then fails with `detail`.
    pub fn then_fail(mut self, detail: impl Into<String>) -> Self {
        self.fail = Some(detail.into());
        self
    }
}

/// A test-controlled latch for holding a reply.
#[derive(Clone, Debug, Default)]
pub struct ScriptedGate(Arc<Mutex<bool>>);

impl ScriptedGate {
    pub fn open(&self) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }

    pub fn is_open(&self) -> bool {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// How a script was followed: what a test checks once the provider is done.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScriptedReport {
    pub requests: usize,
    pub remaining: usize,
    pub violations: Vec<String>,
}

impl ScriptedReport {
    pub fn load(path: &Path) -> Result<Self, LanguageModelError> {
        let json = std::fs::read_to_string(path)
            .map_err(|error| script_store_error(path, error.to_string()))?;
        serde_json::from_str(&json).map_err(|error| script_store_error(path, error.to_string()))
    }

    /// Panics if any request violated the script or exchanges were left
    /// unserved.
    pub fn assert_finished(&self) {
        let mut problems = self.violations.clone();
        if self.remaining > 0 {
            problems.push(format!(
                "{} scripted exchange(s) were never requested",
                self.remaining
            ));
        }
        assert!(
            problems.is_empty(),
            "scripted provider:\n  {}",
            problems.join("\n  ")
        );
    }
}

#[derive(Default)]
struct ScriptState {
    exchanges: VecDeque<ScriptedExchange>,
    served: usize,
    requests: Vec<LanguageModelRequest>,
    violations: Vec<String>,
    gates: HashMap<String, ScriptedGate>,
}

impl ScriptState {
    fn report(&self) -> ScriptedReport {
        ScriptedReport {
            requests: self.requests.len(),
            remaining: self.exchanges.len(),
            violations: self.violations.clone(),
        }
    }
}

/// Replays a `ModelScript`. Clones share the script, so a test can keep a
/// handle to inspect requests after moving the provider into an agent.
#[derive(Clone)]
pub struct ScriptedProvider {
    state: Arc<(Mutex<ScriptState>, Condvar)>,
    gate_dir: Option<PathBuf>,
    report_path: Option<PathBuf>,
    request_log: Option<PathBuf>,
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
            gate_dir: None,
            report_path: None,
            request_log: None,
        }
    }

    /// Gates also open when a file named after the gate exists in `dir`, so
    /// a test can release a reply held in another process.
    pub fn with_gate_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.gate_dir = Some(dir.into());
        self
    }

    /// Rewrites `path` with the provider's `ScriptedReport` after every
    /// request, so a test in another process can check the script.
    pub fn with_report_file(self, path: impl Into<PathBuf>) -> Self {
        let provider = Self {
            report_path: Some(path.into()),
            ..self
        };
        provider.write_report(&provider.lock());
        provider
    }

    /// Appends each request to `path` as a JSON line, so a test in another
    /// process can inspect what the model was sent.
    pub fn with_request_log(mut self, path: impl Into<PathBuf>) -> Self {
        self.request_log = Some(path.into());
        self
    }

    /// The gate named `name`; replies waiting on it continue once it opens.
    pub fn gate(&self, name: &str) -> ScriptedGate {
        self.lock()
            .gates
            .entry(name.to_string())
            .or_default()
            .clone()
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

    pub fn report(&self) -> ScriptedReport {
        self.lock().report()
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
        self.report().assert_finished();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ScriptState> {
        self.state
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_report(&self, state: &ScriptState) {
        let Some(path) = &self.report_path else {
            return;
        };
        // A report that cannot be written makes the test's check fail on a
        // missing or stale file, which names the path.
        if let Ok(json) = serde_json::to_string_pretty(&state.report()) {
            let staged = path.with_extension("tmp");
            if std::fs::write(&staged, json).is_ok() {
                let _ = std::fs::rename(&staged, path);
            }
        }
    }

    fn log_request(&self, request: &LanguageModelRequest) {
        use std::io::Write;
        let Some(path) = &self.request_log else {
            return;
        };
        // Like the report, a log that cannot be written shows up as a
        // missing or short file.
        if let Ok(line) = serde_json::to_string(request)
            && let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
        {
            let _ = writeln!(file, "{line}");
        }
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
        self.log_request(request);
        let index = state.served;
        let result = match state.exchanges.pop_front() {
            None => Err(format!(
                "request #{} arrived after the script ended: {}",
                index + 1,
                describe_request(request)
            )),
            Some(exchange) => {
                state.served += 1;
                let failures = exchange_failures(&exchange, request);
                if failures.is_empty() {
                    Ok((index, exchange))
                } else {
                    Err(format!(
                        "exchange #{} does not match its request ({}):\n    {}",
                        index + 1,
                        describe_request(request),
                        failures.join("\n    ")
                    ))
                }
            }
        };
        if let Err(violation) = &result {
            state.violations.push(violation.clone());
        }
        self.write_report(&state);
        result
    }

    fn record_violation(&self, violation: String) {
        let mut state = self.lock();
        state.violations.push(violation);
        self.write_report(&state);
    }

    fn gate_open(&self, name: &str) -> bool {
        self.gate(name).is_open()
            || self
                .gate_dir
                .as_ref()
                .is_some_and(|dir| dir.join(name).exists())
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

fn exchange_failures(exchange: &ScriptedExchange, request: &LanguageModelRequest) -> Vec<String> {
    let expectations = exchange.expectations.iter().filter_map(|expectation| {
        expectation
            .check(request)
            .err()
            .map(|detail| format!("{expectation:?}: {detail}"))
    });
    let unoffered_calls = exchange
        .reply
        .stream
        .iter()
        .filter_map(|chunk| match chunk {
            ScriptedChunk::ToolCall(call) if !request.tools.iter().any(|t| t.name == call.name) => {
                Some(format!("reply calls `{}`, which is not offered", call.name))
            }
            _ => None,
        });
    expectations.chain(unoffered_calls).collect()
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
        if let Some(gate) = &reply.after_gate {
            let what = format!("gate `{gate}` for exchange #{}", index + 1);
            if self.wait(|| self.gate_open(gate), &what).map_err(failed)? {
                return Err(cancelled_language_model_error(request, "before response"));
            }
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: request.provider.clone(),
            model: request.model.clone(),
        });
        let mut content = String::new();
        let mut tool_calls = Vec::new();
        for chunk in reply.stream {
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

        if let Some(detail) = reply.fail {
            return Err(failed(detail));
        }
        if reply.await_cancellation {
            let what = format!("cancellation of exchange #{}", index + 1);
            self.wait(|| false, &what).map_err(failed)?;
            return Err(cancelled_language_model_error(request, "during response"));
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

/// The provider for the configured provider id. Configured providers are
/// built per request, so the script's progress lives in a process-wide
/// provider per script file.
pub(crate) fn configured_scripted_provider() -> Result<ScriptedProvider, LanguageModelError> {
    static PROVIDERS: OnceLock<Mutex<HashMap<PathBuf, ScriptedProvider>>> = OnceLock::new();
    let path = std::env::var_os(SCRIPTED_PROVIDER_SCRIPT_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| LanguageModelError::ProviderInvocationFailed {
            provider: SCRIPTED_PROVIDER_ID.to_string(),
            model: SCRIPTED_DEFAULT_MODEL.to_string(),
            detail: format!("set {SCRIPTED_PROVIDER_SCRIPT_ENV} to the script to replay"),
        })?;
    let mut providers = PROVIDERS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(provider) = providers.get(&path) {
        return Ok(provider.clone());
    }
    let provider = ScriptedProvider::new(ModelScript::load(&path)?)
        .with_gate_dir(path.with_extension("gates"))
        .with_report_file(path.with_extension("report.json"))
        .with_request_log(path.with_extension("requests.jsonl"));
    providers.insert(path, provider.clone());
    Ok(provider)
}

fn request_texts(request: &LanguageModelRequest) -> impl Iterator<Item = String> + '_ {
    let messages = request
        .messages
        .iter()
        .map(|message| message.content.clone());
    let items = request.items.iter().filter_map(|item| match item {
        LanguageModelRequestItem::Image { media_type, .. } => Some(format!("[image:{media_type}]")),
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

    pub(super) fn request(user: &str, tools: &[&str]) -> LanguageModelRequest {
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
        let provider = ScriptedProvider::new(
            ModelScript::new()
                .reply(ScriptedReply::new().text("released").after_gate("release"))
                .reply(ScriptedReply::await_cancellation()),
        );
        let worker = {
            let provider = provider.clone();
            std::thread::spawn(move || provider.complete(&request("go", &[])))
        };
        assert!(provider.wait_for_requests(1, Duration::from_secs(5)));
        assert!(!worker.is_finished());
        provider.gate("release").open();
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

    #[test]
    fn scripts_round_trip_through_json() {
        let script = ModelScript::new()
            .describe("reads, then answers")
            .expect(ScriptedExpectation::tool_offered("read_file"))
            .reply(
                ScriptedReply::new()
                    .reasoning("look first")
                    .tool_call("read_file", serde_json::json!({ "path": "a" })),
            )
            .reply(
                ScriptedReply::new()
                    .text("done")
                    .usage(3, 1)
                    .finish_reason("stop")
                    .after_gate("go"),
            )
            .reply(ScriptedReply::error("overloaded"))
            .reply(ScriptedReply::await_cancellation());
        let json = script.to_json().unwrap();
        let reloaded = ModelScript::from_json(&json).unwrap();
        assert_eq!(reloaded.to_json().unwrap(), json);
        assert_eq!(reloaded.len(), 4);
        assert!(json.contains(r#""tool_offered": "read_file""#), "{json}");
    }

    #[test]
    fn the_documented_file_format_loads() {
        let docs = include_str!("mod.rs")
            .lines()
            .skip_while(|line| *line != "//! ```json")
            .skip(1)
            .take_while(|line| *line != "//! ```")
            .map(|line| line.trim_start_matches("//!"))
            .collect::<Vec<_>>()
            .join(
                "
",
            );
        let provider = ScriptedProvider::new(ModelScript::from_json(&docs).unwrap());
        let first = provider
            .complete(&request("what is in Cargo.toml?", &["read_file"]))
            .unwrap();
        assert_eq!(first.tool_calls[0].name, "read_file");
        let second = provider.complete(&request("next", &["read_file"])).unwrap();
        assert_eq!(second.content, "It is a workspace manifest.");
        provider.assert_finished();
    }

    #[test]
    fn unknown_fields_and_rust_checks_are_rejected() {
        let typo = r#"{ "exchanges": [{ "reply": { "steam": [] } }] }"#;
        assert!(ModelScript::from_json(typo).unwrap_err().contains("steam"));
        let custom = ModelScript::new()
            .expect(ScriptedExpectation::custom("any", |_| Ok(())))
            .reply(ScriptedReply::new());
        assert!(custom.to_json().is_err());
    }

    #[test]
    fn another_process_can_open_gates_and_read_the_report() {
        let dir = sinomo_testdir::TestDir::new("scripted-provider-files");
        let report_path = dir.join("report.json");
        let provider = ScriptedProvider::new(
            ModelScript::new().reply(ScriptedReply::new().text("released").after_gate("go")),
        )
        .with_gate_dir(dir.join("gates"))
        .with_report_file(&report_path);
        assert_eq!(ScriptedReport::load(&report_path).unwrap().remaining, 1);

        let worker = {
            let provider = provider.clone();
            std::thread::spawn(move || provider.complete(&request("go", &[])))
        };
        assert!(provider.wait_for_requests(1, Duration::from_secs(5)));
        std::fs::create_dir_all(dir.join("gates")).unwrap();
        std::fs::write(dir.join("gates").join("go"), "").unwrap();
        assert_eq!(worker.join().unwrap().unwrap().content, "released");

        let report = ScriptedReport::load(&report_path).unwrap();
        assert_eq!(report.requests, 1);
        report.assert_finished();
    }
}
