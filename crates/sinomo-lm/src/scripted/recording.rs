//! Records a real provider's replies as a `ModelScript`.
//!
//! `RecordingProvider` wraps any provider and appends one exchange per
//! request to its script file, rewriting the file after each exchange so a
//! stopped process still leaves a usable script. Recorded exchanges expect
//! only what is stable across runs: that the previous reply's tool calls
//! came back as tool outputs. Authors tighten them by hand.
//!
//! Every recorded string is redacted before it is kept: configured literals,
//! the home directory, values of secret-looking environment variables, and
//! tokens with well-known credential prefixes. Provider continuation state and
//! reasoning are not recorded.

use super::{ModelScript, ScriptedExpectation, ScriptedReply, script_store_error};
use crate::{
    LanguageModelCancellation, LanguageModelError, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelRequestItem,
    LanguageModelResponse, LanguageModelStreamEvent,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

/// Records every configured provider's replies into this script file, for
/// harnesses that select providers by id (such as a daemon).
pub const RECORD_MODEL_SCRIPT_ENV: &str = "SINOMO_RECORD_MODEL_SCRIPT";

const REDACTED: &str = "<redacted>";
const SECRET_ENV_MARKERS: &[&str] = &["KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"];
const SECRET_PREFIXES: &[&str] = &[
    "sk-",
    "sk_live_",
    "sk_test_",
    "ghp_",
    "gho_",
    "ghu_",
    "ghs_",
    "ghr_",
    "github_pat_",
    "xoxb-",
    "xoxp-",
    "xapp-",
    "AKIA",
    "ASIA",
    "AIza",
    "eyJ",
];
const MIN_SECRET_TOKEN_CHARS: usize = 20;

/// Replaces sensitive text in recorded strings.
#[derive(Clone, Debug, Default)]
pub struct ScriptRedactor {
    literals: Vec<(String, String)>,
}

impl ScriptRedactor {
    /// Redacts the home directory and secret-looking environment values of
    /// this process, plus well-known credential tokens.
    pub fn for_current_process() -> Self {
        let mut redactor = Self::default();
        for (name, value) in std::env::vars() {
            let upper = name.to_ascii_uppercase();
            if value.len() >= 8
                && SECRET_ENV_MARKERS
                    .iter()
                    .any(|marker| upper.contains(marker))
            {
                redactor = redactor.with_literal(value, REDACTED);
            }
        }
        for name in ["USERPROFILE", "HOME"] {
            if let Some(home) = std::env::var_os(name).filter(|home| home.len() > 3) {
                redactor = redactor.with_path(Path::new(&home), "~");
            }
        }
        redactor
    }

    pub fn with_literal(
        mut self,
        literal: impl Into<String>,
        replacement: impl Into<String>,
    ) -> Self {
        let literal = literal.into();
        if !literal.is_empty() {
            self.literals.push((literal, replacement.into()));
            // Longest first, so a path is replaced before a prefix of it.
            self.literals
                .sort_by_key(|(literal, _)| std::cmp::Reverse(literal.len()));
        }
        self
    }

    /// Redacts a path in its native, forward-slash, and JSON-escaped forms.
    pub fn with_path(self, path: &Path, replacement: &str) -> Self {
        let native = path.display().to_string();
        self.with_literal(native.replace('\\', "\\\\"), replacement)
            .with_literal(native.replace('\\', "/"), replacement)
            .with_literal(native, replacement)
    }

    pub fn redact(&self, text: &str) -> String {
        let mut text = text.to_string();
        for (literal, replacement) in &self.literals {
            if text.contains(literal.as_str()) {
                text = text.replace(literal.as_str(), replacement);
            }
        }
        redact_secret_tokens(&text)
    }

    fn redact_json(&self, value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::String(text) => serde_json::Value::String(self.redact(text)),
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(|item| self.redact_json(item)).collect())
            }
            serde_json::Value::Object(fields) => serde_json::Value::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), self.redact_json(value)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }
}

fn is_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')
}

/// Replaces tokens that start with a credential prefix (or follow `Bearer `)
/// and are long enough to be a credential.
fn redact_secret_tokens(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        let at_boundary = out.chars().next_back().is_none_or(|ch| !is_token_char(ch));
        let after_bearer = out.ends_with("Bearer ") || out.ends_with("bearer ");
        let token_len = rest
            .find(|ch: char| !is_token_char(ch))
            .unwrap_or(rest.len());
        let token = &rest[..token_len];
        let secret = at_boundary
            && token.chars().count() >= MIN_SECRET_TOKEN_CHARS
            && (after_bearer
                || SECRET_PREFIXES
                    .iter()
                    .any(|prefix| token.starts_with(prefix)));
        if secret {
            out.push_str(REDACTED);
            rest = &rest[token_len..];
        } else if token_len > 0 {
            out.push_str(token);
            rest = &rest[token_len..];
        } else {
            let ch = rest.chars().next().expect("non-empty rest");
            out.push(ch);
            rest = &rest[ch.len_utf8()..];
        }
    }
    out
}

struct RecorderState {
    script: ModelScript,
    /// Tool calls of the previous reply, by call id, awaiting their outputs.
    pending_calls: HashMap<String, String>,
}

/// The script being recorded at one path. Clones share it.
#[derive(Clone)]
pub struct ScriptRecorder {
    path: PathBuf,
    redactor: ScriptRedactor,
    state: Arc<Mutex<RecorderState>>,
}

impl ScriptRecorder {
    pub fn new(path: impl Into<PathBuf>, redactor: ScriptRedactor) -> Self {
        Self {
            path: path.into(),
            redactor,
            state: Arc::new(Mutex::new(RecorderState {
                script: ModelScript::new(),
                pending_calls: HashMap::new(),
            })),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn script_json(&self) -> String {
        self.lock().script.to_json().unwrap_or_default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RecorderState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record(
        &self,
        request: &LanguageModelRequest,
        outcome: &Result<LanguageModelResponse, LanguageModelError>,
        finish_reason: Option<String>,
    ) -> Result<(), LanguageModelError> {
        let mut state = self.lock();
        let mut script = std::mem::take(&mut state.script);
        for item in &request.items {
            if let LanguageModelRequestItem::ToolCallOutput { call_id, name, .. } = item
                && state.pending_calls.remove(call_id).is_some()
            {
                script = script.expect(ScriptedExpectation::tool_output_for(name.clone()));
            }
        }
        state.pending_calls.clear();
        let reply = match outcome {
            Ok(response) => {
                for call in &response.tool_calls {
                    if let Some(call_id) = &call.call_id {
                        state
                            .pending_calls
                            .insert(call_id.clone(), call.name.clone());
                    }
                }
                let mut reply = self.redacted_reply(response);
                if let Some(reason) = finish_reason {
                    reply = reply.finish_reason(reason);
                }
                reply
            }
            Err(error) => ScriptedReply::error(self.redactor.redact(&error.to_string())),
        };
        state.script = script.reply(reply);
        state.script.save(&self.path)
    }

    fn redacted_reply(&self, response: &LanguageModelResponse) -> ScriptedReply {
        let mut redacted = response.clone();
        redacted.content = self.redactor.redact(&response.content);
        for call in &mut redacted.tool_calls {
            call.input = self.redactor.redact_json(&call.input);
            call.summary = call
                .summary
                .as_deref()
                .map(|text| self.redactor.redact(text));
        }
        ScriptedReply::from_response(&redacted)
    }
}

/// Wraps a provider and records each request's reply into a script.
pub struct RecordingProvider<P> {
    inner: P,
    recorder: ScriptRecorder,
}

impl<P: LanguageModelProvider> RecordingProvider<P> {
    pub fn new(inner: P, recorder: ScriptRecorder) -> Self {
        Self { inner, recorder }
    }

    pub fn recorder(&self) -> &ScriptRecorder {
        &self.recorder
    }

    fn recorded(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
        call: impl FnOnce(
            &P,
            &mut dyn FnMut(LanguageModelStreamEvent),
        ) -> Result<LanguageModelResponse, LanguageModelError>,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let mut finish_reason = None;
        let outcome = call(&self.inner, &mut |event| {
            if let LanguageModelStreamEvent::Finished {
                finish_reason: Some(reason),
            } = &event
            {
                finish_reason = Some(reason.clone());
            }
            on_event(event);
        });
        if let Err(error) = self.recorder.record(request, &outcome, finish_reason) {
            // Recording must not change what the caller sees; the missing
            // or stale script file shows the failure.
            eprintln!("failed to record model exchange: {error}");
        }
        outcome
    }
}

impl<P: LanguageModelProvider> LanguageModelProvider for RecordingProvider<P> {
    fn info(&self) -> LanguageModelProviderInfo {
        self.inner.info()
    }

    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.inner.available_models()
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.recorded(request, &mut |_| {}, |inner, _| inner.complete(request))
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.recorded(request, on_event, |inner, on_event| {
            inner.complete_streaming_events(request, on_event)
        })
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.recorded(request, on_event, |inner, on_event| {
            inner.complete_streaming_events_with_cancellation(request, cancellation, on_event)
        })
    }
}

/// Wraps a configured provider for recording when `RECORD_MODEL_SCRIPT_ENV`
/// names a script file. Configured providers are built per request, so the
/// recording for each file is process-wide.
pub(crate) fn record_configured_provider(
    provider: Box<dyn LanguageModelProvider>,
) -> Result<Box<dyn LanguageModelProvider>, LanguageModelError> {
    static RECORDERS: OnceLock<Mutex<HashMap<PathBuf, ScriptRecorder>>> = OnceLock::new();
    let Some(path) = std::env::var_os(RECORD_MODEL_SCRIPT_ENV).map(PathBuf::from) else {
        return Ok(provider);
    };
    if path.as_os_str().is_empty() {
        return Err(script_store_error(&path, "empty script path".to_string()));
    }
    let recorder = RECORDERS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(path.clone())
        .or_insert_with(|| ScriptRecorder::new(path, ScriptRedactor::for_current_process()))
        .clone();
    Ok(Box::new(RecordingProvider::new(
        ConfiguredProvider(provider),
        recorder,
    )))
}

struct ConfiguredProvider(Box<dyn LanguageModelProvider>);

impl LanguageModelProvider for ConfiguredProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        self.0.info()
    }

    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.0.available_models()
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.0.complete(request)
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.0.complete_streaming_events(request, on_event)
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.0
            .complete_streaming_events_with_cancellation(request, cancellation, on_event)
    }

    fn complete_streaming(
        &self,
        request: &LanguageModelRequest,
        on_delta: &mut dyn FnMut(&str),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.0.complete_streaming(request, on_delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripted::{ScriptedProvider, tests::request};
    use crate::{LanguageModelToolCall, LanguageModelUsage};

    struct FixedProvider(LanguageModelResponse);

    impl LanguageModelProvider for FixedProvider {
        fn info(&self) -> LanguageModelProviderInfo {
            ScriptedProvider::new(ModelScript::new()).info()
        }

        fn complete(
            &self,
            _: &LanguageModelRequest,
        ) -> Result<LanguageModelResponse, LanguageModelError> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn redaction_covers_literals_paths_and_credential_tokens() {
        let redactor = ScriptRedactor::default()
            .with_literal("hunter2-password", "<redacted>")
            .with_path(Path::new(r"C:\Users\pat"), "~");
        let text = r#"path C:\Users\pat\repo and C:/Users/pat/x and "C:\\Users\\pat\\y"; key sk-ant-REDACTMEREDACTMEREDACTME1234, Bearer abcdefghijklmnopqrstuvwxyz0123, task-runner-config-file-name-long, hunter2-password"#;
        let redacted = redactor.redact(text);
        assert_eq!(
            redacted,
            r#"path ~\repo and ~/x and "~\\y"; key <redacted>, Bearer <redacted>, task-runner-config-file-name-long, <redacted>"#
        );
    }

    #[test]
    fn recordings_replay_and_expect_the_outputs_of_recorded_calls() {
        let dir = sinomo_testdir::TestDir::new("scripted-recording");
        let path = dir.join("recorded.json");
        let recorder = ScriptRecorder::new(&path, ScriptRedactor::default());
        let calls_tool = RecordingProvider::new(
            FixedProvider(LanguageModelResponse {
                provider: "real".into(),
                model: "real-model".into(),
                content: String::new(),
                items: Vec::new(),
                tool_calls: vec![LanguageModelToolCall {
                    call_id: Some("call-1".into()),
                    name: "read_file".into(),
                    input: serde_json::json!({ "path": "a.txt" }),
                    summary: None,
                }],
                usage: Some(LanguageModelUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    ..Default::default()
                }),
            }),
            recorder.clone(),
        );
        calls_tool
            .complete(&request("read a.txt", &["read_file"]))
            .unwrap();
        let answers = RecordingProvider::new(
            FixedProvider(LanguageModelResponse {
                provider: "real".into(),
                model: "real-model".into(),
                content: "It says hi.".into(),
                items: Vec::new(),
                tool_calls: Vec::new(),
                usage: None,
            }),
            recorder,
        );
        let mut with_output = request("read a.txt", &["read_file"]);
        with_output
            .items
            .push(LanguageModelRequestItem::tool_call_output(
                "call-1",
                "read_file",
                serde_json::json!("hi"),
            ));
        answers.complete(&with_output).unwrap();

        let replay = ScriptedProvider::new(ModelScript::load(&path).unwrap());
        let first = replay
            .complete(&request("read a.txt", &["read_file"]))
            .unwrap();
        assert_eq!(first.tool_calls[0].call_id.as_deref(), Some("call-1"));
        assert_eq!(first.usage.unwrap().input_tokens, Some(10));
        let missing_output = replay
            .complete(&request("read a.txt", &["read_file"]))
            .unwrap_err();
        assert!(
            missing_output.to_string().contains("tool_output_for"),
            "{missing_output}"
        );
    }
}
