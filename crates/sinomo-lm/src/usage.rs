//! Normalization of provider-reported units. Never estimate missing counts.
use crate::LanguageModelUsage;
use serde_json::Value;

pub(crate) fn openai(value: &Value) -> Option<LanguageModelUsage> {
    let u = value.get("usage").filter(|u| u.is_object())?;
    let result = LanguageModelUsage {
        input_tokens: u["input_tokens"]
            .as_u64()
            .or_else(|| u["prompt_tokens"].as_u64()),
        output_tokens: u["output_tokens"]
            .as_u64()
            .or_else(|| u["completion_tokens"].as_u64()),
        cache_read_input_tokens: u
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .or_else(|| {
                u.pointer("/prompt_tokens_details/cached_tokens")
                    .and_then(Value::as_u64)
            })
            .or_else(|| {
                u.pointer("/input_token_details/cached_tokens")
                    .and_then(Value::as_u64)
            })
            .or_else(|| u["prompt_cache_hit_tokens"].as_u64()),
        reasoning_tokens: u
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .or_else(|| {
                u.pointer("/completion_tokens_details/reasoning_tokens")
                    .and_then(Value::as_u64)
            }),
        ..Default::default()
    };
    record_usage_report(&result);
    Some(result)
}

pub(crate) fn anthropic(u: &Value) -> LanguageModelUsage {
    let read = u["cache_read_input_tokens"].as_u64();
    let write = u["cache_creation_input_tokens"].as_u64();
    let result = LanguageModelUsage {
        // Anthropic reports uncached input separately; normalize to inclusive input.
        input_tokens: u["input_tokens"].as_u64().map(|n| {
            n.saturating_add(read.unwrap_or(0))
                .saturating_add(write.unwrap_or(0))
        }),
        output_tokens: u["output_tokens"].as_u64(),
        cache_read_input_tokens: read,
        cache_write_input_tokens: write,
        hosted_searches: u
            .pointer("/server_tool_use/web_search_requests")
            .and_then(Value::as_u64),
        ..Default::default()
    };
    record_usage_report(&result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn absent_null_empty_and_zero_remain_distinct() {
        assert_eq!(openai(&json!({})), None);
        assert_eq!(openai(&json!({"usage":null})), None);
        assert!(!openai(&json!({"usage":{}})).unwrap().reported());
        assert!(
            openai(&json!({"usage":{"input_tokens":0}}))
                .unwrap()
                .reported()
        );
    }
    #[test]
    fn openai_responses_and_chat_details_are_preserved() {
        for value in [
            json!({"usage":{"input_tokens":100,"output_tokens":20,"input_tokens_details":{"cached_tokens":70},"output_tokens_details":{"reasoning_tokens":10}}}),
            json!({"usage":{"prompt_tokens":100,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":70},"completion_tokens_details":{"reasoning_tokens":10}}}),
        ] {
            let u = openai(&value).unwrap();
            assert_eq!(
                (
                    u.input_tokens,
                    u.output_tokens,
                    u.cache_read_input_tokens,
                    u.reasoning_tokens
                ),
                (Some(100), Some(20), Some(70), Some(10))
            );
        }
    }
    #[test]
    fn anthropic_input_includes_cache_and_search_units() {
        let u = anthropic(
            &json!({"input_tokens":10,"cache_read_input_tokens":20,"cache_creation_input_tokens":30,"server_tool_use":{"web_search_requests":2}}),
        );
        assert_eq!(u.input_tokens, Some(60));
        assert_eq!(u.cache_read_input_tokens, Some(20));
        assert_eq!(u.cache_write_input_tokens, Some(30));
        assert_eq!(u.hosted_searches, Some(2));
        assert_eq!(u.output_tokens, None);
    }
}

use sinomo_runtime::api::app::usage::{UsageAttribution, UsageEvent, UsageRecord, UsageStatus};
pub use sinomo_runtime::api::observe::recording::timestamp_ms;
use sinomo_runtime::local::SinomoDataDir;
use std::time::Instant;
pub fn submit_usage(data_dir: &SinomoDataDir, record: UsageRecord) {
    sinomo_runtime::api::observe::recording::submit(data_dir, record.into_envelope());
}
pub fn record(attribution: UsageAttribution, event: UsageEvent) -> UsageRecord {
    UsageRecord {
        id: sinomo_runtime::core::base::Id::generate().to_string(),
        timestamp_ms: timestamp_ms(),
        attribution,
        event,
    }
}
pub struct RequestObservation<'a> {
    data_dir: &'a SinomoDataDir,
    attribution: UsageAttribution,
    provider: String,
    account: String,
    model: String,
    started: Instant,
    retries: RetryScope,
    retry_count_known: bool,
    completed: bool,
}
impl<'a> RequestObservation<'a> {
    pub fn new(
        data_dir: &'a SinomoDataDir,
        provider: &str,
        model: &str,
        account: Option<&crate::ProviderAccountConfig>,
        request: &crate::LanguageModelRequest,
    ) -> Self {
        let mut attribution: UsageAttribution = request
            .metadata
            .get("usage_attribution")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_else(|| UsageAttribution {
                origin: String::new(),
                purpose: String::new(),
                ..Default::default()
            });
        attribution.inherit_from(&current_attribution());
        if attribution.origin.is_empty() {
            attribution.origin = request
                .metadata
                .get("loop")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("runtime")
                .into();
        }
        if attribution.purpose.is_empty() {
            attribution.purpose = request
                .metadata
                .get("purpose")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("completion")
                .into();
        }
        Self {
            retries: RetryScope::enter(),
            retry_count_known: true,
            completed: false,
            data_dir,
            attribution,
            provider: provider.into(),
            account: account
                .map(|a| a.account.clone())
                .unwrap_or_else(|| "default".into()),
            model: model.into(),
            started: Instant::now(),
        }
    }
    pub fn unreported_retries(&mut self) {
        self.retry_count_known = false;
    }
    pub fn finish(
        mut self,
        result: &Result<crate::LanguageModelResponse, crate::LanguageModelError>,
        cancelled: bool,
        first_output_ms: Option<u64>,
        streamed_usage: Option<LanguageModelUsage>,
    ) {
        self.completed = true;
        if let Some(usage) = streamed_usage {
            self.retries.report(&usage);
        }
        if let Some(usage) = result.as_ref().ok().and_then(|r| r.usage.as_ref()) {
            self.retries.report(usage);
        }
        let units = self.retries.reported_usage();
        let status = if cancelled {
            UsageStatus::Cancelled
        } else if result.is_ok() {
            UsageStatus::Succeeded
        } else {
            UsageStatus::Failed
        };
        let error_class = result.as_ref().err().map(|e| {
            match e {
                crate::LanguageModelError::AuthenticationRequired { .. } => "authentication",
                crate::LanguageModelError::ProviderSpawnFailed { .. } => "spawn",
                _ => "provider",
            }
            .to_string()
        });
        let model = result
            .as_ref()
            .ok()
            .map(|r| r.model.clone())
            .unwrap_or_else(|| self.model.clone());
        tracing::info!(target:"sinomo.lm.usage",provider=%self.provider,model=%model,account=%self.account,status=?status,retries=?self.retry_count_known.then(||self.retries.count()),duration_ms=self.started.elapsed().as_millis() as u64,session=self.attribution.session.as_deref(),operation=self.attribution.operation.as_deref(),reported=units.reported(),"model request completed");
        submit_usage(
            self.data_dir,
            record(
                self.attribution.clone(),
                UsageEvent::ModelRequest {
                    provider: self.provider.clone(),
                    account: self.account.clone(),
                    model,
                    operation: "completion".into(),
                    status,
                    error_class,
                    retry_count: self.retry_count_known.then(|| self.retries.count()),
                    duration_ms: Some(
                        self.started.elapsed().as_millis().min(u64::MAX as u128) as u64
                    ),
                    first_output_ms,
                    reported: units.reported(),
                    units,
                },
            ),
        );
    }
}
#[cfg(test)]
mod recording_tests {
    use super::*;
    use crate::*;
    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: "unknown".into(),
            model: "test".into(),
            messages: vec![],
            items: vec![],
            tools: vec![],
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::json!({"usage_attribution":UsageAttribution{session:Some("session".into()),purpose:"decision_repair".into(),..Default::default()}}),
        }
    }
    #[test]
    fn failed_configured_request_is_recorded_once_with_attribution() {
        let dir = sinomo_testdir::TestDir::new("usage-configured-failure");
        let data = SinomoDataDir::new(&dir);
        let (tx, mut rx) = std::sync::mpsc::sync_channel(2);
        let _registration = sinomo_runtime::api::observe::recording::register(&data, tx);
        assert!(
            complete_configured_model_request(&data, "unknown", "test", None, &request()).is_err()
        );
        let sinomo_runtime::api::observe::recording::Submission::Event(envelope) =
            rx.try_recv().unwrap()
        else {
            panic!("event")
        };
        let record: UsageRecord = serde_json::from_value(envelope.payload).unwrap();
        assert_eq!(record.attribution.session.as_deref(), Some("session"));
        assert_eq!(record.attribution.purpose, "decision_repair");
        assert!(matches!(
            record.event,
            UsageEvent::ModelRequest {
                status: UsageStatus::Failed,
                reported: false,
                ..
            }
        ));
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn closed_usage_receiver_does_not_change_original_error() {
        let dir = sinomo_testdir::TestDir::new("usage-closed");
        let data = SinomoDataDir::new(&dir);
        let baseline =
            complete_configured_model_request(&data, "unknown", "test", None, &request())
                .unwrap_err();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let _registration = sinomo_runtime::api::observe::recording::register(&data, tx);
        drop(rx);
        let actual = complete_configured_model_request(&data, "unknown", "test", None, &request())
            .unwrap_err();
        assert_eq!(baseline, actual);
    }
    #[test]
    fn abandoned_request_retains_native_partial_usage() {
        let dir = sinomo_testdir::TestDir::new("usage-abandoned");
        let data = SinomoDataDir::new(&dir);
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let _registration = sinomo_runtime::api::observe::recording::register(&data, tx);
        let observation = RequestObservation::new(&data, "fixture", "model", None, &request());
        record_usage_report(&LanguageModelUsage {
            input_tokens: Some(17),
            ..Default::default()
        });
        drop(observation);
        let sinomo_runtime::api::observe::recording::Submission::Event(event) =
            rx.try_recv().unwrap()
        else {
            panic!("event")
        };
        let record: UsageRecord = serde_json::from_value(event.payload).unwrap();
        assert!(matches!(
            record.event,
            UsageEvent::ModelRequest {
                status: UsageStatus::Cancelled,
                reported: true,
                units: LanguageModelUsage {
                    input_tokens: Some(17),
                    ..
                },
                ..
            }
        ));
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn cancelled_stream_retains_last_reported_units() {
        let dir = sinomo_testdir::TestDir::new("usage-cancelled");
        let data = SinomoDataDir::new(&dir);
        let (tx, mut rx) = std::sync::mpsc::sync_channel(1);
        let _registration = sinomo_runtime::api::observe::recording::register(&data, tx);
        let observation = RequestObservation::new(&data, "fixture", "model", None, &request());
        observation.finish(
            &Err(LanguageModelError::ProviderInvocationFailed {
                provider: "fixture".into(),
                model: "model".into(),
                detail: "cancelled".into(),
            }),
            true,
            Some(4),
            Some(LanguageModelUsage {
                input_tokens: Some(12),
                ..Default::default()
            }),
        );
        let sinomo_runtime::api::observe::recording::Submission::Event(envelope) =
            rx.try_recv().unwrap()
        else {
            panic!("event")
        };
        let record: UsageRecord = serde_json::from_value(envelope.payload).unwrap();
        assert!(matches!(
            record.event,
            UsageEvent::ModelRequest {
                status: UsageStatus::Cancelled,
                reported: true,
                units: LanguageModelUsage {
                    input_tokens: Some(12),
                    ..
                },
                ..
            }
        ));
    }
}

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, Ordering},
};
#[derive(Default)]
pub(crate) struct RequestMeasurements {
    retries: AtomicU32,
    units: Mutex<ReportedUnits>,
}
#[derive(Default)]
struct ReportedUnits {
    committed: LanguageModelUsage,
    retry_committed: LanguageModelUsage,
    current: LanguageModelUsage,
}
thread_local! {
    static ATTRIBUTION:RefCell<Vec<UsageAttribution>>=const {RefCell::new(Vec::new())};
    static RETRIES:RefCell<Vec<Arc<RequestMeasurements>>>=const {RefCell::new(Vec::new())};
}
/// Thread-affine scopes are explicitly propagated when a host starts a worker.
pub struct AttributionScope {
    _not_send: std::marker::PhantomData<Rc<()>>,
}
pub fn enter_attribution(mut attribution: UsageAttribution) -> AttributionScope {
    attribution.inherit_from(&current_attribution());
    ATTRIBUTION.with(|stack| stack.borrow_mut().push(attribution));
    AttributionScope {
        _not_send: Default::default(),
    }
}
impl Drop for AttributionScope {
    fn drop(&mut self) {
        ATTRIBUTION.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}
pub fn current_attribution() -> UsageAttribution {
    ATTRIBUTION.with(|stack| stack.borrow().last().cloned().unwrap_or_default())
}
pub struct RetryScope {
    state: Arc<RequestMeasurements>,
    _not_send: std::marker::PhantomData<Rc<()>>,
}
impl RetryScope {
    pub fn enter() -> Self {
        let state = Arc::new(RequestMeasurements::default());
        RETRIES.with(|stack| stack.borrow_mut().push(state.clone()));
        Self {
            state,
            _not_send: Default::default(),
        }
    }
    pub fn count(&self) -> u32 {
        self.state.retries.load(Ordering::Relaxed)
    }
    pub fn reported_usage(&self) -> LanguageModelUsage {
        let units = self.state.units.lock().unwrap_or_else(|e| e.into_inner());
        let mut total = units.committed.clone();
        total.add(&units.retry_committed);
        total.add(&units.current);
        total
    }
    fn report(&self, report: &LanguageModelUsage) {
        merge_report(
            &mut self
                .state
                .units
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .current,
            report,
        );
    }
}
impl Drop for RetryScope {
    fn drop(&mut self) {
        RETRIES.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}
/// Call immediately before another transport attempt, after cancellable backoff.
pub fn record_retry(reason: &'static str) {
    RETRIES.with(|stack| {
        if let Some(state) = stack.borrow().last() {
            state.retries.fetch_add(1, Ordering::Relaxed);
            let mut units = state.units.lock().unwrap_or_else(|e| e.into_inner());
            let previous = std::mem::take(&mut units.current);
            units.retry_committed.add(&previous);
        }
    });
    tracing::debug!(target:"sinomo.lm.usage",reason,"retrying provider request");
}
/// Cumulative native reports may arrive before a stream fails. This request-local
/// snapshot preserves them without changing public streaming event semantics.
pub(crate) fn record_usage_report(usage: &LanguageModelUsage) {
    RETRIES.with(|stack| {
        if let Some(state) = stack.borrow().last() {
            merge_report(
                &mut state
                    .units
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .current,
                usage,
            );
        }
    });
}
pub(crate) fn merge_report(base: &mut LanguageModelUsage, report: &LanguageModelUsage) {
    macro_rules! copy {($($field:ident),*)=>{$(if report.$field.is_some(){base.$field=report.$field.clone();})*}}
    copy!(
        input_tokens,
        output_tokens,
        cache_read_input_tokens,
        cache_write_input_tokens,
        reasoning_tokens,
        generated_images,
        hosted_searches
    );
    for unit in &report.named_units {
        if let Some(old) = base
            .named_units
            .iter_mut()
            .find(|old| old.name == unit.name)
        {
            *old = unit.clone();
        } else {
            base.named_units.push(unit.clone());
        }
    }
}
/// Recording boundary for a host that already owns a provider implementation.
pub fn complete_provider<P: crate::LanguageModelProvider + ?Sized>(
    data_dir: Option<&SinomoDataDir>,
    provider: &P,
    request: &crate::LanguageModelRequest,
) -> Result<crate::LanguageModelResponse, crate::LanguageModelError> {
    let fallback = data_dir
        .is_none()
        .then(|| SinomoDataDir::from_env_or_home().ok())
        .flatten();
    let Some(data) = data_dir.or(fallback.as_ref()) else {
        tracing::warn!(target:"sinomo.lm.usage","model usage has no local data directory");
        return provider.complete(request);
    };
    let mut observation =
        RequestObservation::new(data, &request.provider, &request.model, None, request);
    if provider.info().invocation == crate::LanguageModelInvocation::Process {
        observation.unreported_retries();
    }
    let result = provider.complete(request);
    observation.finish(&result, false, None, None);
    result
}
pub(crate) fn capture_retries() -> Option<Arc<RequestMeasurements>> {
    RETRIES.with(|stack| stack.borrow().last().cloned())
}
pub(crate) fn enter_retries(state: Option<Arc<RequestMeasurements>>) -> Option<RetryScope> {
    state.map(|state| {
        RETRIES.with(|stack| stack.borrow_mut().push(state.clone()));
        RetryScope {
            state,
            _not_send: Default::default(),
        }
    })
}

impl Drop for RequestObservation<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let units = self.retries.reported_usage();
            submit_usage(
                self.data_dir,
                record(
                    self.attribution.clone(),
                    UsageEvent::ModelRequest {
                        provider: self.provider.clone(),
                        account: self.account.clone(),
                        model: self.model.clone(),
                        operation: "completion".into(),
                        status: if std::thread::panicking() {
                            UsageStatus::Failed
                        } else {
                            UsageStatus::Cancelled
                        },
                        error_class: Some("request_scope_dropped".into()),
                        retry_count: self.retry_count_known.then(|| self.retries.count()),
                        duration_ms: Some(self.started.elapsed().as_millis() as u64),
                        first_output_ms: None,
                        reported: units.reported(),
                        units,
                    },
                ),
            );
        }
    }
}

pub(crate) fn next_usage_segment() {
    RETRIES.with(|stack| {
        if let Some(state) = stack.borrow().last() {
            let mut units = state.units.lock().unwrap_or_else(|e| e.into_inner());
            let previous = std::mem::take(&mut units.current);
            units.committed.add(&previous);
        }
    });
}
pub(crate) fn logical_usage_total(report: &LanguageModelUsage) {
    RETRIES.with(|stack| {
        if let Some(state) = stack.borrow().last() {
            let mut units = state.units.lock().unwrap_or_else(|e| e.into_inner());
            units.committed = Default::default();
            units.current = report.clone();
        }
    });
}
#[cfg(test)]
mod partial_report_tests {
    use super::*;
    #[test]
    fn retries_preserve_known_failed_attempt_units_without_double_counting_stream_snapshots() {
        let scope = RetryScope::enter();
        anthropic(&serde_json::json!({"input_tokens":10,"cache_read_input_tokens":5}));
        record_retry("fixture_retry");
        openai(&serde_json::json!({"usage":{"input_tokens":20,"output_tokens":2}}));
        openai(&serde_json::json!({"usage":{"input_tokens":20,"output_tokens":2}}));
        assert_eq!(scope.count(), 1);
        let units = scope.reported_usage();
        assert_eq!(units.input_tokens, Some(35));
        assert_eq!(units.output_tokens, Some(2));
        assert_eq!(units.cache_read_input_tokens, Some(5));
        logical_usage_total(&LanguageModelUsage {
            input_tokens: Some(20),
            output_tokens: Some(2),
            ..Default::default()
        });
        assert_eq!(scope.reported_usage().input_tokens, Some(35));
    }
    #[test]
    fn initial_usage_is_retained_even_without_a_terminal_stream_event() {
        let scope = RetryScope::enter();
        anthropic(&serde_json::json!({"input_tokens":12,"cache_creation_input_tokens":3}));
        let units = scope.reported_usage();
        assert_eq!(units.input_tokens, Some(15));
        assert_eq!(units.cache_write_input_tokens, Some(3));
        assert_eq!(units.output_tokens, None);
    }
}

pub(crate) fn record_error_usage(provider: &str, body: &str) {
    if body.len() > 64 * 1024 {
        tracing::debug!(target:"sinomo.lm.usage",provider,"error usage extraction skipped: body exceeds observation budget");
        return;
    }
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return;
    };
    match provider {
        "anthropic" | "claude" => {
            if let Some(value) = value.get("usage") {
                anthropic(value);
            }
        }
        "gemini" => {
            let _ = crate::gemini::extract_gemini_usage(&value);
        }
        _ => {
            let _ = openai(value.get("response").unwrap_or(&value));
        }
    }
}
