//! Realtime responses are provider invocations too. Observe only identity, timing,
//! status and native usage; never retain the audio, transcript or tool payload.
use crate::usage;
use sinomo_runtime::{api::app::usage::*, local::SinomoDataDir};
use std::{
    collections::{BTreeMap, VecDeque},
    time::Instant,
};
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimeUsageContext {
    pub data_dir: SinomoDataDir,
    pub attribution: UsageAttribution,
    pub account: String,
}
pub(crate) struct RealtimeUsageRecorder {
    context: RealtimeUsageContext,
    provider: String,
    model: String,
    stream_id: String,
    active: BTreeMap<String, (Option<Instant>, Option<u64>)>,
    completed: VecDeque<String>,
    cancelled: bool,
    local_turn: Option<String>,
}
impl RealtimeUsageRecorder {
    pub fn new(context: RealtimeUsageContext, provider: String, model: String) -> Self {
        Self {
            context,
            provider,
            model,
            stream_id: sinomo_runtime::core::Id::generate().to_string(),
            active: Default::default(),
            completed: Default::default(),
            cancelled: false,
            local_turn: None,
        }
    }
    pub fn cancelled(&mut self) {
        self.cancelled = true;
    }
    pub fn observe(&mut self, value: &serde_json::Value) {
        let kind = value["type"].as_str().unwrap_or_default();
        let id = value
            .pointer("/response/id")
            .or_else(|| value.get("response_id"))
            .and_then(serde_json::Value::as_str);
        if id.is_none() && self.provider == "minicpm-o" {
            if matches!(kind, "response.audio.delta" | "response.output_audio.delta") {
                let id = self
                    .local_turn
                    .get_or_insert_with(|| sinomo_runtime::core::Id::generate().to_string())
                    .clone();
                self.active.entry(id.clone()).or_insert((None, None));
                if value["end_of_turn"].as_bool() == Some(true) {
                    self.active.remove(&id);
                    self.emit(
                        &id,
                        UsageStatus::Succeeded,
                        None,
                        None,
                        usage::openai(value),
                        None,
                    );
                    self.local_turn = None;
                }
            } else if kind == "response.done" {
                if let Some(id) = self.local_turn.take() {
                    let (started, first) = self.active.remove(&id).unwrap_or((None, None));
                    self.emit(
                        &id,
                        UsageStatus::Succeeded,
                        started,
                        first,
                        usage::openai(value),
                        None,
                    );
                }
            }
            return;
        }
        let Some(id) = id else {
            return;
        };
        if self.completed.iter().any(|old| old == id) {
            return;
        }
        if kind == "response.created" {
            if self.active.len() >= 128 {
                if let Some(key) = self.active.keys().next().cloned() {
                    if let Some((started, first)) = self.active.remove(&key) {
                        self.emit(
                            &key,
                            UsageStatus::Failed,
                            started,
                            first,
                            None,
                            Some("response_tracking_limit".into()),
                        );
                    }
                }
            }
            self.active
                .entry(id.into())
                .or_insert((Some(Instant::now()), None));
            return;
        }
        if kind.ends_with(".delta") {
            if let Some((started, first)) = self.active.get_mut(id) {
                if let Some(started) = started {
                    first.get_or_insert(started.elapsed().as_millis() as u64);
                }
            }
            return;
        }
        if kind != "response.done" {
            return;
        }
        let (started, first) = self.active.remove(id).unwrap_or((None, None));
        let response = &value["response"];
        let status = match response["status"].as_str() {
            Some("cancelled") => UsageStatus::Cancelled,
            Some("failed") => UsageStatus::Failed,
            _ => UsageStatus::Succeeded,
        };
        self.emit(
            id,
            status,
            started,
            first,
            usage::openai(response),
            (status == UsageStatus::Failed).then(|| "provider".into()),
        );
        self.completed.push_back(id.into());
        if self.completed.len() > 1024 {
            self.completed.pop_front();
        }
    }
    fn emit(
        &self,
        id: &str,
        status: UsageStatus,
        started: Option<Instant>,
        first: Option<u64>,
        units: Option<LanguageModelUsage>,
        error_class: Option<String>,
    ) {
        let units = units.unwrap_or_default();
        let mut attribution = self.context.attribution.clone();
        attribution.purpose = "realtime_response".into();
        let mut record = usage::record(
            attribution,
            UsageEvent::ModelRequest {
                provider: self.provider.clone(),
                account: self.context.account.clone(),
                model: self.model.clone(),
                operation: "realtime_response".into(),
                status,
                error_class,
                retry_count: Some(0),
                duration_ms: started.map(|started| started.elapsed().as_millis() as u64),
                first_output_ms: first,
                reported: units.reported(),
                units,
            },
        );
        record.id = sinomo_runtime::core::Id::derived(
            "realtime-usage",
            format!(
                "{}:{}:{}:{id}",
                self.stream_id, self.provider, self.context.account
            ),
        )
        .to_string();
        usage::submit_usage(&self.context.data_dir, record);
    }
}
impl Drop for RealtimeUsageRecorder {
    fn drop(&mut self) {
        for (id, (started, first)) in std::mem::take(&mut self.active) {
            self.emit(
                &id,
                if self.cancelled {
                    UsageStatus::Cancelled
                } else {
                    UsageStatus::Failed
                },
                started,
                first,
                None,
                Some("stream_closed".into()),
            );
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn realtime_terminal_usage_is_recorded_once_and_shutdown_retains_cancelled_calls() {
        let dir = sinomo_testdir::TestDir::new("usage-realtime");
        let data = SinomoDataDir::new(&dir);
        let (tx, rx) = std::sync::mpsc::sync_channel(8);
        let _writer = sinomo_runtime::api::observe::recording::register(&data, tx);
        let mut recorder = RealtimeUsageRecorder::new(
            RealtimeUsageContext {
                data_dir: data,
                attribution: UsageAttribution {
                    chat: Some("conversation".into()),
                    ..Default::default()
                },
                account: "work".into(),
            },
            "openai".into(),
            "realtime".into(),
        );
        recorder.observe(&serde_json::json!({"type":"response.created","response":{"id":"r1"}}));
        let done = serde_json::json!({"type":"response.done","response":{"id":"r1","status":"completed","usage":{"input_tokens":20,"output_tokens":5}}});
        recorder.observe(&done);
        recorder.observe(&done);
        recorder.observe(&serde_json::json!({"type":"response.created","response":{"id":"r2"}}));
        recorder.cancelled();
        drop(recorder);
        let records = rx
            .try_iter()
            .filter_map(|r| match r {
                sinomo_runtime::api::observe::recording::Submission::Event(e) => {
                    serde_json::from_value::<UsageRecord>(e.payload).ok()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].attribution.chat.as_deref(), Some("conversation"));
        assert!(matches!(
            records[1].event,
            UsageEvent::ModelRequest {
                status: UsageStatus::Cancelled,
                ..
            }
        ));
    }
}

pub(crate) struct RealtimeConnectionObservation {
    context: RealtimeUsageContext,
    provider: String,
    model: String,
    started: Instant,
    finished: bool,
}
impl RealtimeConnectionObservation {
    pub fn new(context: RealtimeUsageContext, provider: String, model: String) -> Self {
        Self {
            context,
            provider,
            model,
            started: Instant::now(),
            finished: false,
        }
    }
    pub fn finish(&mut self, status: UsageStatus) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut attribution = self.context.attribution.clone();
        attribution.purpose = "realtime_connect".into();
        usage::submit_usage(
            &self.context.data_dir,
            usage::record(
                attribution,
                UsageEvent::ModelRequest {
                    provider: self.provider.clone(),
                    account: self.context.account.clone(),
                    model: self.model.clone(),
                    operation: "realtime_connect".into(),
                    status,
                    error_class: (status != UsageStatus::Succeeded).then(|| "connection".into()),
                    retry_count: Some(0),
                    duration_ms: Some(self.started.elapsed().as_millis() as u64),
                    first_output_ms: None,
                    reported: false,
                    units: Default::default(),
                },
            ),
        );
    }
}
impl Drop for RealtimeConnectionObservation {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(UsageStatus::Cancelled);
        }
    }
}
