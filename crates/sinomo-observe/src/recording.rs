//! Observe-owned durable intake, shared by in-process and native-module producers.
use super::*;
use sinomo_api::observe::recording::{self, Submission};
use sinomo_core::local::SinomoDataDir;
use std::sync::mpsc;

pub struct ObserveEventWriter {
    registration: Option<recording::Registration>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Drop for ObserveEventWriter {
    fn drop(&mut self) {
        self.registration.take();
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::warn!(target:"sinomo.observe","Observe event writer panicked during shutdown");
            }
        }
    }
}
pub fn start_event_writer(data_dir: &SinomoDataDir) -> Result<ObserveEventWriter, ObserveError> {
    let (tx, rx) = mpsc::sync_channel(4096);
    let data = data_dir.clone();
    let worker=std::thread::Builder::new().name("sinomo-observe-events".into()).spawn(move||{
        while let Ok(submission)=rx.recv(){match submission{
            Submission::Flush(ack)=>{let _=ack.send(());},
            Submission::Event(event)=>{if let Err(error)=append_observation_event(&data,event){recording::dropped("append");tracing::warn!(target:"sinomo.observe",%error,"Observe event append failed");}}
        }}
    }).map_err(|e|ObserveError::Runtime{detail:e.to_string()})?;
    Ok(ObserveEventWriter {
        registration: Some(recording::register(data_dir, tx)),
        worker: Some(worker),
    })
}
pub(crate) fn append_observation_event(
    data: &SinomoDataDir,
    mut event: EventEnvelope,
) -> Result<(), ObserveError> {
    event.origin_node_id = super::operational_origin_node_id(&data.root());
    if matches!(
        event.kind.as_str(),
        "lm.request.completed"
            | "agent.tool.call.completed"
            | "skill.activated"
            | "skill.version.published"
            | "lm.provider.quota.sampled"
    ) {
        let mut record: sinomo_api::app::usage::UsageRecord = serde_json::from_value(event.payload)
            .map_err(|e| ObserveError::Parse {
                detail: e.to_string(),
            })?;
        if record.event.kind() != event.kind.as_str() {
            return Err(ObserveError::Parse {
                detail: "usage event kind does not match its typed payload".into(),
            });
        }
        record.attribution.node = event.origin_node_id.to_string();
        if let sinomo_api::app::usage::UsageEvent::ToolCall {
            runtime,
            executing_node,
            ..
        } = &mut record.event
        {
            if executing_node.is_none() && matches!(runtime.as_str(), "agent" | "local" | "mcp") {
                *executing_node = Some(record.attribution.node.clone());
            }
        }
        usage::metrics::record(&record);
        usage::UsageLedger::new(data)
            .append(&record)
            .map_err(|detail| ObserveError::Runtime { detail })?;
        tracing::debug!(target:"sinomo.observe.usage",record_id=%record.id,event_kind=record.event.kind(),session=record.attribution.session.as_deref(),operation=record.attribution.operation.as_deref(),"usage event retained");
        return Ok(());
    }
    append_jsonl_record(
        &operational_events_path(data.root()),
        &OperationalEventRecord {
            appended_at: OffsetDateTime::now_utc(),
            severity: OperationalSeverity::Info,
            component: event.source.as_str().to_string(),
            message: event.kind.as_str().to_string(),
            operation_id: None,
            event,
        },
    )
}
pub(crate) struct ObserveAppendService;
impl Service for ObserveAppendService {
    fn name(&self) -> &'static str {
        "observe-event-intake"
    }
    fn routes(&self) -> &'static [&'static str] {
        &[recording::APPEND_ROUTE]
    }
    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::from_route_metadata(
                recording::APPEND_ROUTE,
                OBSERVE_RPC_CATEGORY,
                "local-command",
                ["observe.events.append"],
            )
            .with_concurrency_limit(8),
        ]
    }
    fn handle(
        &self,
        mut ctx: ServiceContext,
        request: sinomo_core::network::ObjectRpcRequest,
    ) -> ServiceFuture<'_> {
        Box::pin(async move {
            if !ctx.signer.is_local() {
                return Err(sinomo_core::service::ServiceError::Service(
                    "Observe producer intake requires a local process".into(),
                ));
            }
            let event: EventEnvelope = ctx.decode_request_body(&request).await?;
            let data = ctx.data_dir.clone();
            tokio::task::spawn_blocking(move || append_observation_event(&data, event))
                .await
                .map_err(|e| sinomo_core::service::ServiceError::Service(e.to_string()))?
                .map_err(service_error)?;
            ctx.respond_value(
                recording::APPEND_ROUTE,
                serde_json::json!({"accepted":true}),
            )?;
            Ok(sinomo_core::service::ServiceDisposition::Continue)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_api::app::usage::*;
    fn sample(id: &str) -> UsageRecord {
        UsageRecord {
            id: id.into(),
            timestamp_ms: 60_001,
            attribution: UsageAttribution {
                node: "untrusted-producer".into(),
                ..Default::default()
            },
            event: UsageEvent::ModelRequest {
                provider: "openai".into(),
                account: "default".into(),
                model: "gpt-4.1-mini".into(),
                operation: "completion".into(),
                status: UsageStatus::Succeeded,
                error_class: None,
                retry_count: Some(0),
                duration_ms: Some(20),
                first_output_ms: Some(3),
                reported: true,
                units: LanguageModelUsage {
                    input_tokens: Some(12),
                    ..Default::default()
                },
            },
        }
    }
    #[test]
    fn observe_writer_drains_and_retains_attributed_events_without_a_parallel_metric_store() {
        let dir = sinomo_testdir::TestDir::new("observe-usage-writer");
        let data = SinomoDataDir::new(&dir);
        let writer = start_event_writer(&data).unwrap();
        let record = sample("record");
        recording::submit(&data, record.into_envelope());
        assert!(recording::flush(&data, std::time::Duration::from_secs(5)));
        drop(writer);
        let response = usage::UsageLedger::new(&data)
            .query(&UsageQuery::default(), "node")
            .unwrap();
        assert_eq!(response.totals.model_requests, 1);
        assert_ne!(response.records[0].attribution.node, "untrusted-producer");
        assert!(!data.root().join("events/agent/usage").exists());
        let mut layout = sinomo_core::storage::StorageLayout::new();
        register_storage_paths(&mut layout).unwrap();
        assert!(layout.owner_for("observe/usage/0.jsonl").is_some());
    }
    #[test]
    fn append_failure_does_not_poison_the_producer_or_writer() {
        let dir = sinomo_testdir::TestDir::new("observe-usage-append-failure");
        let data = SinomoDataDir::new(&dir);
        fs::create_dir_all(data.root().join("observe")).unwrap();
        fs::write(data.root().join("observe/usage"), "occupied").unwrap();
        let writer = start_event_writer(&data).unwrap();
        recording::submit(&data, sample("lost").into_envelope());
        assert!(recording::flush(&data, std::time::Duration::from_secs(5)));
        fs::remove_file(data.root().join("observe/usage")).unwrap();
        recording::submit(&data, sample("retained").into_envelope());
        drop(writer);
        let records = usage::UsageLedger::new(&data)
            .query(&UsageQuery::default(), "node")
            .unwrap()
            .records;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "retained");
    }
}
