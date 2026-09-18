//! Hosted daemon event subscriptions and the in-process publication hub.

use std::sync::{Arc, RwLock};

use sinomo_core::network::{
    ObjectFrameIoOptions, ObjectRpcBodySource, ObjectRpcRequest, ObjectRpcResponse,
    write_object_frame_async,
};
use sinomo_core::service::{
    Service, ServiceBodyMode, ServiceContext, ServiceDisposition, ServiceError, ServiceFuture,
    ServiceRegistryBuilder, ServiceRoute,
};
#[cfg(test)]
use sinomo_core::{
    Confidence, EventKind, LatencyClass, PrivacyClass, RoutingMetadata, RoutingScope, Salience,
    Source,
};
use sinomo_core::{EventEnvelope, Id};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::broadcast;

#[cfg(test)]
use super::DFS_NAMESPACE_CHANGED_EVENT_KIND;
use super::{
    DAEMON_EVENT_SUBSCRIBE_ROUTE, DAEMON_RPC_CATEGORY, DaemonEventStreamItem,
    DaemonEventSubscribeRequest, DaemonEventSubscribeResponse,
};

const DAEMON_EVENT_BUFFER_CAPACITY: usize = 1_024;
const DAEMON_EVENT_STREAM_BODY_BUFFER_BYTES: usize = 64 * 1_024;
const DAEMON_EVENT_MAX_FILTERS: usize = 64;

pub trait DaemonEventMirror: Send + Sync {
    fn publish(&self, event: EventEnvelope);
}

#[derive(Clone)]
pub struct DaemonEventHub {
    sender: broadcast::Sender<EventEnvelope>,
    mirror: Arc<RwLock<Option<Arc<dyn DaemonEventMirror>>>>,
}

impl std::fmt::Debug for DaemonEventHub {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonEventHub")
            .field("receivers", &self.sender.receiver_count())
            .field(
                "mirrored",
                &self
                    .mirror
                    .read()
                    .map(|mirror| mirror.is_some())
                    .unwrap_or(false),
            )
            .finish()
    }
}

impl DaemonEventHub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(DAEMON_EVENT_BUFFER_CAPACITY);
        Self {
            sender,
            mirror: Arc::new(RwLock::new(None)),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.sender.subscribe()
    }

    pub fn publish(&self, event: EventEnvelope) {
        // No receivers is a normal state. The durable DFS namespace remains
        // the source of truth and subscribers load a snapshot after Ready.
        let _ = self.sender.send(event.clone());
        if let Ok(mirror) = self.mirror.read()
            && let Some(mirror) = mirror.as_ref()
        {
            mirror.publish(event);
        }
    }

    pub fn set_mirror(&self, mirror: Arc<dyn DaemonEventMirror>) {
        if let Ok(mut installed) = self.mirror.write() {
            *installed = Some(mirror);
        }
    }
}

impl Default for DaemonEventHub {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DaemonEventService {
    runtime: Option<tokio::runtime::Handle>,
    hub: Arc<DaemonEventHub>,
}

impl DaemonEventService {
    pub fn new(runtime: Option<tokio::runtime::Handle>, hub: Arc<DaemonEventHub>) -> Self {
        Self { runtime, hub }
    }
}

impl Service for DaemonEventService {
    fn name(&self) -> &'static str {
        "daemon-events"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[DAEMON_EVENT_SUBSCRIBE_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(DAEMON_EVENT_SUBSCRIBE_ROUTE, DAEMON_RPC_CATEGORY)
                .with_auth_scope("local-command")
                .require_capability("daemon.events.subscribe")
                .with_response_body_mode(ServiceBodyMode::Required),
        ]
    }

    fn handle(&self, mut ctx: ServiceContext, request: ObjectRpcRequest) -> ServiceFuture<'_> {
        let runtime = self.runtime.clone();
        let hub = Arc::clone(&self.hub);
        Box::pin(async move {
            let runtime = runtime.ok_or_else(|| {
                ServiceError::Service(
                    "daemon event subscriptions require the daemon-owned Tokio runtime".to_string(),
                )
            })?;
            let filters: DaemonEventSubscribeRequest = ctx.decode_request_body(&request).await?;
            validate_filters(&filters)?;

            let subscription_id = Id::generate().to_string();
            let mut receiver = hub.subscribe();
            let (reader, mut writer) = tokio::io::duplex(DAEMON_EVENT_STREAM_BODY_BUFFER_BYTES);
            let producer_subscription_id = subscription_id.clone();
            let producer = runtime.spawn(async move {
                if write_stream_item(
                    &mut writer,
                    &DaemonEventStreamItem::Ready {
                        subscription_id: producer_subscription_id,
                    },
                )
                .await
                .is_err()
                {
                    return;
                }
                loop {
                    let item = match receiver.recv().await {
                        Ok(event) => {
                            if !event_matches(&filters, &event) {
                                continue;
                            }
                            DaemonEventStreamItem::Event { event }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            DaemonEventStreamItem::Lagged { skipped }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    };
                    if write_stream_item(&mut writer, &item).await.is_err() {
                        break;
                    }
                }
                let _ = writer.shutdown().await;
            });
            let body = ObjectRpcBodySource::async_reader(reader, None)
                .with_lifetime_guard(producer)
                .map_err(ServiceError::Network)?;
            ctx.respond_with_body(
                ObjectRpcResponse::encode_body(
                    DAEMON_EVENT_SUBSCRIBE_ROUTE,
                    &DaemonEventSubscribeResponse { subscription_id },
                )?,
                body,
            )?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_daemon_event_service(
    builder: ServiceRegistryBuilder,
    runtime: Option<tokio::runtime::Handle>,
    hub: Arc<DaemonEventHub>,
) -> ServiceRegistryBuilder {
    builder.register(DaemonEventService::new(runtime, hub))
}

fn validate_filters(filters: &DaemonEventSubscribeRequest) -> Result<(), ServiceError> {
    if filters.event_kind_prefixes.len() > DAEMON_EVENT_MAX_FILTERS
        || filters.path_prefixes.len() > DAEMON_EVENT_MAX_FILTERS
    {
        return Err(ServiceError::Service(format!(
            "daemon event subscriptions accept at most {DAEMON_EVENT_MAX_FILTERS} filters"
        )));
    }
    if filters
        .event_kind_prefixes
        .iter()
        .any(|prefix| prefix.trim().is_empty())
    {
        return Err(ServiceError::Service(
            "daemon event kind prefixes cannot be empty".to_string(),
        ));
    }
    if filters
        .path_prefixes
        .iter()
        .any(|prefix| !prefix.starts_with('/'))
    {
        return Err(ServiceError::Service(
            "daemon event path prefixes must be absolute DFS paths".to_string(),
        ));
    }
    Ok(())
}

fn event_matches(filters: &DaemonEventSubscribeRequest, event: &EventEnvelope) -> bool {
    let kind_matches = filters.event_kind_prefixes.is_empty()
        || filters
            .event_kind_prefixes
            .iter()
            .any(|prefix| event.kind.as_str().starts_with(prefix));
    kind_matches
        && (filters.path_prefixes.is_empty()
            || event_paths(event).any(|path| {
                filters
                    .path_prefixes
                    .iter()
                    .any(|prefix| path_matches_prefix(path, prefix))
            }))
}

fn event_paths(event: &EventEnvelope) -> impl Iterator<Item = &str> {
    event
        .payload
        .get("paths")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

async fn write_stream_item(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    item: &DaemonEventStreamItem,
) -> Result<(), sinomo_core::network::NetworkError> {
    let response = ObjectRpcResponse::encode_body(DAEMON_EVENT_SUBSCRIBE_ROUTE, item)?;
    write_object_frame_async(writer, &response, ObjectFrameIoOptions::none()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingMirror(AtomicUsize);

    impl DaemonEventMirror for CountingMirror {
        fn publish(&self, _event: EventEnvelope) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn namespace_event(path: &str) -> EventEnvelope {
        EventEnvelope::new(
            Id::generate(),
            time::OffsetDateTime::now_utc(),
            Id::generate(),
            Source::new("daemon.dfs").expect("source"),
            EventKind::new(DFS_NAMESPACE_CHANGED_EVENT_KIND).expect("kind"),
            Salience::new(0.3).expect("salience"),
            Confidence::new(1.0).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Local,
                LatencyClass::Reactive,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({ "paths": [path] }))
    }

    #[test]
    fn path_filters_match_only_complete_namespace_prefixes() {
        let filters =
            DaemonEventSubscribeRequest::for_path_prefix("/services/sinomo-agent/channels");
        assert!(event_matches(
            &filters,
            &namespace_event("/services/sinomo-agent/channels/chat/snapshot.json")
        ));
        assert!(!event_matches(
            &filters,
            &namespace_event("/services/sinomo-agent/channels-old/chat.json")
        ));
    }

    #[tokio::test]
    async fn hub_reports_lag_instead_of_silently_dropping_events() {
        let (sender, _) = broadcast::channel(1);
        let hub = DaemonEventHub {
            sender,
            mirror: Arc::new(RwLock::new(None)),
        };
        let mut receiver = hub.subscribe();
        hub.publish(namespace_event("/first"));
        hub.publish(namespace_event("/second"));
        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(1))
        ));
    }

    #[test]
    fn compatibility_hub_mirrors_into_the_component_event_path() {
        let hub = DaemonEventHub::new();
        let mirror = Arc::new(CountingMirror(AtomicUsize::new(0)));
        hub.set_mirror(mirror.clone());
        hub.publish(namespace_event("/services/example"));
        assert_eq!(mirror.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stream_items_are_standard_object_rpc_frames() {
        let (mut reader, mut writer) = tokio::io::duplex(4_096);
        let item = DaemonEventStreamItem::Ready {
            subscription_id: Id::generate().to_string(),
        };
        write_stream_item(&mut writer, &item)
            .await
            .expect("write stream item");
        writer.shutdown().await.expect("close writer");

        let response: ObjectRpcResponse = sinomo_core::network::read_object_frame_async(
            &mut reader,
            ObjectFrameIoOptions::none(),
        )
        .await
        .expect("read object frame");
        assert_eq!(response.route, DAEMON_EVENT_SUBSCRIBE_ROUTE);
        assert_eq!(
            response
                .decode_body::<DaemonEventStreamItem>()
                .expect("decode stream item"),
            item
        );
    }
}
