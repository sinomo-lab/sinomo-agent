//! Lifecycle-friendly component facts on one host process.
//!
//! This is deliberately a notification bus, not a distributed transaction or
//! middleware chain. Commands and queries continue to use services. Cluster
//! forwarding and durable append are explicit host adapters rather than
//! implicit behavior hidden behind `publish`.

use std::sync::Arc;
use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use sinomo_core::service::{
    Service, ServiceContext, ServiceDisposition, ServiceFuture, ServiceRegistryBuilder,
    ServiceRoute,
};
use sinomo_core::{EventEnvelope, Id, RoutingScope, Source};
use thiserror::Error;

use crate::{
    ComponentDescriptor, ComponentEventDurability, ComponentEventLocality,
    ComponentEventPublication, ComponentEventSubscription,
};

const DEFAULT_COMPONENT_EVENT_CAPACITY: usize = 1_024;

pub const COMPONENT_EVENT_INGRESS_ROUTE: &str = "/component/events/ingest";
pub const COMPONENT_EVENT_RPC_CATEGORY: &str = "component.events";
pub const COMPONENT_EVENT_INGRESS_CAPABILITY: &str = "component.events.ingest";
const COMPONENT_EVENT_INGRESS_MAX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComponentEventPublisher {
    pub component_id: String,
    pub component_version: String,
    pub artifact_id: Option<String>,
    pub artifact_generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ComponentEvent {
    pub publisher: ComponentEventPublisher,
    pub schema_id: String,
    pub schema_version: String,
    pub envelope: EventEnvelope,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ComponentEventIngressRequest {
    pub event: ComponentEvent,
    pub durability: ComponentEventDurability,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComponentEventIngressResponse {
    pub event_id: String,
    pub accepted: bool,
}

/// Optional durable append owned by the host. Implementations must append
/// before returning so transient delivery never precedes its durability claim.
pub trait ComponentEventPersistence: Send + Sync {
    fn append(&self, event: &ComponentEvent) -> Result<(), String>;
}

pub type ComponentEventTransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Cluster transport for a fully attributed event. Transports preserve the
/// event id and publisher metadata so receivers can deduplicate retries.
pub trait ComponentEventTransport: Send + Sync {
    fn publish<'a>(
        &'a self,
        event: Arc<ComponentEvent>,
        durability: ComponentEventDurability,
    ) -> ComponentEventTransportFuture<'a>;
}

#[derive(Clone)]
pub struct ComponentEventBus {
    tx: tokio::sync::broadcast::Sender<Arc<ComponentEvent>>,
    persistence: Option<Arc<dyn ComponentEventPersistence>>,
    transport: Option<Arc<dyn ComponentEventTransport>>,
    origin_node_id: Option<Id>,
}

impl std::fmt::Debug for ComponentEventBus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ComponentEventBus")
            .field("receivers", &self.tx.receiver_count())
            .field("durable", &self.persistence.is_some())
            .field("cluster_transport", &self.transport.is_some())
            .field("origin_node_id", &self.origin_node_id)
            .finish()
    }
}

impl Default for ComponentEventBus {
    fn default() -> Self {
        Self::new(DEFAULT_COMPONENT_EVENT_CAPACITY)
    }
}

impl ComponentEventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(capacity.max(1));
        Self {
            tx,
            persistence: None,
            transport: None,
            origin_node_id: None,
        }
    }

    pub fn with_persistence(mut self, persistence: Arc<dyn ComponentEventPersistence>) -> Self {
        self.persistence = Some(persistence);
        self
    }

    pub fn with_transport(mut self, transport: Arc<dyn ComponentEventTransport>) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn with_origin_node_id(mut self, origin_node_id: Id) -> Self {
        self.origin_node_id = Some(origin_node_id);
        self
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Arc<ComponentEvent>> {
        self.tx.subscribe()
    }

    pub fn subscribe_host(&self) -> tokio::sync::broadcast::Receiver<Arc<ComponentEvent>> {
        self.subscribe()
    }

    async fn publish(
        &self,
        event: Arc<ComponentEvent>,
        durability: ComponentEventDurability,
        locality: ComponentEventLocality,
    ) -> Result<usize, ComponentEventError> {
        if durability == ComponentEventDurability::Durable {
            self.persistence
                .as_ref()
                .ok_or(ComponentEventError::DurabilityUnavailable)?
                .append(&event)
                .map_err(|detail| ComponentEventError::Persistence { detail })?;
        }
        if locality == ComponentEventLocality::Cluster {
            self.transport
                .as_ref()
                .ok_or_else(|| ComponentEventError::ClusterTransportUnavailable {
                    kind: event.envelope.kind.as_str().to_string(),
                })?
                .publish(Arc::clone(&event), durability)
                .await
                .map_err(|detail| ComponentEventError::Transport { detail })?;
        }
        Ok(self.tx.send(event).unwrap_or(0))
    }

    /// Deliver an already authenticated remote event locally without
    /// forwarding it again. Durable events are appended before notification.
    pub async fn ingest_remote(
        &self,
        event: Arc<ComponentEvent>,
        durability: ComponentEventDurability,
    ) -> Result<usize, ComponentEventError> {
        if durability == ComponentEventDurability::Durable {
            self.persistence
                .as_ref()
                .ok_or(ComponentEventError::DurabilityUnavailable)?
                .append(&event)
                .map_err(|detail| ComponentEventError::Persistence { detail })?;
        }
        Ok(self.tx.send(event).unwrap_or(0))
    }

    /// Publish a host-authored local fact from synchronous host code. Cluster
    /// forwarding stays on the async path.
    pub fn publish_host_local(
        &self,
        mut event: ComponentEvent,
        durability: ComponentEventDurability,
    ) -> Result<usize, ComponentEventError> {
        if let Some(origin_node_id) = &self.origin_node_id {
            event.envelope.origin_node_id = origin_node_id.clone();
        }
        if durability == ComponentEventDurability::Durable {
            self.persistence
                .as_ref()
                .ok_or(ComponentEventError::DurabilityUnavailable)?
                .append(&event)
                .map_err(|detail| ComponentEventError::Persistence { detail })?;
        }
        Ok(self.tx.send(Arc::new(event)).unwrap_or(0))
    }
}

struct ComponentEventIngressService {
    bus: ComponentEventBus,
}

impl Service for ComponentEventIngressService {
    fn name(&self) -> &'static str {
        "component-event-ingress"
    }

    fn routes(&self) -> &'static [&'static str] {
        &[COMPONENT_EVENT_INGRESS_ROUTE]
    }

    fn route_categories(&self) -> Vec<ServiceRoute> {
        vec![
            ServiceRoute::new(COMPONENT_EVENT_INGRESS_ROUTE, COMPONENT_EVENT_RPC_CATEGORY)
                .with_auth_scope("cluster-member")
                .require_capability(COMPONENT_EVENT_INGRESS_CAPABILITY)
                .with_concurrency_limit(16),
        ]
    }

    fn handle(
        &self,
        mut context: ServiceContext,
        request: sinomo_core::network::ObjectRpcRequest,
    ) -> ServiceFuture<'_> {
        let bus = self.bus.clone();
        Box::pin(async move {
            let request: ComponentEventIngressRequest = context
                .decode_request_body_bounded(&request, COMPONENT_EVENT_INGRESS_MAX_BYTES)
                .await?;
            if let Some((signer_node_id, _)) = context.signer.cluster_identity()
                && signer_node_id != request.event.envelope.origin_node_id.as_str()
            {
                return Err(sinomo_core::service::ServiceError::Service(
                    "component event origin node does not match authenticated signer".to_string(),
                ));
            }
            let event_id = request.event.envelope.event_id.to_string();
            bus.ingest_remote(Arc::new(request.event), request.durability)
                .await
                .map_err(|error| sinomo_core::service::ServiceError::Service(error.to_string()))?;
            context.respond_value(
                COMPONENT_EVENT_INGRESS_ROUTE,
                ComponentEventIngressResponse {
                    event_id,
                    accepted: true,
                },
            )?;
            Ok(ServiceDisposition::Continue)
        })
    }
}

pub fn register_component_event_ingress_service(
    builder: ServiceRegistryBuilder,
    bus: ComponentEventBus,
) -> ServiceRegistryBuilder {
    builder.register(ComponentEventIngressService { bus })
}

#[derive(Clone, Debug)]
pub struct ComponentEventClient {
    caller: ComponentDescriptor,
    bus: ComponentEventBus,
}

impl ComponentEventClient {
    pub(crate) fn new(caller: ComponentDescriptor, bus: ComponentEventBus) -> Self {
        Self { caller, bus }
    }

    pub async fn publish(
        &self,
        publication: &ComponentEventPublication,
        mut envelope: EventEnvelope,
    ) -> Result<usize, ComponentEventError> {
        if !self
            .caller
            .event_publications
            .iter()
            .any(|declared| declared == publication)
        {
            return Err(ComponentEventError::UndeclaredPublication {
                component_id: self.caller.component_id.clone(),
                kind: publication.kind.clone(),
            });
        }
        if envelope.kind.as_str() != publication.kind {
            return Err(ComponentEventError::KindMismatch {
                declared: publication.kind.clone(),
                actual: envelope.kind.as_str().to_string(),
            });
        }
        match publication.locality {
            ComponentEventLocality::Local if envelope.routing.scope != RoutingScope::Local => {
                return Err(ComponentEventError::RoutingScopeMismatch {
                    kind: publication.kind.clone(),
                });
            }
            ComponentEventLocality::Cluster
                if !matches!(
                    envelope.routing.scope,
                    RoutingScope::TrustedNetwork | RoutingScope::DurableGlobal
                ) =>
            {
                return Err(ComponentEventError::RoutingScopeMismatch {
                    kind: publication.kind.clone(),
                });
            }
            ComponentEventLocality::Local | ComponentEventLocality::Cluster => {}
        }
        envelope.source =
            Source::new(format!("component/{}", self.caller.component_id)).map_err(|error| {
                ComponentEventError::InvalidProvenance {
                    detail: error.to_string(),
                }
            })?;
        if let Some(origin_node_id) = &self.bus.origin_node_id {
            envelope.origin_node_id = origin_node_id.clone();
        }
        self.bus
            .publish(
                Arc::new(ComponentEvent {
                    publisher: ComponentEventPublisher {
                        component_id: self.caller.component_id.clone(),
                        component_version: self.caller.component_version.clone(),
                        artifact_id: self.caller.artifact_id.clone(),
                        artifact_generation: self.caller.artifact_generation,
                    },
                    schema_id: publication.schema_id.clone(),
                    schema_version: publication.schema_version.clone(),
                    envelope,
                }),
                publication.durability,
                publication.locality,
            )
            .await
    }

    pub fn subscribe(
        &self,
        subscription: &ComponentEventSubscription,
    ) -> Result<ComponentEventReceiver, ComponentEventError> {
        if !self
            .caller
            .event_subscriptions
            .iter()
            .any(|declared| declared == subscription)
        {
            return Err(ComponentEventError::UndeclaredSubscription {
                component_id: self.caller.component_id.clone(),
                kind_pattern: subscription.kind_pattern.clone(),
            });
        }
        if subscription.locality == ComponentEventLocality::Cluster {
            return Err(ComponentEventError::ClusterTransportUnavailable {
                kind: subscription.kind_pattern.clone(),
            });
        }
        Ok(ComponentEventReceiver {
            subscription: subscription.clone(),
            rx: self.bus.subscribe(),
        })
    }
}

pub struct ComponentEventReceiver {
    subscription: ComponentEventSubscription,
    rx: tokio::sync::broadcast::Receiver<Arc<ComponentEvent>>,
}

impl ComponentEventReceiver {
    pub async fn recv(&mut self) -> Result<Arc<ComponentEvent>, ComponentEventReceiveError> {
        loop {
            let event = self.rx.recv().await.map_err(|error| match error {
                tokio::sync::broadcast::error::RecvError::Closed => {
                    ComponentEventReceiveError::Closed
                }
                tokio::sync::broadcast::error::RecvError::Lagged(skipped) => {
                    ComponentEventReceiveError::Lagged { skipped }
                }
            })?;
            if self.subscription.matches_kind(event.envelope.kind.as_str())
                && (self.subscription.schema_id == "*"
                    || self.subscription.schema_id == event.schema_id)
                && (self.subscription.schema_version == "*"
                    || self.subscription.schema_version == event.schema_version)
            {
                return Ok(event);
            }
        }
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ComponentEventError {
    #[error("component `{component_id}` did not declare publication `{kind}`")]
    UndeclaredPublication { component_id: String, kind: String },
    #[error("component `{component_id}` did not declare subscription `{kind_pattern}`")]
    UndeclaredSubscription {
        component_id: String,
        kind_pattern: String,
    },
    #[error("event kind `{actual}` does not match declared publication `{declared}`")]
    KindMismatch { declared: String, actual: String },
    #[error("local event `{kind}` must use local routing scope")]
    RoutingScopeMismatch { kind: String },
    #[error("cluster event transport is not installed for `{kind}`")]
    ClusterTransportUnavailable { kind: String },
    #[error("durable component event persistence is not installed")]
    DurabilityUnavailable,
    #[error("persist component event: {detail}")]
    Persistence { detail: String },
    #[error("transport component event: {detail}")]
    Transport { detail: String },
    #[error("invalid component event provenance: {detail}")]
    InvalidProvenance { detail: String },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ComponentEventReceiveError {
    #[error("component event bus closed")]
    Closed,
    #[error("component event receiver lagged and skipped {skipped} facts")]
    Lagged { skipped: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        Confidence, EventKind, Id, LatencyClass, PrivacyClass, RoutingMetadata, Salience,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::OffsetDateTime;

    fn event(kind: &str) -> EventEnvelope {
        EventEnvelope::new(
            Id::generate(),
            OffsetDateTime::now_utc(),
            Id::from_legacy_named("fixture-node").expect("node id"),
            Source::new("untrusted.fixture.source").expect("source"),
            EventKind::new(kind).expect("event kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(1.0).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Local,
                LatencyClass::Reactive,
                PrivacyClass::Derived,
            ),
        )
    }

    struct CountingPersistence(AtomicUsize);

    impl ComponentEventPersistence for CountingPersistence {
        fn append(&self, _event: &ComponentEvent) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct CountingTransport(AtomicUsize);

    impl ComponentEventTransport for CountingTransport {
        fn publish<'a>(
            &'a self,
            _event: Arc<ComponentEvent>,
            _durability: ComponentEventDurability,
        ) -> ComponentEventTransportFuture<'a> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn declared_local_facts_are_filtered_and_carry_host_provenance() {
        let publication =
            ComponentEventPublication::new("fixture.job.completed", "sinomo.fixture.job", "1.0.0");
        let subscription =
            ComponentEventSubscription::new("fixture.job.*", "sinomo.fixture.job", "1.0.0");
        let bus = ComponentEventBus::new(8);
        let publisher = ComponentEventClient::new(
            ComponentDescriptor::new("fixture.publisher", "2.0.0")
                .publish_event(publication.clone()),
            bus.clone(),
        );
        let subscriber = ComponentEventClient::new(
            ComponentDescriptor::new("fixture.subscriber", "1.0.0")
                .subscribe_event(subscription.clone()),
            bus,
        );
        let mut receiver = subscriber
            .subscribe(&subscription)
            .expect("declared subscription");

        assert_eq!(
            publisher
                .publish(&publication, event("fixture.job.completed"))
                .await
                .expect("publish fact"),
            1
        );
        let received = receiver.recv().await.expect("receive fact");
        assert_eq!(received.publisher.component_id, "fixture.publisher");
        assert_eq!(received.schema_id, "sinomo.fixture.job");
        assert_eq!(
            received.envelope.source.as_str(),
            "component/fixture.publisher"
        );
    }

    #[tokio::test]
    async fn undeclared_publication_fails_closed() {
        let publication =
            ComponentEventPublication::new("fixture.job.completed", "sinomo.fixture.job", "1.0.0");
        let publisher = ComponentEventClient::new(
            ComponentDescriptor::new("fixture.publisher", "2.0.0"),
            ComponentEventBus::default(),
        );
        assert!(matches!(
            publisher
                .publish(&publication, event("fixture.job.completed"))
                .await,
            Err(ComponentEventError::UndeclaredPublication { .. })
        ));
    }

    #[tokio::test]
    async fn durable_cluster_publication_uses_both_adapters_and_host_origin() {
        let persistence = Arc::new(CountingPersistence(AtomicUsize::new(0)));
        let transport = Arc::new(CountingTransport(AtomicUsize::new(0)));
        let host_node = Id::from_legacy_named("host-node").expect("host node");
        let bus = ComponentEventBus::new(8)
            .with_persistence(persistence.clone())
            .with_transport(transport.clone())
            .with_origin_node_id(host_node.clone());
        let publication =
            ComponentEventPublication::new("fixture.job.completed", "sinomo.fixture.job", "1.0.0")
                .cluster_visible()
                .durable();
        let publisher = ComponentEventClient::new(
            ComponentDescriptor::new("fixture.publisher", "1.0.0")
                .publish_event(publication.clone()),
            bus.clone(),
        );
        let mut receiver = bus.subscribe_host();
        let mut envelope = event("fixture.job.completed");
        envelope.routing.scope = RoutingScope::TrustedNetwork;

        publisher
            .publish(&publication, envelope)
            .await
            .expect("publish durable cluster event");
        let received = receiver.recv().await.expect("local delivery");
        assert_eq!(received.envelope.origin_node_id, host_node);
        assert_eq!(persistence.0.load(Ordering::SeqCst), 1);
        assert_eq!(transport.0.load(Ordering::SeqCst), 1);
    }
}
