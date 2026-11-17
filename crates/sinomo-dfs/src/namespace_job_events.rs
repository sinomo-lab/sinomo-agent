//! Typed events of namespace jobs that work on other shards waits on.
//!
//! A shard's conversion reads every other shard's contributions to its counts
//! and does not accept them while a Running job of another shard touches it
//! (`sinomo-dfs-migration`). Two facts about such a job change that answer,
//! and neither changes a KV key, so the job's coordinator announces each:
//!
//! - `dfs.namespace.job.parked`: a step of the job was refused until the
//!   storage of the touched shards is Active. The job stays Running at that
//!   step, and a reader of its coordinator's tables sees it waiting there.
//! - `dfs.namespace.job.completed`: the job left Running, Completed or Failed,
//!   while a touched shard was converting. The touched shards are the
//!   converting owners of its root and target paths and of their parents,
//!   and the shards it last parked on.
//!
//! Each event names its filesystem, the coordinator shard (the owner of the
//! job's root path), the job and the touched shards, in its payload and as
//! routing hints. It carries no authority: a woken reader reads the
//! coordinator's tables again.
//!
//! **Delivery.** The coordinator's node hands each event to the listeners
//! registered on that node at once, and publishes it on the component event
//! bus as a cluster-visible event. Each node's DFS component hands the events
//! other nodes published to its own listeners. Delivery is best effort and
//! nothing is retried on a timer: every later refusal of the parked step
//! announces it again, and a lost end leaves the waiter to its other wakes.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::{DfsFileSystemId, DfsMetadataShardId, DfsNamespaceJobStatus};

pub const DFS_NAMESPACE_JOB_PARKED_EVENT_KIND: &str = "dfs.namespace.job.parked";
pub const DFS_NAMESPACE_JOB_COMPLETED_EVENT_KIND: &str = "dfs.namespace.job.completed";
pub const DFS_NAMESPACE_JOB_EVENT_SCHEMA_ID: &str = "sinomo.dfs.namespace-job";
pub const DFS_NAMESPACE_JOB_EVENT_SCHEMA_VERSION: &str = "1.0.0";

/// Events waiting for this node's publisher. A full queue drops the event: it
/// is a wake, and the next refusal of a parked step announces it again.
const DFS_NAMESPACE_JOB_EVENT_QUEUE: usize = 1024;

/// Which fact a [`DfsNamespaceJobEvent`] announces.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNamespaceJobEventKind {
    Parked,
    Completed,
}

impl DfsNamespaceJobEventKind {
    pub fn event_kind(self) -> &'static str {
        match self {
            Self::Parked => DFS_NAMESPACE_JOB_PARKED_EVENT_KIND,
            Self::Completed => DFS_NAMESPACE_JOB_COMPLETED_EVENT_KIND,
        }
    }
}

/// One announced fact about a namespace job.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DfsNamespaceJobEvent {
    pub kind: DfsNamespaceJobEventKind,
    pub file_system_id: DfsFileSystemId,
    /// The shard whose primary coordinates the job: the owner of its root
    /// path.
    pub coordinator_shard_id: DfsMetadataShardId,
    pub job_id: String,
    /// Running for a park; Completed or Failed for an end.
    pub status: DfsNamespaceJobStatus,
    /// A park: the shards whose storage the refused step waits for. An end:
    /// the converting shards the job touched.
    pub touched_shard_ids: Vec<DfsMetadataShardId>,
}

type Listener = Arc<dyn Fn(&DfsNamespaceJobEvent) + Send + Sync>;

static LISTENERS: Mutex<BTreeMap<NodeId, BTreeMap<u64, Listener>>> = Mutex::new(BTreeMap::new());
static NEXT_LISTENER: AtomicU64 = AtomicU64::new(0);
static PUBLISHERS: Mutex<BTreeMap<NodeId, tokio::sync::mpsc::Sender<DfsNamespaceJobEvent>>> =
    Mutex::new(BTreeMap::new());

/// A listener registered with [`listen_for_namespace_job_events`]; dropping
/// it unregisters the listener.
#[must_use = "the listener is unregistered when this is dropped"]
pub struct DfsNamespaceJobEventListener {
    node: NodeId,
    id: u64,
}

impl Drop for DfsNamespaceJobEventListener {
    fn drop(&mut self) {
        let mut listeners = LISTENERS.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(registered) = listeners.get_mut(&self.node) {
            registered.remove(&self.id);
            if registered.is_empty() {
                listeners.remove(&self.node);
            }
        }
    }
}

/// Call `listener` for every namespace job event `node` announces or
/// receives from another node. The listener runs on the announcing thread
/// and must only wake work.
pub fn listen_for_namespace_job_events(
    node: &NodeId,
    listener: impl Fn(&DfsNamespaceJobEvent) + Send + Sync + 'static,
) -> DfsNamespaceJobEventListener {
    let id = NEXT_LISTENER.fetch_add(1, Ordering::Relaxed);
    LISTENERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .entry(node.clone())
        .or_default()
        .insert(id, Arc::new(listener));
    DfsNamespaceJobEventListener {
        node: node.clone(),
        id,
    }
}

fn deliver_locally(node: &NodeId, event: &DfsNamespaceJobEvent) {
    let listeners = LISTENERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(node)
        .map(|listeners| listeners.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    for listener in listeners {
        listener(event);
    }
}

/// Announce `event` from `node`: to its own listeners now, and to the other
/// nodes through its DFS component's publisher when one runs.
pub(crate) fn announce_namespace_job_event(node: &NodeId, event: DfsNamespaceJobEvent) {
    deliver_locally(node, &event);
    let publisher = PUBLISHERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(node)
        .cloned();
    if let Some(publisher) = publisher
        && let Err(error) = publisher.try_send(event)
    {
        tracing::debug!(target: "sinomo.dfs", %error,
            "namespace job event dropped before cluster publication");
    }
}

struct PublisherRegistration(NodeId);

impl Drop for PublisherRegistration {
    fn drop(&mut self) {
        PUBLISHERS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.0);
    }
}

pub(crate) fn namespace_job_event_publication(
    kind: DfsNamespaceJobEventKind,
) -> sinomo_module::ComponentEventPublication {
    sinomo_module::ComponentEventPublication::new(
        kind.event_kind(),
        DFS_NAMESPACE_JOB_EVENT_SCHEMA_ID,
        DFS_NAMESPACE_JOB_EVENT_SCHEMA_VERSION,
    )
    .cluster_visible()
}

/// Every namespace job event, as this node receives it: its own and those
/// other nodes forward to it.
pub(crate) fn namespace_job_event_subscription() -> sinomo_module::ComponentEventSubscription {
    sinomo_module::ComponentEventSubscription::new(
        "dfs.namespace.job.*",
        DFS_NAMESPACE_JOB_EVENT_SCHEMA_ID,
        DFS_NAMESPACE_JOB_EVENT_SCHEMA_VERSION,
    )
}

fn routing_hint(label: &str, value: &str) -> Option<sinomo_core::RoutingHint> {
    sinomo_core::RoutingHint::new(format!("{label}:{value}")).ok()
}

fn envelope(
    local: &NodeId,
    event: &DfsNamespaceJobEvent,
) -> Result<sinomo_core::EventEnvelope, String> {
    let mut routing = sinomo_core::RoutingMetadata::new(
        sinomo_core::RoutingScope::TrustedNetwork,
        sinomo_core::LatencyClass::Background,
        sinomo_core::PrivacyClass::Derived,
    );
    let hints = [
        routing_hint("dfs.filesystem", &event.file_system_id.to_string()),
        routing_hint("dfs.coordinator-shard", &event.coordinator_shard_id.0),
        routing_hint("dfs.namespace-job", &event.job_id),
    ]
    .into_iter()
    .chain(
        event
            .touched_shard_ids
            .iter()
            .map(|shard| routing_hint("dfs.touched-shard", &shard.0)),
    );
    for hint in hints.flatten() {
        routing = routing.with_hint(hint);
    }
    let mut envelope = sinomo_core::EventEnvelope::new(
        sinomo_core::Id::generate(),
        time::OffsetDateTime::now_utc(),
        local.clone(),
        sinomo_core::Source::new("component/sinomo-dfs").map_err(|error| error.to_string())?,
        sinomo_core::EventKind::new(event.kind.event_kind()).map_err(|error| error.to_string())?,
        sinomo_core::Salience::new(0.1).map_err(|error| error.to_string())?,
        sinomo_core::Confidence::new(1.0).map_err(|error| error.to_string())?,
        routing,
    )
    .with_payload(serde_json::to_value(event).map_err(|error| error.to_string())?);
    // Evidence: the coordinator's job row the event reports on.
    envelope.evidence.refs.push(sinomo_core::EvidenceRef::new(
        format!(
            "sinomo-dfs:namespace-job/{}/{}/{}",
            event.file_system_id, event.coordinator_shard_id.0, event.job_id
        ),
        sinomo_core::EvidenceKind::Other,
    ));
    Ok(envelope)
}

/// Publish the events `local` announces to the other nodes, and hand the
/// events other nodes publish to `local`'s listeners, until `shutdown`.
pub(crate) async fn run_namespace_job_events(
    local: NodeId,
    client: sinomo_module::ComponentEventClient,
    mut received: sinomo_module::ComponentEventReceiver,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let (sender, mut outbound) = tokio::sync::mpsc::channel(DFS_NAMESPACE_JOB_EVENT_QUEUE);
    PUBLISHERS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(local.clone(), sender);
    let _registration = PublisherRegistration(local.clone());
    loop {
        if *shutdown.borrow() {
            return;
        }
        tokio::select! {
            Some(event) = outbound.recv() => {
                let published = match envelope(&local, &event) {
                    Ok(envelope) => client
                        .publish(&namespace_job_event_publication(event.kind), envelope)
                        .await
                        .map(|_| ())
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                };
                if let Err(error) = published {
                    tracing::debug!(target: "sinomo.dfs", %error, job_id = %event.job_id,
                        "namespace job event was not published to the cluster");
                }
            }
            event = received.recv() => match event {
                Ok(event) if event.envelope.origin_node_id != local => {
                    match serde_json::from_value::<DfsNamespaceJobEvent>(event.envelope.payload.clone()) {
                        Ok(job_event) if job_event.kind.event_kind() == event.envelope.kind.as_str() => {
                            deliver_locally(&local, &job_event);
                        }
                        Ok(_) => tracing::debug!(target: "sinomo.dfs",
                            "namespace job event payload names another kind"),
                        Err(error) => tracing::debug!(target: "sinomo.dfs", %error,
                            "namespace job event payload is not readable"),
                    }
                }
                // This node's own events reached its listeners when announced.
                Ok(_) => {}
                Err(sinomo_module::ComponentEventReceiveError::Lagged { skipped }) => {
                    tracing::debug!(target: "sinomo.dfs", skipped,
                        "namespace job event receiver lagged");
                }
                Err(sinomo_module::ComponentEventReceiveError::Closed) => return,
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: DfsNamespaceJobEventKind) -> DfsNamespaceJobEvent {
        DfsNamespaceJobEvent {
            kind,
            file_system_id: crate::sifs_file_system_id(),
            coordinator_shard_id: DfsMetadataShardId::new("services"),
            job_id: "job-touching-users".into(),
            status: DfsNamespaceJobStatus::Running,
            touched_shard_ids: vec![DfsMetadataShardId::new("users")],
        }
    }

    #[test]
    fn an_announced_event_reaches_only_its_nodes_listeners_while_registered() {
        let announcing = NodeId::from_legacy_named("job-events-announcing").unwrap();
        let other = NodeId::from_legacy_named("job-events-other").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = |seen: &Arc<Mutex<Vec<(String, DfsNamespaceJobEvent)>>>, label: &str| {
            let (seen, label) = (Arc::clone(seen), label.to_owned());
            move |event: &DfsNamespaceJobEvent| {
                seen.lock().unwrap().push((label.clone(), event.clone()));
            }
        };
        let listener = listen_for_namespace_job_events(&announcing, record(&seen, "announcing"));
        let _other = listen_for_namespace_job_events(&other, record(&seen, "other"));
        announce_namespace_job_event(&announcing, event(DfsNamespaceJobEventKind::Parked));
        assert_eq!(
            *seen.lock().unwrap(),
            vec![("announcing".into(), event(DfsNamespaceJobEventKind::Parked))]
        );
        drop(listener);
        announce_namespace_job_event(&announcing, event(DfsNamespaceJobEventKind::Completed));
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "a dropped listener hears nothing"
        );
    }

    /// Records what the host's cluster transport would forward.
    #[derive(Default)]
    struct RecordedTransport(Mutex<Vec<Arc<sinomo_module::ComponentEvent>>>);

    impl sinomo_module::ComponentEventTransport for RecordedTransport {
        fn publish<'a>(
            &'a self,
            event: Arc<sinomo_module::ComponentEvent>,
            _durability: sinomo_module::ComponentEventDurability,
        ) -> sinomo_module::ComponentEventTransportFuture<'a> {
            self.0.lock().unwrap().push(event);
            Box::pin(async { Ok(()) })
        }
    }

    /// The component task forwards its node's announcements to the cluster
    /// transport, and hands another node's event, as the host's ingress
    /// delivers it, to its node's listeners once; its own events, which its
    /// listeners already heard, are not handed to them again.
    #[test]
    fn the_component_forwards_its_nodes_events_and_delivers_other_nodes_events() {
        let local = NodeId::from_legacy_named("job-events-component-local").unwrap();
        let remote = NodeId::from_legacy_named("job-events-component-remote").unwrap();
        let root = sinomo_testdir::TestDir::new("namespace-job-events-component");
        let transport = Arc::new(RecordedTransport::default());
        let bus = sinomo_module::ComponentEventBus::default()
            .with_origin_node_id(local.clone())
            .with_transport(transport.clone());
        let descriptor = sinomo_module::ComponentDescriptor::new("sinomo-dfs", "test")
            .publish_event(namespace_job_event_publication(
                DfsNamespaceJobEventKind::Parked,
            ))
            .publish_event(namespace_job_event_publication(
                DfsNamespaceJobEventKind::Completed,
            ))
            .subscribe_event(namespace_job_event_subscription());
        let context = sinomo_module::ComponentContext::new(Arc::new(
            sinomo_core::local::SinomoDataDir::new(root.path()),
        ))
        .with_event_bus(bus.clone());
        let client = context.event_client(&descriptor).unwrap();
        let received = client
            .subscribe(&namespace_job_event_subscription())
            .unwrap();
        let (heard_tx, mut heard) = tokio::sync::mpsc::unbounded_channel();
        let _listener = listen_for_namespace_job_events(&local, move |event| {
            let _ = heard_tx.send(event.clone());
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (stop, shutdown) = tokio::sync::watch::channel(false);
            let task = tokio::spawn(run_namespace_job_events(
                local.clone(),
                client,
                received,
                shutdown,
            ));
            // Let the task register its publisher.
            while !PUBLISHERS.lock().unwrap().contains_key(&local) {
                tokio::task::yield_now().await;
            }
            let own = event(DfsNamespaceJobEventKind::Parked);
            announce_namespace_job_event(&local, own.clone());
            assert_eq!(heard.recv().await, Some(own.clone()));
            while transport.0.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            let forwarded = transport.0.lock().unwrap()[0].clone();
            assert_eq!(
                forwarded.envelope.kind.as_str(),
                DFS_NAMESPACE_JOB_PARKED_EVENT_KIND
            );
            assert_eq!(forwarded.envelope.origin_node_id, local);
            assert_eq!(
                serde_json::from_value::<DfsNamespaceJobEvent>(forwarded.envelope.payload.clone())
                    .unwrap(),
                own
            );
            let mut theirs = event(DfsNamespaceJobEventKind::Completed);
            theirs.status = DfsNamespaceJobStatus::Completed;
            let mut from_remote = forwarded.as_ref().clone();
            from_remote.envelope = envelope(&remote, &theirs).unwrap();
            bus.ingest_remote(
                Arc::new(from_remote),
                sinomo_module::ComponentEventDurability::Transient,
            )
            .await
            .unwrap();
            assert_eq!(heard.recv().await, Some(theirs));
            stop.send(true).unwrap();
            task.await.unwrap();
        });
        assert!(
            heard.try_recv().is_err(),
            "the node's own event reached its listeners once"
        );
        assert!(!PUBLISHERS.lock().unwrap().contains_key(&local));
    }

    #[test]
    fn the_event_is_a_typed_cluster_fact_with_its_routing() {
        let local = NodeId::from_legacy_named("job-events-envelope").unwrap();
        let parked = event(DfsNamespaceJobEventKind::Parked);
        let envelope = envelope(&local, &parked).unwrap();
        assert_eq!(envelope.kind.as_str(), "dfs.namespace.job.parked");
        assert_eq!(
            envelope.routing.scope,
            sinomo_core::RoutingScope::TrustedNetwork
        );
        let hints = envelope
            .routing
            .hints
            .iter()
            .map(|hint| hint.as_str().to_owned())
            .collect::<Vec<_>>();
        for expected in [
            format!("dfs.filesystem:{}", parked.file_system_id),
            "dfs.coordinator-shard:services".into(),
            "dfs.namespace-job:job-touching-users".into(),
            "dfs.touched-shard:users".into(),
        ] {
            assert!(hints.contains(&expected), "{expected} in {hints:?}");
        }
        assert_eq!(
            serde_json::from_value::<DfsNamespaceJobEvent>(envelope.payload).unwrap(),
            parked
        );
        for kind in [
            DfsNamespaceJobEventKind::Parked,
            DfsNamespaceJobEventKind::Completed,
        ] {
            let publication = namespace_job_event_publication(kind);
            publication.validate().unwrap();
            assert_eq!(
                publication.locality,
                sinomo_module::ComponentEventLocality::Cluster
            );
            assert!(namespace_job_event_subscription().matches_kind(kind.event_kind()));
        }
        namespace_job_event_subscription().validate().unwrap();
    }
}
