//! Process-local resource observations supplied by the native application host.
//!
//! These conditions are advisory scheduling evidence. They are deliberately
//! neither persisted nor authoritative cluster state: an embedded runtime may
//! use them to defer passive work, while explicit requests and correctness
//! work retain their normal transport and authority paths.

use std::sync::Arc;

use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NodeResourceMode {
    #[default]
    Normal,
    Constrained,
    Restricted,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NodeNetworkMetering {
    Unmetered,
    Metered,
    #[default]
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeResourceSnapshot {
    /// Distinguishes a real all-default observation from startup uncertainty.
    pub observed: bool,
    pub mode: NodeResourceMode,
    pub network_metering: NodeNetworkMetering,
}

impl NodeResourceSnapshot {
    pub fn observed(mode: NodeResourceMode, network_metering: NodeNetworkMetering) -> Self {
        Self {
            observed: true,
            mode,
            network_metering,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeResourcePublisher {
    sender: Arc<watch::Sender<NodeResourceSnapshot>>,
}

impl NodeResourcePublisher {
    /// Publishes a changed snapshot and wakes subscribers. Equal observations
    /// are deduplicated so a native sampler cannot turn into a runtime timer.
    pub fn publish(&self, snapshot: NodeResourceSnapshot) -> bool {
        self.sender.send_if_modified(|current| {
            if *current == snapshot {
                false
            } else {
                *current = snapshot;
                true
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct NodeResourceMonitor {
    receiver: watch::Receiver<NodeResourceSnapshot>,
    // A monitor remains a valid wait source even if its host-side publisher is
    // dropped. This prevents a default/unobserved monitor from spinning after
    // observing a permanently closed watch channel.
    _keepalive: Arc<watch::Sender<NodeResourceSnapshot>>,
}

impl Default for NodeResourceMonitor {
    fn default() -> Self {
        node_resource_channel().1
    }
}

impl NodeResourceMonitor {
    pub fn latest(&self) -> NodeResourceSnapshot {
        *self.receiver.borrow()
    }

    /// Reads the current snapshot and consumes its watch generation. Runtime
    /// loops use this before waiting so a condition already applied to the
    /// current pass does not cause one duplicate wake.
    pub fn take_latest(&mut self) -> NodeResourceSnapshot {
        *self.receiver.borrow_and_update()
    }

    pub async fn changed(&mut self) -> NodeResourceSnapshot {
        // `_keepalive` makes closure impossible for this monitor's lifetime.
        let _ = self.receiver.changed().await;
        self.latest()
    }
}

pub fn node_resource_channel() -> (NodeResourcePublisher, NodeResourceMonitor) {
    let (sender, receiver) = watch::channel(NodeResourceSnapshot::default());
    let sender = Arc::new(sender);
    (
        NodeResourcePublisher {
            sender: Arc::clone(&sender),
        },
        NodeResourceMonitor {
            receiver,
            _keepalive: sender,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resource_channel_deduplicates_and_wakes_on_real_changes() {
        let (publisher, mut monitor) = node_resource_channel();
        assert_eq!(monitor.latest(), NodeResourceSnapshot::default());
        assert!(!publisher.publish(NodeResourceSnapshot::default()));

        let observed = NodeResourceSnapshot::observed(
            NodeResourceMode::Restricted,
            NodeNetworkMetering::Metered,
        );
        assert!(publisher.publish(observed));
        assert_eq!(monitor.changed().await, observed);
        assert!(!publisher.publish(observed));
    }
}
