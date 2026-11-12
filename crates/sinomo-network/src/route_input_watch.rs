//! Process-local notification that an input of route planning may have changed.
//!
//! Producers are the owners of those inputs: the local connectivity store, the
//! committed-directory invalidation boundary, the network-module registry and
//! the verification registry. Consumers re-read their exact inputs after a wake;
//! a notification is only a hint and carries no state of its own, so a lagged
//! receiver loses nothing by treating the lag as one more wake.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

const ROUTE_INPUT_CHANGE_CHANNEL_CAPACITY: usize = 128;

static ROUTE_INPUT_CHANGES: LazyLock<tokio::sync::broadcast::Sender<RouteInputChange>> =
    LazyLock::new(|| tokio::sync::broadcast::channel(ROUTE_INPUT_CHANGE_CHANNEL_CAPACITY).0);

/// Scope of one route-input change hint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RouteInputChange {
    /// An input owned by one cluster root changed.
    Root(PathBuf),
    /// A process-wide input changed, such as the network-module registry.
    All,
}

impl RouteInputChange {
    pub(crate) fn affects(&self, root: &Path) -> bool {
        match self {
            Self::Root(changed) => changed == root,
            Self::All => true,
        }
    }
}

pub(crate) fn notify_route_input_changed(root: &Path) {
    let _ = ROUTE_INPUT_CHANGES.send(RouteInputChange::Root(root.to_path_buf()));
}

pub(crate) fn notify_all_route_inputs_changed() {
    let _ = ROUTE_INPUT_CHANGES.send(RouteInputChange::All);
}

pub(crate) fn subscribe_route_input_changes() -> tokio::sync::broadcast::Receiver<RouteInputChange>
{
    ROUTE_INPUT_CHANGES.subscribe()
}
