//! Explicit R0 boundary facade.
//!
//! `sinomo-core` defines the R0 boundary: the minimal set of crates that
//! defines the cluster. R1 crates treat this crate as the public foundation
//! surface for cluster contracts; they must not depend on the R0 crates
//! directly. The facade intentionally aggregates only R0 crates and adds no
//! behavior of its own.
//!
//! Base contracts (IDs, event envelope, domain types, scalars) are re-exported
//! at the crate root so existing `sinomo_core::...` paths keep working.

pub use sinomo_base as base;
pub use sinomo_base::*;

/// Sinomo Object wire codec (SOBJ).
pub mod object {
    pub use sinomo_object::*;
}

/// Storage layout and durable object schema contracts.
pub mod storage {
    pub use sinomo_storage::*;
}

/// Local data-directory layout.
pub mod local {
    pub use sinomo_local::*;
}

/// Node identity, capabilities, and cluster member records.
pub mod node {
    pub use sinomo_node::*;
}

/// Event persistence and replay contracts.
pub mod events {
    pub use sinomo_events::*;
}

/// Cluster control plane: membership, invites, joins, KV/Raft replication
/// drivers, cluster state publishing, and the KV-backed cluster directory.
pub mod cluster {
    pub use sinomo_cluster::*;
}

/// Cluster networking library: object RPC, routing, transports, network modules.
pub mod network {
    pub use sinomo_network::*;
}

/// Cluster KV store, Raft consensus, and replication.
pub mod kv {
    pub use sinomo_kv::*;
}

/// Service registration, discovery, and routing target selection.
pub mod service {
    pub use sinomo_service::*;
}
