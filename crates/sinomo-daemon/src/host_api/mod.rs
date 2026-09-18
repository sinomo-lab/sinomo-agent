//! Daemon-side adapters for the shared wire API.
use sinomo_api::*;
mod auth_context;
mod event_service;
mod replay_protection;
mod service;
pub use auth_context::{AuthenticatedDaemonCommand, DaemonSignerAuthority};
pub use event_service::{
    DaemonEventHub, DaemonEventMirror, DaemonEventService, register_daemon_event_service,
};
pub use replay_protection::{ReplayNonceCachePoisoned, ScopedReplayNonceCache};
pub use service::{
    DaemonLifecycleFuture, DaemonLifecycleHost, DaemonOperationsFuture, DaemonOperationsHost,
    DaemonShutdownKind, HostedDaemonLifecycleService, HostedDaemonOperationsService,
    daemon_object_rpc_forward_options, daemon_object_rpc_forward_transport_timeouts,
    enforce_daemon_object_rpc_forward_route_affinity, register_hosted_daemon_lifecycle_service,
    register_hosted_daemon_operations_service,
};
