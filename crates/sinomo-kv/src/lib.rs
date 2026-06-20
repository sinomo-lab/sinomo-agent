//! In-process cluster KV core.
//!
//! This crate provides the first etcd-like control-plane KV surface for
//! Sinomo: monotonic revisions, compare-and-swap transactions, leases,
//! watches, snapshots, and a deterministic Raft replication core for tests,
//! local development, and daemon integration.
//!
//! This crate owns KV storage and Raft leader/election/log semantics. Cross-node
//! transmission should be supplied by network-owned transport capabilities; KV
//! code should not decide routes or interpret cluster connectivity topology. KV
//! can persist connectivity records as ordinary key/value entries, but those
//! records remain network-domain facts. KV RPC route/category/body payload specs
//! and local read/watch route dispatch live here; the cluster network wraps them
//! in its generic object-RPC envelope when transmitting between peers.

include!("root.rs");
