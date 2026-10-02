//! Bounded diagnostics only; these observations never grant replica authority.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{DfsMetadataShardId, DfsObjectId, NodeId};

const INTERVAL: Duration = Duration::from_secs(60);
const MAX_KEYS: usize = 256;
const MAX_LOGS_PER_INTERVAL: usize = 16;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PeerKey(NodeId, NodeId, DfsMetadataShardId);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Recovery {
    Startup,
    Network,
    Healthy,
    Unclassified,
}

impl Recovery {
    fn label(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Network => "network-recovery",
            Self::Healthy => "steady-state",
            Self::Unclassified => "unclassified",
        }
    }
}

#[derive(Default)]
struct Diagnostics {
    recovery: BTreeMap<PeerKey, Recovery>,
    limiter: Limiter<DiagnosticKey>,
    fetch_limiter: Limiter<(NodeId, NodeId, &'static str)>,
}

impl Diagnostics {
    fn update_recovery(&mut self, key: PeerKey, phase: Option<Recovery>) {
        if let Some(current) = self.recovery.get_mut(&key) {
            if let Some(phase) = phase {
                *current = phase;
            }
        } else if self.recovery.len() < MAX_KEYS {
            self.recovery
                .insert(key, phase.unwrap_or(Recovery::Startup));
        }
    }

    fn recovery(&self, key: &PeerKey) -> Recovery {
        self.recovery
            .get(key)
            .copied()
            .unwrap_or(Recovery::Unclassified)
    }
}

type DiagnosticKey = (PeerKey, &'static str, &'static str, bool);

struct Limiter<K> {
    keys: BTreeMap<K, (Instant, u64)>,
    window: Option<Instant>,
    emitted: usize,
    suppressed: u64,
}

impl<K> Default for Limiter<K> {
    fn default() -> Self {
        Self {
            keys: BTreeMap::new(),
            window: None,
            emitted: 0,
            suppressed: 0,
        }
    }
}

impl<K: Ord + Clone> Limiter<K> {
    fn admit(&mut self, key: K, now: Instant) -> Option<(u64, u64)> {
        if self
            .window
            .is_none_or(|start| now.duration_since(start) >= INTERVAL)
        {
            self.window = Some(now);
            self.emitted = 0;
        }
        if let Some((last, suppressed)) = self.keys.get_mut(&key)
            && (now.duration_since(*last) < INTERVAL || self.emitted >= MAX_LOGS_PER_INTERVAL)
        {
            *suppressed = suppressed.saturating_add(1);
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        if self.emitted >= MAX_LOGS_PER_INTERVAL {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        let repeated = self.keys.remove(&key).map_or(0, |(_, count)| count);
        if self.keys.len() >= MAX_KEYS {
            let oldest = self
                .keys
                .iter()
                .min_by_key(|(_, (last, _))| *last)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.keys.remove(&oldest);
            }
        }
        self.keys.insert(key, (now, 0));
        self.emitted += 1;
        Some((repeated, std::mem::take(&mut self.suppressed)))
    }
}

fn diagnostics() -> &'static Mutex<Diagnostics> {
    static STATE: OnceLock<Mutex<Diagnostics>> = OnceLock::new();
    STATE.get_or_init(|| {
        metrics::describe_counter!("sinomo.dfs.metadata_catchup", metrics::Unit::Count,
            "Metadata snapshot repairs and historical journal backfills, including suppressed diagnostics");
        metrics::describe_counter!("sinomo.dfs.blob_fetch_rejections", metrics::Unit::Count,
            "DFS blob fetch rejections, including rate-limited warnings");
        Mutex::new(Diagnostics::default())
    })
}

/// Called by the existing publisher's route/result handling. An initial sync
/// or an observed missing/failed transport is explicit recovery evidence; elapsed
/// time, a zero cursor and "background" alone never exempt a repair warning.
pub(crate) fn replication_started(local: &NodeId, peer: &NodeId, shard: &DfsMetadataShardId) {
    let mut state = diagnostics()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let key = PeerKey(local.clone(), peer.clone(), shard.clone());
    state.update_recovery(key, None);
}

pub(crate) fn replication_unavailable(local: &NodeId, peer: &NodeId, shard: &DfsMetadataShardId) {
    let mut state = diagnostics()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let key = PeerKey(local.clone(), peer.clone(), shard.clone());
    state.update_recovery(key, Some(Recovery::Network));
}

pub(crate) fn replication_converged(local: &NodeId, peer: &NodeId, shard: &DfsMetadataShardId) {
    let mut state = diagnostics()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let key = PeerKey(local.clone(), peer.clone(), shard.clone());
    state.update_recovery(key, Some(Recovery::Healthy));
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record(
    local: &NodeId,
    peer: &NodeId,
    shard: &DfsMetadataShardId,
    operation: &'static str,
    kind: &'static str,
    after_sequence: Option<u64>,
    through_sequence: Option<u64>,
    reason: &str,
) {
    let key = PeerKey(local.clone(), peer.clone(), shard.clone());
    let (recovery, admission) = {
        let mut state = diagnostics()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let recovery = state.recovery(&key);
        let admission = state.limiter.admit(
            (
                key,
                operation,
                kind,
                matches!(recovery, Recovery::Healthy | Recovery::Unclassified),
            ),
            Instant::now(),
        );
        (recovery, admission)
    };
    metrics::counter!("sinomo.dfs.metadata_catchup", "operation" => operation,
        "kind" => kind, "context" => recovery.label())
    .increment(1);
    let Some((suppressed_similar, suppressed_total)) = admission else {
        return;
    };
    // No request IDs, paths or free-form error strings participate in the key.
    // Every emission includes the current cause and counts since prior logs.
    if matches!(recovery, Recovery::Healthy | Recovery::Unclassified) {
        tracing::warn!(target: "sinomo.dfs.catchup", local_node_id = %local,
            peer_node_id = %peer, shard = %shard.0, operation, kind,
            context = recovery.label(), ?after_sequence, ?through_sequence,
            suppressed_similar, suppressed_total, reason,
            "unexpected DFS metadata catch-up during ordinary operation");
    } else {
        tracing::info!(target: "sinomo.dfs.catchup", local_node_id = %local,
            peer_node_id = %peer, shard = %shard.0, operation, kind,
            context = recovery.label(), ?after_sequence, ?through_sequence,
            suppressed_similar, suppressed_total, reason,
            "DFS metadata catch-up during explicit recovery");
    }
}

fn blob_fetch_rejection_kind(reason: &str) -> &'static str {
    if reason.starts_with("DFS fetch response source mismatch:") {
        "source-mismatch"
    } else if reason == "unexpected DFS fetch response source" {
        "unexpected-source"
    } else if reason == "unexpected DFS fetch response request id" {
        "unexpected-request"
    } else if reason.contains("missing pending write for fetched blob")
        || reason.contains("unknown pending write intent:")
    {
        "missing-pending-write"
    } else if reason.contains("hash mismatch") {
        "content-hash-mismatch"
    } else if reason == "blob not found" {
        "not-found"
    } else if crate::repair_rejection_indicates_lost_authority(reason) {
        "authority"
    } else {
        "other"
    }
}

pub(crate) fn blob_fetch_rejected(
    local: &NodeId,
    source: &NodeId,
    intent_id: &str,
    object_id: &DfsObjectId,
    reason: &str,
) {
    let kind = blob_fetch_rejection_kind(reason);
    let admission = diagnostics()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .fetch_limiter
        .admit((local.clone(), source.clone(), kind), Instant::now());
    metrics::counter!("sinomo.dfs.blob_fetch_rejections", "kind" => kind).increment(1);
    let Some((suppressed_similar, suppressed_total)) = admission else {
        return;
    };
    tracing::warn!(target: "sinomo.dfs.blob_fetch", local_node_id = %local,
        source_node_id = %source, intent_id, object_id = %object_id,
        kind, reason, suppressed_similar, suppressed_total,
        "DFS blob fetch rejected");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(index: usize) -> (PeerKey, &'static str, &'static str, bool) {
        (
            PeerKey(
                NodeId::from_legacy_named("local").unwrap(),
                NodeId::from_legacy_named(format!("peer-{index}")).unwrap(),
                DfsMetadataShardId::new("root"),
            ),
            "routed-write",
            "snapshot-repair",
            true,
        )
    }

    #[test]
    fn only_observed_startup_or_transport_failure_exempts_recovery() {
        let mut state = Diagnostics::default();
        let peer = key(0).0;
        assert_eq!(state.recovery(&peer), Recovery::Unclassified);
        state.update_recovery(peer.clone(), None);
        assert_eq!(state.recovery(&peer), Recovery::Startup);
        state.update_recovery(peer.clone(), Some(Recovery::Healthy));
        state.update_recovery(peer.clone(), None);
        assert_eq!(
            state.recovery(&peer),
            Recovery::Healthy,
            "a new publish invocation must not relabel a healthy peer as starting"
        );
        state.update_recovery(peer.clone(), Some(Recovery::Network));
        assert_eq!(state.recovery(&peer), Recovery::Network);
        state.update_recovery(peer.clone(), Some(Recovery::Healthy));
        assert_eq!(state.recovery(&peer), Recovery::Healthy);
    }

    #[test]
    fn first_unexpected_repair_is_not_hidden_by_a_recovery_log() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        let mut expected = key(0);
        expected.3 = false;
        assert!(limiter.admit(expected, now).is_some());
        assert!(limiter.admit(key(0), now).is_some());
    }

    #[test]
    fn retries_and_changing_request_details_do_not_flood_logs() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        assert_eq!(limiter.admit(key(0), now), Some((0, 0)));
        for _ in 0..1000 {
            assert_eq!(limiter.admit(key(0), now), None);
        }
        assert_eq!(limiter.admit(key(0), now + INTERVAL), Some((1000, 1000)));
        assert_eq!(limiter.admit(key(0), now + INTERVAL), None);
    }

    #[test]
    fn many_peers_have_a_process_budget_and_bounded_memory() {
        let mut limiter = Limiter::default();
        let now = Instant::now();
        for interval in 0..40 {
            let at = now + INTERVAL * interval;
            let admitted = (0..100)
                .filter(|peer| {
                    limiter
                        .admit(key(interval as usize * 100 + peer), at)
                        .is_some()
                })
                .count();
            assert_eq!(admitted, MAX_LOGS_PER_INTERVAL);
            assert!(limiter.keys.len() <= MAX_KEYS);
        }
    }

    #[test]
    fn blob_fetch_kinds_and_changing_operation_details_keep_a_bounded_log_budget() {
        assert_eq!(
            blob_fetch_rejection_kind("unexpected DFS fetch response source"),
            "unexpected-source"
        );
        assert_eq!(
            blob_fetch_rejection_kind("unexpected DFS fetch response request id"),
            "unexpected-request"
        );
        assert_eq!(
            blob_fetch_rejection_kind("DFS fetch response source mismatch: a, b"),
            "source-mismatch"
        );
        assert_eq!(
            blob_fetch_rejection_kind(
                "distributed DFS error: missing pending write for fetched blob: intent-1"
            ),
            "missing-pending-write"
        );
        assert_eq!(
            blob_fetch_rejection_kind("content-addressed blob hash mismatch: expected=a, actual=b"),
            "content-hash-mismatch"
        );
        assert_eq!(blob_fetch_rejection_kind("blob not found"), "not-found");
        let local = NodeId::from_legacy_named("fetch-local").unwrap();
        let source = NodeId::from_legacy_named("fetch-source").unwrap();
        let now = Instant::now();
        let mut limiter = Limiter::default();
        let key = (local.clone(), source, "missing-pending-write");
        assert_eq!(limiter.admit(key.clone(), now), Some((0, 0)));
        for index in 0..1000 {
            let reason = format!("missing pending write for fetched blob: intent-{index}");
            assert_eq!(blob_fetch_rejection_kind(&reason), key.2);
            assert_eq!(limiter.admit(key.clone(), now), None);
        }
        assert_eq!(limiter.admit(key, now + INTERVAL), Some((1000, 1000)));
        for interval in 2..40 {
            let at = now + INTERVAL * interval;
            let admitted = (0..100)
                .filter(|peer| {
                    let source =
                        NodeId::from_legacy_named(format!("fetch-peer-{interval}-{peer}")).unwrap();
                    limiter
                        .admit((local.clone(), source, "other"), at)
                        .is_some()
                })
                .count();
            assert_eq!(admitted, MAX_LOGS_PER_INTERVAL);
            assert!(limiter.keys.len() <= MAX_KEYS);
        }
    }
}
