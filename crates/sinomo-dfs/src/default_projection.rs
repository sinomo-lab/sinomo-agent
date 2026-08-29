//! Committed KV projection for default distributed-DFS reconciliation.
//!
//! The process host supplies committed snapshots or deltas and a watch sender.
//! This module owns which KV inputs affect DFS placement, their stable semantic
//! representation, revision continuity, and parked-handoff wake filtering.

use crate::{
    DefaultDfsParkedHandoff, DefaultDfsPermitGeneration, DefaultDfsReconciliationProjection,
};
use sinomo_core::kv::{KvEntry, KvRevision, KvSnapshot};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

pub const DEFAULT_DFS_MEMBER_PREFIX: &str = "/cluster/members/";
pub const DEFAULT_DFS_ENDPOINT_PREFIX: &str = "/cluster/endpoints/";
pub const DEFAULT_DFS_CONNECTIVITY_PREFIX: &str = "/cluster/network/connectivity/";

pub fn default_dfs_reconciliation_exact_input_keys() -> &'static BTreeSet<String> {
    static KEYS: OnceLock<BTreeSet<String>> = OnceLock::new();
    KEYS.get_or_init(|| {
        let file_system_id = crate::sifs_file_system_id();
        let mut keys = BTreeSet::new();
        keys.insert(sinomo_core::kv::cluster_kv_raft_config_key());
        keys.insert(crate::file_system_config_key(&file_system_id));
        for shard_id in ["root", "services", "users"] {
            keys.insert(crate::metadata_shard_key(&file_system_id, shard_id));
            keys.insert(crate::metadata_shard_membership_handoff_key(
                &file_system_id,
                shard_id,
            ));
            keys.insert(crate::metadata_shard_membership_handoff_proof_key(
                &file_system_id,
                shard_id,
            ));
            keys.insert(crate::metadata_shard_head_watermark_key(
                &file_system_id,
                shard_id,
            ));
        }
        keys
    })
}

pub fn default_dfs_reconciliation_exact_retry_key(key: &str) -> bool {
    let file_system_id = crate::sifs_file_system_id();
    key == crate::placement_authority_permit_key(&file_system_id)
        || ["root", "services", "users"].into_iter().any(|shard_id| {
            key == crate::metadata_shard_key(&file_system_id, shard_id)
                || key == crate::metadata_shard_membership_handoff_key(&file_system_id, shard_id)
                || key
                    == crate::metadata_shard_membership_handoff_proof_key(&file_system_id, shard_id)
                || key == crate::metadata_shard_head_watermark_key(&file_system_id, shard_id)
        })
}

pub fn default_dfs_reconciliation_prefixes() -> [&'static str; 6] {
    [
        DEFAULT_DFS_MEMBER_PREFIX,
        DEFAULT_DFS_ENDPOINT_PREFIX,
        DEFAULT_DFS_CONNECTIVITY_PREFIX,
        sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX,
        crate::metadata_services_prefix(),
        crate::content_services_prefix(),
    ]
}

#[derive(serde::Serialize)]
struct DefaultDfsEndpointTopologyProjection<'a> {
    endpoint_id: &'a sinomo_core::Id,
    node_id: &'a NodeId,
    transport: &'a sinomo_core::ClusterTransportKind,
    address: &'a str,
    source: &'a sinomo_core::ClusterEndpointSource,
    privacy: &'a sinomo_core::ClusterEndpointPrivacy,
    priority: u16,
}

impl<'a> From<&'a sinomo_core::ClusterEndpointRecord> for DefaultDfsEndpointTopologyProjection<'a> {
    fn from(endpoint: &'a sinomo_core::ClusterEndpointRecord) -> Self {
        Self {
            endpoint_id: &endpoint.endpoint_id,
            node_id: &endpoint.node_id,
            transport: &endpoint.transport,
            address: &endpoint.address,
            source: &endpoint.source,
            privacy: &endpoint.privacy,
            priority: endpoint.priority,
        }
    }
}

#[derive(serde::Serialize)]
struct DefaultDfsMemberProjection<'a> {
    cluster_id: &'a sinomo_core::Id,
    node_id: &'a NodeId,
    role: &'a sinomo_core::ClusterMemberRole,
    state: &'a sinomo_core::ClusterMemberState,
    endpoints: Vec<DefaultDfsEndpointTopologyProjection<'a>>,
}

#[derive(serde::Serialize)]
struct DefaultDfsConnectivityProjection<'a> {
    cluster_id: &'a sinomo_core::Id,
    source_node_id: &'a NodeId,
    target_node_id: &'a NodeId,
    endpoint_id: &'a sinomo_core::network::ClusterEndpointId,
    transport: &'a sinomo_core::ClusterTransportKind,
    address: &'a str,
    state: &'a sinomo_core::network::ClusterConnectivityState,
    failure_reason: &'a Option<sinomo_core::network::ClusterConnectivityFailureReason>,
}

#[derive(serde::Serialize)]
struct DefaultDfsAdvertisementProjection<'a> {
    node_id: &'a NodeId,
    capacity_weight: u32,
    accepts_metadata: bool,
    accepts_content: bool,
    zone: &'a Option<String>,
}

#[derive(serde::Serialize)]
struct DefaultDfsServiceInstanceProjection<'a> {
    node_id: &'a NodeId,
    capabilities: &'a [String],
}

fn reconciliation_input_value(entry: &KvEntry) -> Vec<u8> {
    if entry.key.starts_with(DEFAULT_DFS_MEMBER_PREFIX)
        && let Ok(member) = entry.decode::<sinomo_core::ClusterMemberRecord>()
    {
        let mut endpoints = member
            .endpoints
            .iter()
            .map(DefaultDfsEndpointTopologyProjection::from)
            .collect::<Vec<_>>();
        endpoints.sort_by(|left, right| left.endpoint_id.cmp(right.endpoint_id));
        // Placement consumes membership eligibility/order and stable endpoint
        // topology, not liveness timestamps or observed latency.
        return sinomo_core::kv::encode_kv_value(&DefaultDfsMemberProjection {
            cluster_id: &member.cluster_id,
            node_id: &member.node_id,
            role: &member.role,
            state: &member.state,
            endpoints,
        })
        .unwrap_or_else(|_| entry.value.clone());
    }
    if entry.key.starts_with(DEFAULT_DFS_ENDPOINT_PREFIX)
        && let Ok(endpoint) = entry.decode::<sinomo_core::ClusterEndpointRecord>()
    {
        return sinomo_core::kv::encode_kv_value(&DefaultDfsEndpointTopologyProjection::from(
            &endpoint,
        ))
        .unwrap_or_else(|_| entry.value.clone());
    }
    if entry.key.starts_with(DEFAULT_DFS_CONNECTIVITY_PREFIX)
        && let Ok(record) = entry.decode::<sinomo_core::network::ClusterConnectivityRecord>()
    {
        // Match the stable connectivity equality contract: expiry,
        // observations, and latency refreshes do not affect route existence.
        return sinomo_core::kv::encode_kv_value(&DefaultDfsConnectivityProjection {
            cluster_id: &record.cluster_id,
            source_node_id: &record.source_node_id,
            target_node_id: &record.target_node_id,
            endpoint_id: &record.endpoint_id,
            transport: &record.transport,
            address: &record.address,
            state: &record.state,
            failure_reason: &record.failure_reason,
        })
        .unwrap_or_else(|_| entry.value.clone());
    }
    if entry
        .key
        .starts_with(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
        && let Ok(instance) = entry.decode::<sinomo_core::service::ServiceInstanceRecord>()
    {
        return sinomo_core::kv::encode_kv_value(&DefaultDfsServiceInstanceProjection {
            node_id: &instance.node_id,
            capabilities: &instance.provider_capabilities,
        })
        .unwrap_or_else(|_| entry.value.clone());
    }
    if (entry.key.starts_with(crate::metadata_services_prefix())
        || entry.key.starts_with(crate::content_services_prefix()))
        && let Ok(advertisement) = entry.decode::<crate::DfsNodeAdvertisement>()
    {
        // Advertisements remain projected so deletion or semantic corruption
        // self-heals. Refresh-only `updated_at` churn is deliberately excluded.
        return sinomo_core::kv::encode_kv_value(&DefaultDfsAdvertisementProjection {
            node_id: &advertisement.node_id,
            capacity_weight: advertisement.capacity_weight,
            accepts_metadata: advertisement.accepts_metadata,
            accepts_content: advertisement.accepts_content,
            zone: &advertisement.zone,
        })
        .unwrap_or_else(|_| entry.value.clone());
    }
    entry.value.clone()
}

fn reconciliation_projection_from_entries(
    entries: &[KvEntry],
) -> (Vec<(String, Vec<u8>)>, Option<DefaultDfsPermitGeneration>) {
    let permit_key = crate::placement_authority_permit_key(&crate::sifs_file_system_id());
    let mut inputs = Vec::new();
    let mut placement_authority_permit = None;
    for entry in entries {
        if entry.key == permit_key {
            placement_authority_permit = Some(DefaultDfsPermitGeneration {
                modify_revision: entry.modify_revision,
                version: entry.version,
                value: entry.value.clone(),
            });
        } else if default_dfs_reconciliation_key_is_relevant(&entry.key) {
            inputs.push((entry.key.clone(), reconciliation_input_value(entry)));
        }
    }
    inputs.sort_by(|left, right| left.0.cmp(&right.0));
    inputs.dedup_by(|left, right| left.0 == right.0);
    (inputs, placement_authority_permit)
}

fn reconciliation_projection(
    snapshot: &KvSnapshot,
) -> (Vec<(String, Vec<u8>)>, Option<DefaultDfsPermitGeneration>) {
    let exact_keys = default_dfs_reconciliation_exact_input_keys();
    let permit_key = crate::placement_authority_permit_key(&crate::sifs_file_system_id());
    let mut inputs = Vec::new();
    for key in exact_keys {
        if let Some(entry) = snapshot.get(key) {
            inputs.push((entry.key.clone(), reconciliation_input_value(entry)));
        }
    }
    for prefix in default_dfs_reconciliation_prefixes() {
        for entry in snapshot.prefix_entries(prefix) {
            inputs.push((entry.key.clone(), reconciliation_input_value(entry)));
        }
    }
    inputs.sort_by(|left, right| left.0.cmp(&right.0));
    let placement_authority_permit =
        snapshot
            .get(&permit_key)
            .map(|entry| DefaultDfsPermitGeneration {
                modify_revision: entry.modify_revision,
                version: entry.version,
                value: entry.value.clone(),
            });
    (inputs, placement_authority_permit)
}

fn default_dfs_reconciliation_key_is_relevant(key: &str) -> bool {
    default_dfs_reconciliation_exact_input_keys().contains(key)
        || key == crate::placement_authority_permit_key(&crate::sifs_file_system_id())
        || default_dfs_reconciliation_prefixes()
            .into_iter()
            .any(|prefix| key.starts_with(prefix))
}

fn events_cover_revision_range(
    previous_revision: KvRevision,
    current_revision: KvRevision,
    events: &[sinomo_core::kv::KvEvent],
) -> bool {
    current_revision <= previous_revision
        || (events.is_empty() && current_revision == previous_revision.next())
        || (events.first().map(|event| event.revision) == Some(previous_revision.next())
            && events.last().map(|event| event.revision) == Some(current_revision)
            && events.windows(2).all(|events| {
                let previous = events[0].revision;
                let current = events[1].revision;
                current == previous || current == previous.next()
            }))
}

pub fn observe_default_dfs_reconciliation(
    projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
    snapshot: &KvSnapshot,
    parked_handoff: Option<&DefaultDfsParkedHandoff>,
) {
    let (inputs, placement_authority_permit) = reconciliation_projection(snapshot);
    projection_tx.send_if_modified(|observed| {
        if observed
            .revision
            .is_some_and(|revision| snapshot.revision < revision)
        {
            return false;
        }
        let changed = parked_handoff.map_or_else(
            || {
                observed.inputs != inputs
                    || observed.placement_authority_permit != placement_authority_permit
            },
            |parked| {
                parked.wake_inputs(&observed.inputs) != parked.wake_inputs(&inputs)
                    || observed.placement_authority_permit != placement_authority_permit
            },
        );
        observed.revision = Some(snapshot.revision);
        observed.inputs.clone_from(&inputs);
        observed
            .placement_authority_permit
            .clone_from(&placement_authority_permit);
        changed
    });
}

/// Incremental committed-projection engine for the default DFS reactor.
///
/// Hosts own subscription registration and resident KV reads. The owner engine
/// accepts only complete monotonic deltas and returns `false` when the host must
/// rebuild from its authoritative bounded resident projection.
#[derive(Debug, Default)]
pub struct DefaultDfsCommittedProjectionObserver {
    revision: Option<KvRevision>,
    parked_subscription: Option<Arc<Mutex<Option<DefaultDfsParkedHandoff>>>>,
    rebuild_count: usize,
}

impl DefaultDfsCommittedProjectionObserver {
    pub fn new_with_parked_subscription(
        parked_subscription: Arc<Mutex<Option<DefaultDfsParkedHandoff>>>,
    ) -> Self {
        Self {
            parked_subscription: Some(parked_subscription),
            ..Self::default()
        }
    }

    pub fn revision(&self) -> Option<KvRevision> {
        self.revision
    }

    #[doc(hidden)]
    pub fn rebuild_count(&self) -> usize {
        self.rebuild_count
    }

    fn parked_handoff(&self) -> Option<DefaultDfsParkedHandoff> {
        self.parked_subscription
            .as_ref()
            .and_then(|parked| parked.lock().ok()?.clone())
    }

    pub fn rebuild_from_entries(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
        revision: KvRevision,
        entries: &[KvEntry],
    ) {
        if self
            .revision
            .is_some_and(|observed_revision| revision <= observed_revision)
        {
            return;
        }
        self.revision = Some(revision);
        self.rebuild_count = self.rebuild_count.saturating_add(1);
        let parked_handoff = self.parked_handoff();
        let (inputs, placement_authority_permit) = reconciliation_projection_from_entries(entries);
        projection_tx.send_if_modified(|observed| {
            if observed
                .revision
                .is_some_and(|observed_revision| revision < observed_revision)
            {
                return false;
            }
            let changed = parked_handoff.as_ref().map_or_else(
                || {
                    observed.inputs != inputs
                        || observed.placement_authority_permit != placement_authority_permit
                },
                |parked| {
                    parked.wake_inputs(&observed.inputs) != parked.wake_inputs(&inputs)
                        || observed.placement_authority_permit != placement_authority_permit
                },
            );
            observed.revision = Some(revision);
            observed.inputs.clone_from(&inputs);
            observed
                .placement_authority_permit
                .clone_from(&placement_authority_permit);
            changed
        });
    }

    #[doc(hidden)]
    pub fn observe_snapshot(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
        snapshot: &KvSnapshot,
    ) {
        let Some(previous_revision) = self.revision else {
            self.rebuild_snapshot(projection_tx, snapshot);
            return;
        };
        if snapshot.revision <= previous_revision {
            return;
        }
        if previous_revision < snapshot.compacted_revision {
            self.rebuild_snapshot(projection_tx, snapshot);
            return;
        }
        let first_new = snapshot
            .events
            .partition_point(|event| event.revision <= previous_revision);
        let new_events = &snapshot.events[first_new..];
        if !events_cover_revision_range(previous_revision, snapshot.revision, new_events) {
            self.rebuild_snapshot(projection_tx, snapshot);
            return;
        }
        let parked_handoff = self.parked_handoff();
        let relevant_change = new_events.iter().any(|event| {
            parked_handoff.as_ref().map_or_else(
                || default_dfs_reconciliation_key_is_relevant(&event.key),
                |parked| {
                    parked.owns_key(&event.key)
                        || event.key
                            == crate::placement_authority_permit_key(&crate::sifs_file_system_id())
                },
            )
        });
        self.revision = Some(snapshot.revision);
        if relevant_change {
            self.rebuild_projection(projection_tx, snapshot);
        } else if let Some(parked) = parked_handoff.as_ref() {
            self.apply_parked_projection_deltas(projection_tx, snapshot, first_new, parked);
        } else {
            projection_tx.send_if_modified(|observed| {
                if observed
                    .revision
                    .is_some_and(|revision| snapshot.revision <= revision)
                {
                    return false;
                }
                observed.revision = Some(snapshot.revision);
                false
            });
        }
    }

    pub fn observe_delta(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
        delta: &sinomo_core::kv::ClusterKvCommittedDelta,
    ) -> bool {
        let Some(previous_revision) = self.revision else {
            return false;
        };
        if delta.revision <= previous_revision {
            return true;
        }
        if previous_revision < delta.compacted_revision {
            return false;
        }
        if !events_cover_revision_range(previous_revision, delta.revision, &delta.events) {
            return false;
        }
        let relevant_event_keys = delta
            .events
            .iter()
            .filter(|event| default_dfs_reconciliation_key_is_relevant(&event.key))
            .map(|event| event.key.as_str())
            .collect::<BTreeSet<_>>();
        let mut final_keys = BTreeSet::new();
        let mut relevant_final_keys = BTreeSet::new();
        for state in &delta.final_key_states {
            if !final_keys.insert(state.key.as_str())
                || state
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.key != state.key)
            {
                return false;
            }
            if default_dfs_reconciliation_key_is_relevant(&state.key) {
                relevant_final_keys.insert(state.key.as_str());
            }
        }
        if relevant_event_keys != relevant_final_keys {
            return false;
        }

        let parked_handoff = self.parked_handoff();
        let permit_key = crate::placement_authority_permit_key(&crate::sifs_file_system_id());
        let has_relevant_state = delta.final_key_states.iter().any(|state| {
            state.key == permit_key || default_dfs_reconciliation_key_is_relevant(&state.key)
        });
        if !has_relevant_state {
            projection_tx.send_if_modified(|observed| {
                if observed
                    .revision
                    .is_some_and(|revision| delta.revision <= revision)
                {
                    return false;
                }
                observed.revision = Some(delta.revision);
                false
            });
            self.revision = Some(delta.revision);
            return true;
        }
        projection_tx.send_if_modified(|observed| {
            if observed
                .revision
                .is_some_and(|revision| delta.revision <= revision)
            {
                return false;
            }
            let previous_wake_inputs = parked_handoff
                .as_ref()
                .map(|parked| parked.wake_inputs(&observed.inputs));
            let previous_permit = observed.placement_authority_permit.clone();
            let mut inputs_changed = false;
            for state in &delta.final_key_states {
                if state.key == permit_key {
                    observed.placement_authority_permit =
                        state
                            .entry
                            .as_ref()
                            .map(|entry| DefaultDfsPermitGeneration {
                                modify_revision: entry.modify_revision,
                                version: entry.version,
                                value: entry.value.clone(),
                            });
                    continue;
                }
                if !default_dfs_reconciliation_key_is_relevant(&state.key) {
                    continue;
                }
                let position = observed
                    .inputs
                    .binary_search_by(|(key, _)| key.as_str().cmp(state.key.as_str()));
                match state.entry.as_ref() {
                    Some(entry) => {
                        let value = reconciliation_input_value(entry);
                        match position {
                            Ok(index) if observed.inputs[index].1 == value => {}
                            Ok(index) => {
                                observed.inputs[index].1 = value;
                                inputs_changed = true;
                            }
                            Err(index) => {
                                observed.inputs.insert(index, (state.key.clone(), value));
                                inputs_changed = true;
                            }
                        }
                    }
                    None => {
                        if let Ok(index) = position {
                            observed.inputs.remove(index);
                            inputs_changed = true;
                        }
                    }
                }
            }
            observed.revision = Some(delta.revision);
            let relevant_inputs_changed =
                parked_handoff.as_ref().map_or(inputs_changed, |parked| {
                    previous_wake_inputs.as_ref() != Some(&parked.wake_inputs(&observed.inputs))
                });
            relevant_inputs_changed || previous_permit != observed.placement_authority_permit
        });
        self.revision = Some(delta.revision);
        true
    }

    fn apply_parked_projection_deltas(
        &self,
        projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
        snapshot: &KvSnapshot,
        first_new: usize,
        parked: &DefaultDfsParkedHandoff,
    ) {
        let permit_key = crate::placement_authority_permit_key(&crate::sifs_file_system_id());
        projection_tx.send_if_modified(|observed| {
            if observed
                .revision
                .is_some_and(|revision| snapshot.revision <= revision)
            {
                return false;
            }
            let previous_wake_inputs = parked.wake_inputs(&observed.inputs);
            let previous_permit = observed.placement_authority_permit.clone();
            for event in &snapshot.events[first_new..] {
                if event.key == permit_key {
                    observed.placement_authority_permit =
                        snapshot
                            .get(&permit_key)
                            .map(|entry| DefaultDfsPermitGeneration {
                                modify_revision: entry.modify_revision,
                                version: entry.version,
                                value: entry.value.clone(),
                            });
                    continue;
                }
                if !default_dfs_reconciliation_key_is_relevant(&event.key) {
                    continue;
                }
                let position = observed
                    .inputs
                    .binary_search_by(|(key, _)| key.as_str().cmp(event.key.as_str()));
                match snapshot.get(&event.key) {
                    Some(entry) => {
                        let value = reconciliation_input_value(entry);
                        match position {
                            Ok(index) => observed.inputs[index].1 = value,
                            Err(index) => observed.inputs.insert(index, (event.key.clone(), value)),
                        }
                    }
                    None => {
                        if let Ok(index) = position {
                            observed.inputs.remove(index);
                        }
                    }
                }
            }
            observed.revision = Some(snapshot.revision);
            previous_wake_inputs != parked.wake_inputs(&observed.inputs)
                || previous_permit != observed.placement_authority_permit
        });
    }

    fn rebuild_snapshot(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
        snapshot: &KvSnapshot,
    ) {
        self.revision = Some(snapshot.revision);
        self.rebuild_projection(projection_tx, snapshot);
    }

    fn rebuild_projection(
        &mut self,
        projection_tx: &tokio::sync::watch::Sender<DefaultDfsReconciliationProjection>,
        snapshot: &KvSnapshot,
    ) {
        self.rebuild_count = self.rebuild_count.saturating_add(1);
        let parked_handoff = self.parked_handoff();
        observe_default_dfs_reconciliation(projection_tx, snapshot, parked_handoff.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{
        ClusterKvCommittedDelta, ClusterKvCommittedKeyState, KvEvent, KvEventKind, KvVersion,
    };

    fn entry(key: impl Into<String>, value: Vec<u8>, revision: u64) -> KvEntry {
        KvEntry {
            key: key.into(),
            value,
            create_revision: KvRevision(1),
            modify_revision: KvRevision(revision),
            version: KvVersion(revision),
            lease_id: None,
        }
    }

    #[test]
    fn complete_deltas_advance_projection_without_rebuild() {
        let config_key = crate::file_system_config_key(&crate::sifs_file_system_id());
        let (projection_tx, mut projection_rx) =
            tokio::sync::watch::channel(DefaultDfsReconciliationProjection::default());
        let mut observer = DefaultDfsCommittedProjectionObserver::default();
        observer.rebuild_from_entries(
            &projection_tx,
            KvRevision(1),
            &[entry(config_key.clone(), vec![1], 1)],
        );
        let _ = projection_rx.borrow_and_update();

        let delta = ClusterKvCommittedDelta {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(2),
                key: config_key.clone(),
                kind: KvEventKind::Put { value: vec![2] },
            }],
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: config_key.clone(),
                entry: Some(entry(config_key.clone(), vec![2], 2)),
            }],
        };
        assert!(observer.observe_delta(&projection_tx, &delta));
        assert_eq!(observer.rebuild_count(), 1);
        assert_eq!(projection_tx.borrow().inputs, vec![(config_key, vec![2])]);
        assert!(projection_rx.has_changed().expect("projection open"));
    }

    #[test]
    fn incomplete_delta_requests_authoritative_rebuild() {
        let config_key = crate::file_system_config_key(&crate::sifs_file_system_id());
        let (projection_tx, _) =
            tokio::sync::watch::channel(DefaultDfsReconciliationProjection::default());
        let mut observer = DefaultDfsCommittedProjectionObserver::default();
        observer.rebuild_from_entries(
            &projection_tx,
            KvRevision(1),
            &[entry(config_key.clone(), vec![1], 1)],
        );
        let gapped = ClusterKvCommittedDelta {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(3),
                key: config_key,
                kind: KvEventKind::Delete,
            }],
            final_key_states: Vec::new(),
        };
        assert!(!observer.observe_delta(&projection_tx, &gapped));
        assert_eq!(observer.revision(), Some(KvRevision(1)));
    }

    #[test]
    fn contiguous_noop_delta_advances_without_authoritative_rebuild() {
        let config_key = crate::file_system_config_key(&crate::sifs_file_system_id());
        let (projection_tx, projection_rx) =
            tokio::sync::watch::channel(DefaultDfsReconciliationProjection::default());
        let mut observer = DefaultDfsCommittedProjectionObserver::default();
        observer.rebuild_from_entries(
            &projection_tx,
            KvRevision(1),
            &[entry(config_key, vec![1], 1)],
        );
        let rebuild_count = observer.rebuild_count();

        let delta = ClusterKvCommittedDelta {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            events: Vec::new(),
            final_key_states: Vec::new(),
        };

        assert!(observer.observe_delta(&projection_tx, &delta));
        assert_eq!(observer.revision(), Some(KvRevision(2)));
        assert_eq!(projection_rx.borrow().revision, Some(KvRevision(2)));
        assert_eq!(observer.rebuild_count(), rebuild_count);
    }

    #[test]
    fn contiguous_noop_snapshot_advances_without_projection_rebuild() {
        let config_key = crate::file_system_config_key(&crate::sifs_file_system_id());
        let (projection_tx, projection_rx) =
            tokio::sync::watch::channel(DefaultDfsReconciliationProjection::default());
        let mut observer = DefaultDfsCommittedProjectionObserver::default();
        observer.rebuild_from_entries(
            &projection_tx,
            KvRevision(1),
            &[entry(config_key.clone(), vec![1], 1)],
        );
        let rebuild_count = observer.rebuild_count();
        let snapshot = KvSnapshot {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            entries: vec![entry(config_key, vec![1], 1)],
            leases: Vec::new(),
            events: Vec::new(),
            committed_requests: Vec::new(),
            expired_requests: Vec::new(),
        };

        observer.observe_snapshot(&projection_tx, &snapshot);

        assert_eq!(observer.revision(), Some(KvRevision(2)));
        assert_eq!(projection_rx.borrow().revision, Some(KvRevision(2)));
        assert_eq!(observer.rebuild_count(), rebuild_count);
    }
}
