//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::node::NodeId;
use time::OffsetDateTime;

pub const DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsHealthState {
    Healthy,
    Degraded,
    Critical,
    #[default]
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObservationFreshness {
    Fresh,
    Stale,
    Offline,
    Unauthorized,
    Unsupported,
    #[default]
    NeverSampled,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNetworkMetering {
    Unmetered,
    Metered,
    #[default]
    Unknown,
}

/// Coarse node-local pressure used only to rank new placement candidates.
/// Exact battery, thermal, and OS policy readings remain private to the node.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsResourcePressure {
    #[default]
    Normal,
    Constrained,
    Restricted,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsPlacementPathState {
    Healthy,
    Degraded,
    /// Node-level delivery is unavailable. A failed direct-endpoint probe
    /// alone cannot establish this state for a routed destination.
    Unreachable,
    #[default]
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementNodeObservation {
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(default)]
    pub storage_health: DfsHealthState,
    #[serde(default)]
    pub storage_freshness: DfsObservationFreshness,
    #[serde(default)]
    pub accepts_new_content: bool,
    #[serde(default)]
    pub capacity_weight: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_free_bytes: Option<u64>,
    #[serde(default)]
    pub content_location_count: u32,
    #[serde(default)]
    pub network_metering: DfsNetworkMetering,
    #[serde(default)]
    pub resource_pressure: DfsResourcePressure,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl DfsPlacementNodeObservation {
    pub fn from_advertisement(advertisement: &DfsNodeAdvertisement) -> Self {
        Self {
            node_id: advertisement.node_id.clone(),
            zone: advertisement.zone.clone(),
            storage_health: advertisement.storage_health,
            storage_freshness: advertisement.storage_freshness,
            accepts_new_content: advertisement.accepts_content,
            capacity_weight: advertisement.capacity_weight.max(1),
            usable_capacity_bytes: advertisement.usable_capacity_bytes,
            reserved_free_bytes: advertisement.reserved_free_bytes,
            content_location_count: advertisement.content_location_count,
            network_metering: advertisement.network_metering,
            resource_pressure: advertisement.resource_pressure,
            observed_at: advertisement.updated_at,
            expires_at: advertisement
                .expires_at
                .unwrap_or_else(|| advertisement.updated_at + time::Duration::minutes(5)),
        }
    }

    pub fn freshness_at(&self, now: OffsetDateTime) -> DfsObservationFreshness {
        if now > self.expires_at {
            DfsObservationFreshness::Stale
        } else {
            self.storage_freshness
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementPathObservation {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    #[serde(default)]
    pub state: DfsPlacementPathState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_bytes_per_second: Option<u64>,
    #[serde(default)]
    pub metering: DfsNetworkMetering,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

impl DfsPlacementPathObservation {
    pub fn freshness_at(&self, now: OffsetDateTime) -> DfsObservationFreshness {
        if now > self.expires_at {
            DfsObservationFreshness::Stale
        } else {
            DfsObservationFreshness::Fresh
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPlacementSnapshot {
    pub schema_version: u32,
    pub generation: u64,
    pub primary_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<DfsPlacementNodeObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<DfsPlacementPathObservation>,
}

impl DfsPlacementSnapshot {
    pub fn empty(primary_node_id: NodeId) -> Self {
        let observed_at = OffsetDateTime::now_utc();
        Self {
            schema_version: DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION,
            generation: 0,
            primary_node_id,
            observed_at,
            expires_at: observed_at + time::Duration::minutes(5),
            nodes: Vec::new(),
            paths: Vec::new(),
        }
    }

    pub fn node(&self, node_id: &NodeId) -> Option<&DfsPlacementNodeObservation> {
        self.nodes.iter().find(|node| &node.node_id == node_id)
    }

    pub fn path(
        &self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
    ) -> Option<&DfsPlacementPathObservation> {
        self.paths.iter().find(|path| {
            &path.source_node_id == source_node_id && &path.target_node_id == target_node_id
        })
    }

    pub fn semantic_digest(&self) -> String {
        format!("blake3:{}", self.semantic_fingerprint().to_hex())
    }

    pub fn semantic_fingerprint(&self) -> blake3::Hash {
        let mut nodes = self.nodes.clone();
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let mut paths = self.paths.clone();
        paths.sort_by(|left, right| {
            left.source_node_id
                .cmp(&right.source_node_id)
                .then_with(|| left.target_node_id.cmp(&right.target_node_id))
        });
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.primary_node_id.as_str().as_bytes());
        for node in nodes {
            hasher.update(node.node_id.as_str().as_bytes());
            if let Some(zone) = &node.zone {
                hasher.update(zone.as_bytes());
            }
            hasher.update(&[
                node.storage_health as u8,
                node.freshness_at(self.observed_at) as u8,
                node.accepts_new_content as u8,
                node.network_metering as u8,
                node.resource_pressure as u8,
            ]);
            hasher.update(&node.capacity_weight.to_be_bytes());
        }
        for path in paths {
            hasher.update(path.source_node_id.as_str().as_bytes());
            hasher.update(path.target_node_id.as_str().as_bytes());
            hasher.update(&[
                path.state as u8,
                path.freshness_at(self.observed_at) as u8,
                path.metering as u8,
            ]);
            hasher.update(
                &path
                    .latency_micros
                    .map(latency_bucket)
                    .unwrap_or_default()
                    .to_be_bytes(),
            );
            hasher.update(
                &path
                    .delivered_bytes_per_second
                    .map(throughput_bucket)
                    .unwrap_or_default()
                    .to_be_bytes(),
            );
        }
        hasher.finalize()
    }
}

#[doc(hidden)]
pub fn throughput_bucket(bytes_per_second: u64) -> u32 {
    64_u32.saturating_sub(bytes_per_second.max(1).leading_zeros())
}

fn latency_bucket(latency_micros: u64) -> u32 {
    64_u32.saturating_sub(latency_micros.max(1).leading_zeros())
}
