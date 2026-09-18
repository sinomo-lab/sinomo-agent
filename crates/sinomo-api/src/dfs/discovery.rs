//! Shared DFS wire values and validation; no storage or service runtime.

use serde::Deserialize;
use serde::Serialize;
use sinomo_core::node::NodeId;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNodeAdvertisement {
    pub node_id: NodeId,
    #[serde(default)]
    pub capacity_weight: u32,
    #[serde(default)]
    pub accepts_metadata: bool,
    #[serde(default)]
    pub accepts_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    /// Sanitized aggregate storage state. Physical locations remain node-local
    /// observation data and never become independent replica identities.
    #[serde(default)]
    pub storage_health: crate::dfs::DfsHealthState,
    #[serde(default)]
    pub storage_freshness: crate::dfs::DfsObservationFreshness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reserved_free_bytes: Option<u64>,
    #[serde(default)]
    pub content_location_count: u32,
    #[serde(default)]
    pub network_metering: crate::dfs::DfsNetworkMetering,
    /// Sanitized scheduling pressure. Exact power/thermal observations remain
    /// node-local and this field never overrides pins or replication policy.
    #[serde(default)]
    pub resource_pressure: crate::dfs::DfsResourcePressure,
    #[serde(default)]
    pub advertisement_generation: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expires_at: Option<OffsetDateTime>,
}

impl DfsNodeAdvertisement {
    pub fn metadata(node_id: NodeId) -> Self {
        Self {
            node_id,
            capacity_weight: 1,
            accepts_metadata: true,
            accepts_content: false,
            zone: None,
            storage_health: crate::dfs::DfsHealthState::Unknown,
            storage_freshness: crate::dfs::DfsObservationFreshness::NeverSampled,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            content_location_count: 0,
            network_metering: crate::dfs::DfsNetworkMetering::Unknown,
            resource_pressure: crate::dfs::DfsResourcePressure::Normal,
            advertisement_generation: 0,
            updated_at: OffsetDateTime::now_utc(),
            expires_at: None,
        }
    }

    pub fn content(node_id: NodeId) -> Self {
        Self {
            node_id,
            capacity_weight: 1,
            accepts_metadata: false,
            accepts_content: true,
            zone: None,
            storage_health: crate::dfs::DfsHealthState::Unknown,
            storage_freshness: crate::dfs::DfsObservationFreshness::NeverSampled,
            usable_capacity_bytes: None,
            reserved_free_bytes: None,
            content_location_count: 1,
            network_metering: crate::dfs::DfsNetworkMetering::Unknown,
            resource_pressure: crate::dfs::DfsResourcePressure::Normal,
            advertisement_generation: 0,
            updated_at: OffsetDateTime::now_utc(),
            expires_at: None,
        }
    }
}
