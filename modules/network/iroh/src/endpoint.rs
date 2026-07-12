use serde::{Deserialize, Serialize};
use sinomo_core::network::{ClusterEndpointId, ClusterPaths, NetworkError, write_object_file};
use sinomo_core::node::{ClusterId, NodeId};
use sinomo_core::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource, ClusterTransportKind,
};
use std::path::PathBuf;

pub(crate) const IROH_ENDPOINT_DIR_NAME: &str = "iroh";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IrohEndpointRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub endpoint_public_key: String,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "sinomo_core::network::deserialize_vec"
    )]
    pub direct_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
    pub relay_mode: IrohRelayMode,
    pub expires_after_tick: u64,
}

impl IrohEndpointRecord {
    #[cfg(test)]
    pub fn from_relay_url(
        cluster_id: ClusterId,
        node_id: NodeId,
        endpoint_public_key: impl Into<String>,
        relay_url: impl Into<String>,
    ) -> Self {
        let relay_url = relay_url.into();
        Self {
            cluster_id,
            node_id,
            endpoint_public_key: endpoint_public_key.into(),
            direct_addresses: Vec::new(),
            relay_url: Some(relay_url.clone()),
            relay_mode: IrohRelayMode::Custom { relay_url },
            expires_after_tick: 0,
        }
    }

    pub fn endpoint(&self) -> ClusterEndpointRecord {
        let has_relay = self.relay_url.is_some();
        let address = if has_relay {
            let mut query = Vec::new();
            if let Some(relay_url) = &self.relay_url {
                query.push(format!("relay={relay_url}"));
            }
            format!("iroh://{}?{}", self.endpoint_public_key, query.join("&"))
        } else {
            format!("iroh://{}", self.endpoint_public_key)
        };

        ClusterEndpointRecord {
            endpoint_id: stable_iroh_endpoint_id(&self.node_id, &self.endpoint_public_key),
            node_id: self.node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address,
            source: ClusterEndpointSource::Rendezvous,
            privacy: if has_relay {
                ClusterEndpointPrivacy::Relayed
            } else {
                ClusterEndpointPrivacy::InternetDirect
            },
            priority: 90,
            last_observed_latency_ms: None,
        }
    }

    pub fn write(&self, paths: &ClusterPaths) -> Result<(), NetworkError> {
        write_iroh_endpoint_record(paths, self)
    }
}

pub fn stable_iroh_endpoint_id(node_id: &NodeId, endpoint_public_key: &str) -> ClusterEndpointId {
    ClusterEndpointId::derived(
        "sinomo.iroh.endpoint",
        format!("{}\0{endpoint_public_key}", node_id.as_str()),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum IrohRelayMode {
    Disabled,
    Default,
    Custom { relay_url: String },
}

pub(crate) fn iroh_endpoint_dir(paths: &ClusterPaths) -> PathBuf {
    paths.root.join(IROH_ENDPOINT_DIR_NAME)
}

fn iroh_endpoint_record_path(paths: &ClusterPaths, node_id: &NodeId) -> PathBuf {
    iroh_endpoint_dir(paths).join(format!("{}.endpoint.sobj", node_id.as_str()))
}

pub(crate) fn write_iroh_endpoint_record(
    paths: &ClusterPaths,
    record: &IrohEndpointRecord,
) -> Result<(), NetworkError> {
    write_object_file(&iroh_endpoint_record_path(paths, &record.node_id), record)
}

pub fn iroh_endpoint_record_from_address(
    address: &str,
    cluster_id: &ClusterId,
    node_id: &NodeId,
) -> Result<IrohEndpointRecord, NetworkError> {
    let rest = address
        .strip_prefix("iroh://")
        .ok_or_else(|| NetworkError::Serialization {
            detail: format!("invalid iroh endpoint address `{address}`"),
        })?;
    let (public_key, query) = rest.split_once('?').unwrap_or((rest, ""));
    if public_key.trim().is_empty() {
        return Err(NetworkError::Serialization {
            detail: format!("invalid iroh endpoint address `{address}`"),
        });
    }
    let mut direct_addresses = Vec::new();
    let mut relay_url = None;
    for part in query.split('&').filter(|part| !part.trim().is_empty()) {
        if let Some(value) = part.strip_prefix("direct=") {
            direct_addresses.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
            );
        } else if let Some(value) = part.strip_prefix("relay=")
            && !value.trim().is_empty()
        {
            relay_url = Some(value.to_string());
        }
    }
    let relay_mode = relay_url
        .as_ref()
        .map(|relay_url| IrohRelayMode::Custom {
            relay_url: relay_url.clone(),
        })
        .unwrap_or(IrohRelayMode::Default);
    Ok(IrohEndpointRecord {
        cluster_id: cluster_id.clone(),
        node_id: node_id.clone(),
        endpoint_public_key: public_key.to_string(),
        direct_addresses,
        relay_url,
        relay_mode,
        expires_after_tick: 0,
    })
}

pub fn iroh_endpoint_public_key(endpoint: &ClusterEndpointRecord) -> Option<String> {
    if endpoint.transport != ClusterTransportKind::Iroh {
        return None;
    }
    let rest = endpoint.address.strip_prefix("iroh://")?;
    let public_key = rest.split(['?', '#']).next()?.trim();
    if public_key.is_empty() {
        None
    } else {
        Some(public_key.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_record_round_trips_address() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_iroh_endpoint_record").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_iroh_endpoint_record").expect("node");
        let record = IrohEndpointRecord::from_relay_url(
            cluster_id.clone(),
            node_id.clone(),
            "public-key",
            "https://relay.example/",
        );
        let endpoint = record.endpoint();
        let parsed = iroh_endpoint_record_from_address(&endpoint.address, &cluster_id, &node_id)
            .expect("parse endpoint");
        assert_eq!(parsed.endpoint_public_key, "public-key");
        assert_eq!(parsed.relay_url.as_deref(), Some("https://relay.example/"));
    }
}
