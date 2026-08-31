//! Small app-facing projections derived from daemon wire status.

use serde::{Deserialize, Serialize};

use crate::api::DaemonStatus;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AppClusterStatus {
    pub clustered: bool,
    pub cluster_id: Option<String>,
    pub local_node_id: Option<String>,
    pub summary: Option<String>,
    pub error: Option<String>,
}

impl AppClusterStatus {
    pub fn from_daemon_status(status: &DaemonStatus) -> Self {
        let Some(cluster) = &status.cluster else {
            return Self::default();
        };
        let cluster_id = cluster.cluster_id.to_string();
        Self {
            clustered: true,
            cluster_id: Some(cluster_id.clone()),
            local_node_id: Some(cluster.node_id.to_string()),
            summary: Some(format!(
                "cluster {} · {}",
                short_id(&cluster_id, 14),
                format!("{:?}", cluster.member_state).to_lowercase()
            )),
            error: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct AppClusterInvite {
    pub uri: String,
    pub join_command: String,
    pub expires_at: String,
    pub persisted_key: String,
}

fn short_id(id: &str, max_chars: usize) -> String {
    if id.chars().count() <= max_chars {
        id.to_string()
    } else {
        let keep = max_chars.saturating_sub(3);
        format!("{}...", id.chars().take(keep).collect::<String>())
    }
}
