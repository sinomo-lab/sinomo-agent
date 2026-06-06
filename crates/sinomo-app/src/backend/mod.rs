//! The app's view of the Sinomo runtime.
//!
//! On launch the app probes for a local daemon. If one is running we attach to it
//! ("connected" mode). Otherwise — unlike the CLI, which would just error — the app
//! becomes a node itself: it initializes the local data directory and operates
//! daemonless with the default **mobile** role, staying as useful as possible in
//! local mode. The embedded node lives in this process and is torn down on quit.

pub mod chat_engine;
pub mod config;
pub mod sessions;
pub mod sifs;

use std::sync::Arc;
use std::time::Duration;

use sinomo_runtime::daemon::{DaemonPaths, DaemonProbeStatus, DaemonStatus, probe_daemon_status};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::{NodeProfile, NodeRole};

const DAEMON_PROBE_TIMEOUT: Duration = Duration::from_millis(400);

/// How the app is currently talking to the runtime.
#[derive(Clone)]
pub enum Connection {
    /// Attached to a running local daemon (and, through it, possibly a cluster).
    Daemon(Box<DaemonStatus>),
    /// No daemon found; running as an embedded in-process node (role: mobile).
    EmbeddedNode,
}

impl Connection {
    pub fn is_daemon(&self) -> bool {
        matches!(self, Connection::Daemon(_))
    }

    pub fn is_clustered(&self) -> bool {
        matches!(self, Connection::Daemon(status) if status.cluster.is_some())
    }
}

/// Shared, cheaply-cloneable handle to runtime facilities.
#[derive(Clone)]
pub struct Backend {
    pub data_dir: Arc<SinomoDataDir>,
    pub node: NodeProfile,
    pub connection: Connection,
}

impl Backend {
    /// Probe for a daemon and, failing that, stand up an embedded mobile node.
    pub fn connect() -> Self {
        let data_dir = SinomoDataDir::from_env_or_home()
            .unwrap_or_else(|_| SinomoDataDir::new(std::env::temp_dir().join("sinomo")));
        // Best-effort: a missing layout shouldn't stop the app from opening.
        let _ = data_dir.ensure_root_layout();

        let mut node = NodeProfile::local("Sinomo App")
            .unwrap_or_else(|_| panic!("local node profile uses a valid trust domain"));
        node.role = NodeRole::Mobile;

        let paths = DaemonPaths::new(&data_dir);
        let connection = match probe_daemon_status(&paths, DAEMON_PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status) => Connection::Daemon(Box::new(status)),
            DaemonProbeStatus::Locked {
                status: Some(status),
            } => Connection::Daemon(Box::new(status)),
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                Connection::EmbeddedNode
            }
        };

        Backend {
            data_dir: Arc::new(data_dir),
            node,
            connection,
        }
    }

    /// Short label for the title-bar status pill.
    pub fn mode_label(&self) -> String {
        match &self.connection {
            Connection::Daemon(status) => {
                if status.cluster.is_some() {
                    "Cluster".to_string()
                } else {
                    "Daemon".to_string()
                }
            }
            Connection::EmbeddedNode => "Local · mobile".to_string(),
        }
    }

    /// One-line human description of the current connection.
    pub fn status_detail(&self) -> String {
        match &self.connection {
            Connection::Daemon(status) => match &status.cluster {
                Some(cluster) => format!(
                    "Connected to daemon at {} · cluster {} · {}",
                    status.address,
                    short_id(&format!("{:?}", cluster.cluster_id)),
                    format!("{:?}", cluster.member_role).to_lowercase(),
                ),
                None => format!("Connected to local daemon at {}", status.address),
            },
            Connection::EmbeddedNode => {
                "Embedded node (mobile role) · daemonless local mode".to_string()
            }
        }
    }

    /// Whether the runtime is healthy enough to talk to.
    pub fn is_online(&self) -> bool {
        true
    }
}

fn short_id(id: &str) -> String {
    let trimmed = id.trim();
    if trimmed.len() <= 10 {
        trimmed.to_string()
    } else {
        format!("{}…", &trimmed[..10])
    }
}
