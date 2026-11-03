//! Thin, non-authority-owning client facade for Sinomo application surfaces.
//!
//! Public signatures in this crate may use wire contracts and app-facing read models, but never
//! daemon, agent, model, DFS, rollout, or module implementation types.

pub mod agents;
pub mod app_status;
pub mod chat;
pub mod config;
pub mod connection;
pub mod daemon_process;
mod desktop_session;
pub use desktop_session::{AppDaemonSupervisor, connect_to_supervised_daemon};
pub mod evolution;
pub mod modules;
pub mod sifs;
pub mod skills;
pub mod updates;

pub use sinomo_runtime::api;
pub use sinomo_runtime::api::app::agent as agent_contract;
pub use sinomo_runtime::api::app::chat as chat_contract;
pub use sinomo_runtime::api::app::evolution as evolution_contract;
pub use sinomo_runtime::api::app::skill as skill_contract;

pub use agents::AgentManagementClient;
pub use app_status::{AppClusterInvite, AppClusterStatus};
pub use chat::{ChatDaemonClient, ChatHistoryClient};
pub use config::NodeConfigClient;
pub use connection::{
    LocalDaemonConnection, connect_or_start_local_daemon, connect_or_start_local_daemon_with_binary,
};
pub use daemon_process::{OwnedDaemonHandle, StartAppDaemonOutcome, start_app_owned_daemon};
pub use evolution::EvolutionClient;
pub use modules::{ModuleObservationsResponse, read_module_observations};
pub use sifs::*;
pub use skills::SkillClient;
pub use updates::AppUpdateClient;

pub fn daemon_protocol_is_compatible(status: &api::DaemonStatus) -> bool {
    status.api_protocol_version == api::SINOMO_DAEMON_API_PROTOCOL_VERSION
}

pub fn incompatible_daemon_protocol_detail(status: &api::DaemonStatus) -> String {
    format!(
        "daemon protocol {} at {} is incompatible with client protocol {}",
        status.api_protocol_version,
        status.address,
        api::SINOMO_DAEMON_API_PROTOCOL_VERSION
    )
}

#[cfg(test)]
mod dependency_tests {
    use std::collections::BTreeSet;
    use std::process::Command;

    #[test]
    fn normal_client_graph_excludes_authority_owning_implementations() {
        let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("workspace root");
        let output = Command::new(env!("CARGO"))
            .args([
                "tree",
                "-p",
                "sinomo-client",
                "--edges",
                "normal,build",
                "--prefix",
                "none",
            ])
            .current_dir(workspace)
            .output()
            .expect("run cargo tree for sinomo-client");
        assert!(
            output.status.success(),
            "cargo tree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let tree = String::from_utf8_lossy(&output.stdout);
        let packages = tree
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|name| !name.starts_with('('))
            .collect::<BTreeSet<_>>();
        for forbidden in [
            "sinomo-dfs",
            "sinomo-dfs-nfs",
            "sinomo-module",
            "sinomo-observe",
            "sinomo-daemon",
            "sinomo-build",
            "sinomo-rollout",
            "sinomo-sandbox",
            "sinomo-agent",
            "sinomo-lm",
            "sinomo-evolution",
            "sinomo-product-profile",
            "sinomo-cli",
        ] {
            assert!(
                !packages.contains(forbidden),
                "sinomo-client normal graph must not contain {forbidden}"
            );
        }
    }
}

pub mod usage;
pub use sinomo_runtime::api::app::usage as usage_contract;
pub use usage::UsageClient;
