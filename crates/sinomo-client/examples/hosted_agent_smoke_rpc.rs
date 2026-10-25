//! Read-only preflight for a later hosted-agent smoke; never starts a model turn.
//!
//! Run on the selected node with its existing local-command authority:
//! hosted_agent_smoke_rpc ADDRESS CLUSTER_ID NODE_ID FULL_COMMIT AGENT_ID OUTPUT
//! OUTPUT must be new. An error preserves the partial receipt and is not retried.
//! Compile from the reviewed candidate with its locked dependencies. A successful
//! preflight does not prove quorum, model execution, session continuity or quietness.
use serde_json::json;
use sinomo_client::AgentManagementClient;
use sinomo_runtime::api::{DaemonApiClient, DaemonStatus};
use std::{fs::OpenOptions, io::Write, time::Duration};

fn identity(status: &DaemonStatus, args: &[String]) -> Result<(), String> {
    let cluster = status.cluster.as_ref().ok_or("missing cluster identity")?;
    if cluster.cluster_id.to_string() != args[2]
        || cluster.node_id.to_string() != args[3]
        || cluster.process != sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE
        || status.commit.as_deref() != Some(args[4].as_str())
        || status.target_os.as_deref() != Some("linux")
        || status.pid == 0
        || status.started_at.is_empty()
    {
        return Err("target is not the exact candidate Linux main daemon".into());
    }
    Ok(())
}

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7
        || args[4].len() != 40
        || !args[4].bytes().all(|b| b.is_ascii_hexdigit())
        || args[2..6].iter().any(|value| value.trim().is_empty())
    {
        return Err("usage: hosted_agent_smoke_rpc ADDRESS CLUSTER_ID NODE_ID FULL_COMMIT AGENT_ID NEW_OUTPUT".into());
    }
    // Local-command agent management is intentionally node-local. Do not add a
    // remote shell, routed fallback, or another identity when this is denied.
    let address: std::net::SocketAddr = args[1].parse().map_err(|e| format!("address: {e}"))?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err("run on the selected node and use its literal loopback Main endpoint".into());
    }
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[6])
        .map_err(|e| format!("create receipt: {e}"))?;
    let mut emit = |value: serde_json::Value| -> Result<(), String> {
        writeln!(output, "{value}").map_err(|e| e.to_string())?;
        output.sync_all().map_err(|e| e.to_string())
    };
    emit(json!({"kind":"intent", "schema":1, "address":args[1],
        "cluster_id":args[2], "node_id":args[3], "candidate":args[4], "agent_id":args[5],
        "model_turn_submitted":false}))?;
    let daemon = DaemonApiClient::at_address(&args[1]).with_timeout(Duration::from_secs(10));
    let result = (|| -> Result<(), String> {
        let before = daemon.status().map_err(|e| e.to_string())?;
        emit(json!({"kind":"before", "status":before}))?;
        identity(&before, &args)?;
        let snapshot = AgentManagementClient::at_address(&args[1])
            .snapshot(Some(args[5].clone()))
            .map_err(|e| e.to_string())?;
        emit(json!({"kind":"catalog", "snapshot":snapshot}))?;
        if snapshot.error.is_some()
            || snapshot
                .selected_agent
                .as_ref()
                .map(|agent| agent.id.as_str())
                != Some(args[5].as_str())
        {
            return Err("agent catalog failed or substituted the selected agent".into());
        }
        let after = daemon.status().map_err(|e| e.to_string())?;
        emit(json!({"kind":"after", "status":after}))?;
        identity(&after, &args)?;
        if before.pid != after.pid || before.started_at != after.started_at {
            return Err("daemon process changed during preflight".into());
        }
        Ok(())
    })();
    emit(json!({"kind":"result", "preflight_observed":result.is_ok(),
        "error":result.as_ref().err(), "model_turn_submitted":false,
        "hosted_smoke_passed":false, "rollout_accepted":false}))?;
    result
}
