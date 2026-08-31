//! Thin app adapter for daemon-owned self-evolution operations.

use std::sync::mpsc::{Receiver, channel};
use std::thread;

use sinomo_client::EvolutionClient;
pub use sinomo_client::evolution_contract::{
    EvolutionDashboard, EvolutionDecisionAction, EvolutionDecisionRequest,
    EvolutionDecisionSummary, EvolutionJobSummary, EvolutionPhase, EvolutionStartRequest,
};
use sinomo_runtime::local::SinomoDataDir;

#[derive(Clone, Debug)]
pub enum EvolutionStartEvent {
    Started,
    Finished(Result<(), String>),
}

#[derive(Clone, Debug)]
pub enum EvolutionDecisionEvent {
    Started,
    Finished(Result<(), String>),
}

pub fn spawn_evolution_job(
    daemon_address: Option<String>,
    request: EvolutionStartRequest,
) -> Receiver<EvolutionStartEvent> {
    let (tx, rx) = channel();
    let failure_tx = tx.clone();
    if let Err(error) = thread::Builder::new()
        .name("sinomo-app-evolution-start".to_string())
        .spawn(move || {
            let _ = tx.send(EvolutionStartEvent::Started);
            let result = daemon_address
                .ok_or_else(|| "evolution requires a daemon connection".to_string())
                .and_then(|address| {
                    EvolutionClient::at_address(address)
                        .start(request)
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                });
            let _ = tx.send(EvolutionStartEvent::Finished(result));
        })
    {
        let _ = failure_tx.send(EvolutionStartEvent::Finished(Err(format!(
            "failed to start evolution worker: {error}"
        ))));
    }
    rx
}

pub fn spawn_evolution_decision(
    daemon_address: Option<String>,
    request: EvolutionDecisionRequest,
) -> Receiver<EvolutionDecisionEvent> {
    let (tx, rx) = channel();
    let failure_tx = tx.clone();
    if let Err(error) = thread::Builder::new()
        .name("sinomo-app-evolution-decision".to_string())
        .spawn(move || {
            let _ = tx.send(EvolutionDecisionEvent::Started);
            let result = daemon_address
                .ok_or_else(|| "evolution requires a daemon connection".to_string())
                .and_then(|address| {
                    EvolutionClient::at_address(address)
                        .decide(request)
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                });
            let _ = tx.send(EvolutionDecisionEvent::Finished(result));
        })
    {
        let _ = failure_tx.send(EvolutionDecisionEvent::Finished(Err(format!(
            "failed to start evolution decision worker: {error}"
        ))));
    }
    rx
}

pub fn load_evolution_dashboard(
    daemon_address: Option<&str>,
    data_dir: &SinomoDataDir,
) -> EvolutionDashboard {
    let source = data_dir.root().join("evolution").display().to_string();
    let Some(address) = daemon_address else {
        return EvolutionDashboard {
            source,
            jobs: Vec::new(),
            error: Some("Evolution requires a daemon connection.".to_string()),
        };
    };
    EvolutionClient::at_address(address)
        .dashboard()
        .unwrap_or_else(|error| EvolutionDashboard {
            source,
            jobs: Vec::new(),
            error: Some(error.to_string()),
        })
}
