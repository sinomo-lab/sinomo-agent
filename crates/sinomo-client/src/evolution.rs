//! Thin typed client for daemon-owned self-evolution operations.

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_app_contract::evolution as contract;
use sinomo_runtime::api::{DaemonApiClient, DaemonApiError};
use sinomo_runtime::network::ObjectRpcRequest;

const EVOLUTION_MUTATION_TIMEOUT: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug)]
pub struct EvolutionClient {
    daemon: DaemonApiClient,
}

impl EvolutionClient {
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.daemon = self.daemon.with_timeout(timeout);
        self
    }

    pub fn dashboard(&self) -> Result<contract::EvolutionDashboard, DaemonApiError> {
        self.call::<_, contract::EvolutionDashboardResponse>(
            contract::EVOLUTION_DASHBOARD_ROUTE,
            &contract::EvolutionDashboardRequest,
        )
        .map(|response| response.dashboard)
    }

    pub fn start(
        &self,
        request: contract::EvolutionStartRequest,
    ) -> Result<contract::EvolutionMutationResponse, DaemonApiError> {
        Self {
            daemon: self.daemon.clone().with_timeout(EVOLUTION_MUTATION_TIMEOUT),
        }
        .call(contract::EVOLUTION_START_ROUTE, &request)
    }

    pub fn decide(
        &self,
        request: contract::EvolutionDecisionRequest,
    ) -> Result<contract::EvolutionMutationResponse, DaemonApiError> {
        Self {
            daemon: self.daemon.clone().with_timeout(EVOLUTION_MUTATION_TIMEOUT),
        }
        .call(contract::EVOLUTION_DECISION_ROUTE, &request)
    }

    fn call<Req, Resp>(&self, route: &str, request: &Req) -> Result<Resp, DaemonApiError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let request =
            ObjectRpcRequest::encode_body(route, contract::EVOLUTION_RPC_CATEGORY, request)?;
        self.daemon
            .call_request(&request)?
            .decode_body()
            .map_err(Into::into)
    }
}
