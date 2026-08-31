//! Thin typed client for daemon-owned agent definition management.

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_app_contract::agent as contract;
use sinomo_runtime::api::{DaemonApiClient, DaemonApiError};
use sinomo_runtime::network::ObjectRpcRequest;

#[derive(Clone, Debug)]
pub struct AgentManagementClient {
    daemon: DaemonApiClient,
}

impl AgentManagementClient {
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
        }
    }

    pub fn snapshot(
        &self,
        selected_agent_id: Option<String>,
    ) -> Result<contract::AgentConfigSnapshot, DaemonApiError> {
        self.call::<_, contract::AgentSnapshotResponse>(
            contract::AGENT_SNAPSHOT_ROUTE,
            &contract::AgentSnapshotRequest { selected_agent_id },
        )
        .map(|response| response.snapshot)
    }

    pub fn save(
        &self,
        request: contract::AgentSaveRequest,
    ) -> Result<contract::AgentMutationResponse, DaemonApiError> {
        self.call(contract::AGENT_SAVE_ROUTE, &request)
    }

    pub fn delete(&self, id: impl Into<String>) -> Result<bool, DaemonApiError> {
        self.call::<_, contract::AgentDeleteResponse>(
            contract::AGENT_DELETE_ROUTE,
            &contract::AgentDeleteRequest { id: id.into() },
        )
        .map(|response| response.deleted)
    }

    pub fn import(&self, path: std::path::PathBuf) -> Result<String, DaemonApiError> {
        self.call::<_, contract::AgentImportResponse>(
            contract::AGENT_IMPORT_ROUTE,
            &contract::AgentImportRequest { path },
        )
        .map(|response| response.id)
    }

    pub fn export(
        &self,
        id: impl Into<String>,
        output: std::path::PathBuf,
    ) -> Result<std::path::PathBuf, DaemonApiError> {
        self.call::<_, contract::AgentExportResponse>(
            contract::AGENT_EXPORT_ROUTE,
            &contract::AgentExportRequest {
                id: id.into(),
                output,
            },
        )
        .map(|response| response.path)
    }

    pub fn validate(
        &self,
        id: impl Into<String>,
    ) -> Result<contract::AgentValidationReport, DaemonApiError> {
        self.inspect(contract::AGENT_VALIDATE_ROUTE, id.into())
    }

    pub fn test(
        &self,
        id: impl Into<String>,
    ) -> Result<contract::AgentValidationReport, DaemonApiError> {
        self.inspect(contract::AGENT_TEST_ROUTE, id.into())
    }

    fn inspect(
        &self,
        route: &str,
        id: String,
    ) -> Result<contract::AgentValidationReport, DaemonApiError> {
        self.call::<_, contract::AgentInspectResponse>(route, &contract::AgentInspectRequest { id })
            .map(|response| response.report)
    }

    fn call<Req, Resp>(&self, route: &str, request: &Req) -> Result<Resp, DaemonApiError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let request =
            ObjectRpcRequest::encode_body(route, contract::AGENT_MANAGEMENT_RPC_CATEGORY, request)?;
        self.daemon
            .call_request(&request)?
            .decode_body()
            .map_err(Into::into)
    }
}
