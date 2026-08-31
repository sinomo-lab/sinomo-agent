//! Thin typed client for daemon-owned skill catalog operations.

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_app_contract::skill as contract;
use sinomo_runtime::api::{DaemonApiClient, DaemonApiError};
use sinomo_runtime::network::ObjectRpcRequest;

#[derive(Clone, Debug)]
pub struct SkillClient {
    daemon: DaemonApiClient,
}

impl SkillClient {
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
        }
    }

    pub fn list(&self, include_all: bool) -> Result<Vec<contract::SkillSummary>, DaemonApiError> {
        self.call::<_, contract::SkillListResponse>(
            contract::SKILL_LIST_ROUTE,
            &contract::SkillListRequest { include_all },
        )
        .map(|response| response.skills)
    }

    pub fn detail(
        &self,
        skill_id: impl Into<String>,
        version: Option<String>,
    ) -> Result<contract::SkillDetail, DaemonApiError> {
        self.call::<_, contract::SkillDetailResponse>(
            contract::SKILL_DETAIL_ROUTE,
            &contract::SkillDetailRequest {
                skill_id: skill_id.into(),
                version,
            },
        )
        .map(|response| response.detail)
    }

    pub fn import(
        &self,
        request: contract::SkillImportRequest,
    ) -> Result<contract::SkillMutationResult, DaemonApiError> {
        self.call::<_, contract::SkillMutationResponse>(contract::SKILL_IMPORT_ROUTE, &request)
            .map(|response| response.result)
    }

    pub fn export(
        &self,
        request: contract::SkillExportRequest,
    ) -> Result<contract::SkillMutationResult, DaemonApiError> {
        self.call::<_, contract::SkillMutationResponse>(contract::SKILL_EXPORT_ROUTE, &request)
            .map(|response| response.result)
    }

    pub fn activate(
        &self,
        request: contract::SkillActivateRequest,
    ) -> Result<contract::SkillMutationResult, DaemonApiError> {
        self.call::<_, contract::SkillMutationResponse>(contract::SKILL_ACTIVATE_ROUTE, &request)
            .map(|response| response.result)
    }

    fn call<Req, Resp>(&self, route: &str, request: &Req) -> Result<Resp, DaemonApiError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let request = ObjectRpcRequest::encode_body(route, contract::SKILL_RPC_CATEGORY, request)?;
        self.daemon
            .call_request(&request)?
            .decode_body()
            .map_err(Into::into)
    }
}
