//! Desktop update observation and consent through the daemon-owned rollout service.
use sinomo_runtime::{
    api::{DaemonApiClient, DaemonApiError, app::update::*},
    network::ObjectRpcRequest,
};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct AppUpdateClient {
    daemon: DaemonApiClient,
}

impl AppUpdateClient {
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address).with_timeout(Duration::from_secs(35)),
        }
    }
    pub fn status(
        &self,
        after_revision: Option<u64>,
        wait_ms: u64,
    ) -> Result<AppUpdateStatus, DaemonApiError> {
        self.call(
            APP_UPDATE_STATUS_ROUTE,
            &AppUpdateStatusRequest {
                after_revision,
                wait_ms,
            },
        )
    }
    pub fn restart(
        &self,
        candidate_release: String,
        launch_token: String,
    ) -> Result<AppUpdateStatus, DaemonApiError> {
        self.call(
            APP_UPDATE_RESTART_ROUTE,
            &AppUpdateRestartRequest {
                candidate_release,
                launch_token,
            },
        )
    }
    pub fn ready(&self, launch_token: String) -> Result<AppUpdateStatus, DaemonApiError> {
        self.call(
            APP_UPDATE_READY_ROUTE,
            &AppUpdateReadyRequest {
                launch_token,
                restoration_schema: 1,
            },
        )
    }
    fn call(
        &self,
        route: &str,
        value: &impl serde::Serialize,
    ) -> Result<AppUpdateStatus, DaemonApiError> {
        let request = ObjectRpcRequest::encode_body(route, "rollout", value)?;
        self.daemon
            .call_request(&request)?
            .decode_body()
            .map_err(Into::into)
    }
}
