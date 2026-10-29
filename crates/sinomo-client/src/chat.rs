use std::time::Duration;

use sinomo_runtime::api::app::chat as wire;
use sinomo_runtime::api::{DaemonApiClient, DaemonApiError};
use sinomo_runtime::network::ObjectRpcRequest;

#[derive(Clone, Debug)]
pub struct ChatDaemonClient {
    daemon: DaemonApiClient,
    client_id: String,
}

#[derive(Clone, Debug)]
pub struct ChatHistoryClient {
    daemon: DaemonApiClient,
}

impl ChatHistoryClient {
    pub fn at_address(address: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
        }
    }

    pub fn list(&self) -> Result<Vec<wire::ChatHistorySessionSummary>, DaemonApiError> {
        self.call::<_, wire::ChatHistoryListResponse>(
            wire::CHAT_HISTORY_LIST_ROUTE,
            &wire::ChatHistoryListRequest,
        )
        .map(|response| response.sessions)
    }

    pub fn load(
        &self,
        request: wire::ChatHistoryLoadRequest,
    ) -> Result<wire::ChatHistoryLoadResponse, DaemonApiError> {
        self.call(wire::CHAT_HISTORY_LOAD_ROUTE, &request)
    }

    /// The activity (reasoning, tool calls) of the agent operations that
    /// produced history messages.
    pub fn activity(
        &self,
        request: wire::ChatSessionActivityRequest,
    ) -> Result<wire::ChatSessionActivityResponse, DaemonApiError> {
        self.call(wire::CHAT_SESSION_ACTIVITY_ROUTE, &request)
    }

    pub fn channel(
        &self,
        session_id: impl Into<String>,
    ) -> Result<Option<wire::ChatHistoryChannel>, DaemonApiError> {
        self.call::<_, wire::ChatHistoryChannelResponse>(
            wire::CHAT_HISTORY_CHANNEL_ROUTE,
            &wire::ChatHistoryChannelRequest {
                session_id: session_id.into(),
            },
        )
        .map(|response| response.channel)
    }

    pub fn create_channel(
        &self,
        request: wire::ChatHistoryCreateChannelRequest,
    ) -> Result<(), DaemonApiError> {
        self.mutate(wire::CHAT_HISTORY_CREATE_CHANNEL_ROUTE, &request)
    }

    pub fn rename(&self, request: wire::ChatHistoryRenameRequest) -> Result<(), DaemonApiError> {
        self.mutate(wire::CHAT_HISTORY_RENAME_ROUTE, &request)
    }

    pub fn delete(&self, session_id: impl Into<String>) -> Result<(), DaemonApiError> {
        self.mutate(
            wire::CHAT_HISTORY_DELETE_ROUTE,
            &wire::ChatHistoryDeleteRequest {
                session_id: session_id.into(),
            },
        )
    }

    pub fn update_participant(
        &self,
        request: wire::ChatHistoryParticipantRequest,
    ) -> Result<(), DaemonApiError> {
        self.mutate(wire::CHAT_HISTORY_PARTICIPANT_ROUTE, &request)
    }

    pub fn append_agent(
        &self,
        request: wire::ChatHistoryAppendAgentRequest,
    ) -> Result<(), DaemonApiError> {
        self.mutate(wire::CHAT_HISTORY_APPEND_AGENT_ROUTE, &request)
    }

    fn mutate(&self, route: &str, request: &impl serde::Serialize) -> Result<(), DaemonApiError> {
        self.call::<_, wire::ChatHistoryMutationResponse>(route, request)
            .map(|_| ())
    }

    fn call<Req, Resp>(&self, route: &str, request: &Req) -> Result<Resp, DaemonApiError>
    where
        Req: serde::Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let request = ObjectRpcRequest::encode_body(route, wire::CHAT_RPC_CATEGORY, request)?;
        self.daemon
            .call_request(&request)?
            .decode_body()
            .map_err(Into::into)
    }
}

impl ChatDaemonClient {
    pub fn at_address(address: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            daemon: DaemonApiClient::at_address(address),
            client_id: client_id.into(),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.daemon = self.daemon.with_timeout(timeout);
        self
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn open(&self) -> Result<wire::ChatSessionOpenResponse, DaemonApiError> {
        self.attach(None)
    }

    pub fn attach(
        &self,
        execution_id: Option<String>,
    ) -> Result<wire::ChatSessionOpenResponse, DaemonApiError> {
        self.call(
            wire::CHAT_SESSION_OPEN_ROUTE,
            &wire::ChatSessionOpenRequest {
                client_id: self.client_id.clone(),
                execution_id,
            },
        )
    }

    /// Non-blocking counterpart with the same request and response semantics.
    pub async fn attach_async(
        &self,
        execution_id: Option<String>,
    ) -> Result<wire::ChatSessionOpenResponse, DaemonApiError> {
        self.call_async(
            wire::CHAT_SESSION_OPEN_ROUTE,
            &wire::ChatSessionOpenRequest {
                client_id: self.client_id.clone(),
                execution_id,
            },
        )
        .await
    }

    pub fn command(
        &self,
        command: wire::ChatSessionCommand,
    ) -> Result<wire::ChatSessionCommandResponse, DaemonApiError> {
        self.command_with_id(command, None)
    }

    pub fn command_with_id(
        &self,
        command: wire::ChatSessionCommand,
        request_id: Option<String>,
    ) -> Result<wire::ChatSessionCommandResponse, DaemonApiError> {
        self.call(
            wire::CHAT_SESSION_COMMAND_ROUTE,
            &wire::ChatSessionCommandRequest {
                client_id: self.client_id.clone(),
                request_id,
                command,
            },
        )
    }

    /// Non-blocking counterpart with the same request and response semantics.
    pub async fn command_with_id_async(
        &self,
        command: wire::ChatSessionCommand,
        request_id: Option<String>,
    ) -> Result<wire::ChatSessionCommandResponse, DaemonApiError> {
        self.call_async(
            wire::CHAT_SESSION_COMMAND_ROUTE,
            &wire::ChatSessionCommandRequest {
                client_id: self.client_id.clone(),
                request_id,
                command,
            },
        )
        .await
    }

    pub fn events(
        &self,
        after_sequence: u64,
        max_events: usize,
        wait: Duration,
    ) -> Result<wire::ChatSessionEventsResponse, DaemonApiError> {
        self.call(
            wire::CHAT_SESSION_EVENTS_ROUTE,
            &wire::ChatSessionEventsRequest {
                client_id: self.client_id.clone(),
                after_sequence,
                max_events,
                wait_ms: wait.as_millis().min(u64::MAX as u128) as u64,
            },
        )
    }

    /// Non-blocking counterpart with the same request and response semantics.
    pub async fn events_async(
        &self,
        after_sequence: u64,
        max_events: usize,
        wait: Duration,
    ) -> Result<wire::ChatSessionEventsResponse, DaemonApiError> {
        self.call_async(
            wire::CHAT_SESSION_EVENTS_ROUTE,
            &wire::ChatSessionEventsRequest {
                client_id: self.client_id.clone(),
                after_sequence,
                max_events,
                wait_ms: wait.as_millis().min(u64::MAX as u128) as u64,
            },
        )
        .await
    }

    pub fn close(&self) -> Result<wire::ChatSessionCloseResponse, DaemonApiError> {
        self.call(
            wire::CHAT_SESSION_CLOSE_ROUTE,
            &wire::ChatSessionCloseRequest {
                client_id: self.client_id.clone(),
            },
        )
    }

    /// Non-blocking counterpart with the same request and response semantics.
    pub async fn close_async(&self) -> Result<wire::ChatSessionCloseResponse, DaemonApiError> {
        self.call_async(
            wire::CHAT_SESSION_CLOSE_ROUTE,
            &wire::ChatSessionCloseRequest {
                client_id: self.client_id.clone(),
            },
        )
        .await
    }

    fn call<Req, Resp>(&self, route: &str, request: &Req) -> Result<Resp, DaemonApiError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let request = ObjectRpcRequest::encode_body(route, wire::CHAT_RPC_CATEGORY, request)?;
        self.daemon
            .call_request(&request)?
            .decode_body()
            .map_err(Into::into)
    }

    async fn call_async<Req, Resp>(
        &self,
        route: &str,
        request: &Req,
    ) -> Result<Resp, DaemonApiError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let request = ObjectRpcRequest::encode_body(route, wire::CHAT_RPC_CATEGORY, request)?;
        self.daemon
            .call_request_async(&request)
            .await?
            .decode_body()
            .map_err(Into::into)
    }
}
