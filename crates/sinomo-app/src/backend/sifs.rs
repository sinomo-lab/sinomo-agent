//! SIFS / DFS browsing: directory listings, per-object node placement and DFS
//! replication policy. (Wired in a later phase.)

#[cfg(test)]
use sinomo_cli::app_api::SifsAgentChannelStore;
use sinomo_client::{ChatHistoryClient, SifsBrowser};

#[derive(Clone)]
pub struct SifsDaemonAccess {
    address: String,
    runtime: tokio::runtime::Handle,
}

impl SifsDaemonAccess {
    pub fn new(address: impl Into<String>, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            address: address.into(),
            runtime: runtime.clone(),
        }
    }

    pub fn open_browser(&self) -> Result<SifsBrowser, String> {
        SifsBrowser::open_daemon_on_runtime(&self.address, &self.runtime)
    }

    #[cfg(test)]
    pub fn open_agent_channel_store(&self) -> Result<SifsAgentChannelStore, String> {
        sinomo_cli::app_api::SifsBrowser::open_daemon_on_runtime(&self.address, &self.runtime)
            .map(SifsAgentChannelStore::from_browser)
    }

    pub fn history_client(&self) -> ChatHistoryClient {
        ChatHistoryClient::at_address(self.address.clone())
    }
}
