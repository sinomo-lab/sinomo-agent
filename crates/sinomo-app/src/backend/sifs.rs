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

    pub fn read_image_snapshot(&self, path: &str) -> Result<Vec<u8>, String> {
        let browser = self.open_browser()?;
        let details = browser.stat(path)?;
        let size = details.size_bytes.ok_or("Image size is unavailable")?;
        if details.kind != "file" || size == 0 || size > 32 * 1024 * 1024 {
            return Err("Image is empty, not a regular file, or exceeds 32 MiB".into());
        }
        let hash = details
            .content_hash
            .as_deref()
            .filter(|hash| !hash.is_empty())
            .ok_or("Image content hash is unavailable")?;
        let bytes =
            browser.read_object_version(&details.object_id, &details.version, hash, size)?;
        if bytes.len() as u64 != size {
            return Err("Image snapshot size changed".into());
        }
        Ok(bytes)
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
