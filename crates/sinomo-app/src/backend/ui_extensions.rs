//! Read-only client for the daemon's generation-stamped UI extension catalog.

use std::time::Duration;

use sinomo_client::api::DaemonApiClient;
use sinomo_runtime::module::{
    MODULE_UI_CATALOG_ROUTE, MODULE_UI_RPC_CATEGORY, UiExtensionCatalogRequest,
    UiExtensionCatalogResponse,
};
use sinomo_runtime::network::ObjectRpcRequest;

use crate::backend::Backend;

const UI_EXTENSION_CATALOG_TIMEOUT: Duration = Duration::from_secs(5);

impl Backend {
    pub fn ui_extension_catalog(
        &self,
        extension_point_id: Option<&str>,
    ) -> Result<UiExtensionCatalogResponse, String> {
        let Some(address) = self.connection.daemon_address() else {
            return Ok(UiExtensionCatalogResponse {
                registry_generation: 0,
                entries: Vec::new(),
                warnings: Vec::new(),
            });
        };
        let request = ObjectRpcRequest::encode_body(
            MODULE_UI_CATALOG_ROUTE,
            MODULE_UI_RPC_CATEGORY,
            &UiExtensionCatalogRequest {
                extension_point_id: extension_point_id.map(str::to_string),
            },
        )
        .map_err(|error| error.to_string())?;
        let response = DaemonApiClient::at_address(address)
            .with_timeout(UI_EXTENSION_CATALOG_TIMEOUT)
            .call_request(&request)
            .map_err(|error| error.to_string())?;
        response.decode_body().map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemonless_backend_has_an_empty_ui_catalog() {
        let response = Backend::local_fallback()
            .ui_extension_catalog(Some(
                sinomo_runtime::module_api::FILE_PREVIEW_EXTENSION_POINT_ID,
            ))
            .expect("daemonless catalog");
        assert_eq!(response.registry_generation, 0);
        assert!(response.entries.is_empty());
    }
}
