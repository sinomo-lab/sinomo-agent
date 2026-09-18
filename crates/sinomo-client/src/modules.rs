//! Module observations obtained from the owning daemon.

use std::time::Duration;

use sinomo_runtime::module::{
    MODULE_OBSERVATIONS_ROUTE, MODULE_UI_RPC_CATEGORY, ModuleObservationsRequest,
};
use sinomo_runtime::network::ObjectRpcRequest;

pub use sinomo_runtime::module::ModuleObservationsResponse;

pub fn read_module_observations(
    address: &str,
    timeout: Duration,
) -> Result<ModuleObservationsResponse, String> {
    let request = ObjectRpcRequest::encode_body(
        MODULE_OBSERVATIONS_ROUTE,
        MODULE_UI_RPC_CATEGORY,
        &ModuleObservationsRequest::default(),
    )
    .map_err(|error| error.to_string())?;
    let response = crate::api::send_daemon_rpc(address, &request, timeout)
        .map_err(|error| error.to_string())?;
    response.decode_body().map_err(|error| error.to_string())
}

/// Submit a local module-setting intent to the daemon that owns its persistence.
pub fn set_native_rust_module_enabled(
    address: &str,
    module_id: &str,
    enabled: bool,
    reason: &str,
    timeout: Duration,
) -> Result<sinomo_runtime::api::module::NativeRustModuleStatus, String> {
    use sinomo_runtime::api::module::{
        MODULE_NATIVE_RUST_RPC_CATEGORY, MODULE_NATIVE_RUST_SET_ENABLED_ROUTE,
        NativeRustModuleSetEnabledRequest, NativeRustModuleSetEnabledResponse,
    };
    let request = ObjectRpcRequest::encode_body(
        MODULE_NATIVE_RUST_SET_ENABLED_ROUTE,
        MODULE_NATIVE_RUST_RPC_CATEGORY,
        &NativeRustModuleSetEnabledRequest {
            module_id: module_id.to_string(),
            enabled,
            reason: reason.to_string(),
        },
    )
    .map_err(|error| error.to_string())?;
    let response: NativeRustModuleSetEnabledResponse =
        crate::api::send_daemon_rpc(address, &request, timeout)
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())?;
    Ok(response.module)
}
