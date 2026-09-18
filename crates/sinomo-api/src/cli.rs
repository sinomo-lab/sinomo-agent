//! Local frontend projection and invocation of module-owned commands.
use serde::{Deserialize, Serialize};
pub use sinomo_module_api::cli::*;

pub const CLI_CATALOG_ROUTE: &str = "/module/cli/catalog";
pub const CLI_INVOKE_ROUTE: &str = "/module/cli/invoke";
pub const CLI_RPC_CATEGORY: &str = "module.cli";

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliCatalogRequest;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliCatalogEntry {
    pub module_id: String,
    pub module_version: String,
    pub command: CliCommandDescriptor,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliCatalogResponse {
    /// Unique service incarnation; generation counters alone can repeat after restart.
    pub catalog_id: String,
    pub generation: u64,
    pub commands: Vec<CliCatalogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CliInvokeRequest {
    pub catalog_id: String,
    pub generation: u64,
    pub module_id: String,
    pub request: CliModuleRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum CliInvokeResponse {
    Completed(CliModuleResponse),
    /// Nothing was invoked. The caller may refresh discovery; it must not silently switch owners.
    CatalogChanged,
    Unavailable {
        detail: String,
    },
    ExecutionFailed {
        detail: String,
    },
}
