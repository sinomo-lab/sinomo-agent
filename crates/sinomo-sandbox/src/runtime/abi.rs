use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxWasiAbi {
    Wasix,
}

pub fn default_sandbox_wasi_abi() -> SandboxWasiAbi {
    SandboxWasiAbi::Wasix
}
