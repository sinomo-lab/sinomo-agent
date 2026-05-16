#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxRuntimeVirtualDeviceCapability {
    pub device: &'static str,
    pub supported: bool,
    pub detail: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxRuntimeProcessCapability {
    pub capability: &'static str,
    pub supported: bool,
    pub detail: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxRuntimeBudgetCapability {
    pub capability: &'static str,
    pub supported: bool,
    pub detail: &'static str,
}
