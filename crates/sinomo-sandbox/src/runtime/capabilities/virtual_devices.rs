use super::types::SandboxRuntimeVirtualDeviceCapability;
use crate::SANDBOX_MVP_VIRTUAL_DEVICES;

pub fn sandbox_runtime_mvp_virtual_device_capabilities()
-> Vec<SandboxRuntimeVirtualDeviceCapability> {
    SANDBOX_MVP_VIRTUAL_DEVICES
        .iter()
        .map(|device| SandboxRuntimeVirtualDeviceCapability {
            device,
            supported: true,
            detail: "the WASI runners install a per-run Sinomo-owned /dev preopen with /dev/null, bounded /dev/zero, and seeded /dev/random plus /dev/urandom; it never maps host device paths",
        })
        .collect()
}

pub fn sandbox_runtime_supports_mvp_virtual_devices() -> bool {
    sandbox_runtime_mvp_virtual_device_capabilities()
        .iter()
        .all(|capability| capability.supported)
}

pub fn sandbox_runtime_disables_ambient_network() -> bool {
    true
}
