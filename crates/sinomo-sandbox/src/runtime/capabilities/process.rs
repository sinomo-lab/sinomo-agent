use super::SandboxRuntimeProcessCapability;
use crate::runtime::{SandboxWasiAbi, default_sandbox_wasi_abi};
use crate::{
    SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY, SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY,
    SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY, SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY,
    SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
};

pub fn sandbox_runtime_process_capabilities() -> Vec<SandboxRuntimeProcessCapability> {
    sandbox_runtime_process_capabilities_for_abi(&default_sandbox_wasi_abi())
}

fn sandbox_runtime_preview1_process_capabilities() -> Vec<SandboxRuntimeProcessCapability> {
    vec![
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY,
            supported: true,
            detail: "the preview1 runner supports a bounded synchronous subprocess bridge that resolves child executables only from the read-only pinned /bin toolchain mount; this is for language-runtime helpers and is not POSIX fork/exec parity",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
            supported: false,
            detail: "the current WASI preview1 runner still lacks POSIX fork/exec/wait, pipes, job control, and general child-process parity; command toolchains must not patch shell semantics around this runtime gap",
        },
    ]
}

pub fn sandbox_runtime_process_capabilities_for_abi(
    target_abi: &SandboxWasiAbi,
) -> Vec<SandboxRuntimeProcessCapability> {
    match target_abi {
        SandboxWasiAbi::Preview1 => sandbox_runtime_preview1_process_capabilities(),
        SandboxWasiAbi::Wasix => sandbox_runtime_wasix_process_capabilities(),
    }
}

#[cfg(feature = "wasix-runtime")]
fn sandbox_runtime_wasix_process_capabilities() -> Vec<SandboxRuntimeProcessCapability> {
    vec![
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
            supported: true,
            detail: "the compiled WASIX runner uses Wasmer's WASIX process substrate; staged /bin commands and PATH lookup run from the pinned toolchain mount",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY,
            supported: true,
            detail: "Sinomo configures the WASIX runner to reject child exec targets outside the pinned /bin toolchain mount, including absolute guest paths from writable mounts",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            supported: true,
            detail: "the WASIX runner can opt out of pinned-only child exec so commands may execute WASM modules and scripts from sandbox-mounted paths such as /work; this still runs through the sandbox virtual filesystem, not host binaries",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY,
            supported: true,
            detail: "the WASIX runner supports proc_fork and proc_fork_env for modules built with the asyncify unwind/rewind hook set; ordinary modules without those hooks must use child exec or posix_spawn paths",
        },
    ]
}

#[cfg(not(feature = "wasix-runtime"))]
fn sandbox_runtime_wasix_process_capabilities() -> Vec<SandboxRuntimeProcessCapability> {
    vec![
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX child-process execution is unavailable",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY,
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX child exec containment is unavailable",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX sandbox-local child exec is unavailable",
        },
        SandboxRuntimeProcessCapability {
            capability: SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY,
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX asyncified fork preflight and execution are unavailable",
        },
    ]
}

pub fn sandbox_runtime_supports_child_processes() -> bool {
    sandbox_runtime_process_capabilities()
        .iter()
        .any(|capability| {
            capability.capability == SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY && capability.supported
        })
}

pub fn sandbox_runtime_supports_child_processes_for_abi(target_abi: &SandboxWasiAbi) -> bool {
    sandbox_runtime_process_capabilities_for_abi(target_abi)
        .iter()
        .any(|capability| {
            capability.capability == SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY && capability.supported
        })
}

pub fn sandbox_runtime_supports_command_capability_for_abi(
    capability_name: &str,
    target_abi: &SandboxWasiAbi,
) -> bool {
    match capability_name {
        SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY => {
            matches!(target_abi, SandboxWasiAbi::Preview1)
        }
        SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY => {
            sandbox_runtime_supports_child_processes_for_abi(target_abi)
        }
        SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY => {
            matches!(target_abi, SandboxWasiAbi::Wasix) && cfg!(feature = "wasix-runtime")
        }
        SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY => {
            matches!(target_abi, SandboxWasiAbi::Wasix) && cfg!(feature = "wasix-runtime")
        }
        SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY => {
            matches!(target_abi, SandboxWasiAbi::Wasix) && cfg!(feature = "wasix-runtime")
        }
        _ => true,
    }
}
