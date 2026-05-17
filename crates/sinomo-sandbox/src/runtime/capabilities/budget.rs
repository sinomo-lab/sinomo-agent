use super::SandboxRuntimeBudgetCapability;
use crate::runtime::{SandboxWasiAbi, default_sandbox_wasi_abi};

pub fn sandbox_runtime_budget_capabilities() -> Vec<SandboxRuntimeBudgetCapability> {
    sandbox_runtime_budget_capabilities_for_abi(&default_sandbox_wasi_abi())
}

pub fn sandbox_runtime_budget_capabilities_for_abi(
    target_abi: &SandboxWasiAbi,
) -> Vec<SandboxRuntimeBudgetCapability> {
    match target_abi {
        SandboxWasiAbi::Preview1 => sandbox_runtime_preview1_budget_capabilities(),
        SandboxWasiAbi::Wasix => sandbox_runtime_wasix_budget_capabilities(),
    }
}

fn sandbox_runtime_preview1_budget_capabilities() -> Vec<SandboxRuntimeBudgetCapability> {
    vec![
        SandboxRuntimeBudgetCapability {
            capability: "wall-timeout",
            supported: true,
            detail: "the Wasmtime preview1 runner uses epoch interruption to stop commands after the requested wall-clock timeout",
        },
        SandboxRuntimeBudgetCapability {
            capability: "cancellation",
            supported: true,
            detail: "the Wasmtime preview1 runner watches the Sinomo cancellation token and interrupts the store when cancellation is requested",
        },
        SandboxRuntimeBudgetCapability {
            capability: "output-limit",
            supported: true,
            detail: "the preview1 runner bounds captured stdout and stderr before returning command output",
        },
        SandboxRuntimeBudgetCapability {
            capability: "input-limit",
            supported: true,
            detail: "the preview1 runner rejects direct stdin payloads above the effective sandbox input limit before command startup",
        },
        SandboxRuntimeBudgetCapability {
            capability: "memory-limit",
            supported: true,
            detail: "the Wasmtime preview1 runner installs StoreLimits from the effective sandbox memory budget",
        },
    ]
}

#[cfg(feature = "wasix-runtime")]
fn sandbox_runtime_wasix_budget_capabilities() -> Vec<SandboxRuntimeBudgetCapability> {
    vec![
        SandboxRuntimeBudgetCapability {
            capability: "task-limit",
            supported: true,
            detail: "the WASIX runner configures Wasmer max_threads to cap runtime process/thread fanout for the command tree",
        },
        SandboxRuntimeBudgetCapability {
            capability: "output-limit",
            supported: true,
            detail: "the WASIX runner captures stdout and stderr through Sinomo virtual files that reject guest writes once the per-stream output limit would be exceeded",
        },
        SandboxRuntimeBudgetCapability {
            capability: "input-limit",
            supported: true,
            detail: "the WASIX runner rejects direct stdin payloads above the effective sandbox input limit before command startup",
        },
        SandboxRuntimeBudgetCapability {
            capability: "memory-limit",
            supported: true,
            detail: "the WASIX runner rejects unbounded or over-budget imported/defined linear memories before compile and installs Wasmer tunables that reject host/VM memory creation above the sandbox memory budget",
        },
        SandboxRuntimeBudgetCapability {
            capability: "instruction-budget",
            supported: true,
            detail: "the WASIX runner compiles command modules with Sinomo's multi-module Wasmer metering so standalone, pinned-child, and sandbox-local child-exec commands consume a deterministic instruction budget; modules that import WASIX sched_yield also receive periodic signal checkpoints at metered branches",
        },
        SandboxRuntimeBudgetCapability {
            capability: "process-tree-timeout",
            supported: true,
            detail: "the WASIX runner installs sandbox-owned POSIX RLIMIT metadata on the root process, children inherit those limits across fork/spawn/exec independent of guest env replacement, and the runner signals the root process tree with SIGKILL after the requested wall-clock budget",
        },
        SandboxRuntimeBudgetCapability {
            capability: "process-tree-cancellation",
            supported: true,
            detail: "the WASIX runner polls the Sinomo cancellation token and signals the root process tree with SIGKILL when cancellation is requested",
        },
        SandboxRuntimeBudgetCapability {
            capability: "wall-timeout",
            supported: false,
            detail: "non-yielding compute loops are bounded by deterministic instruction metering, but exact engine-level wall-clock interruption is still pending",
        },
        SandboxRuntimeBudgetCapability {
            capability: "cancellation",
            supported: false,
            detail: "non-yielding WASIX modules that import sched_yield observe cancellation through metered signal checkpoints, and other non-yielding loops remain bounded by deterministic instruction metering; exact engine-level immediate cancellation is still pending",
        },
    ]
}

#[cfg(not(feature = "wasix-runtime"))]
fn sandbox_runtime_wasix_budget_capabilities() -> Vec<SandboxRuntimeBudgetCapability> {
    vec![
        SandboxRuntimeBudgetCapability {
            capability: "task-limit",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so the WASIX task fanout guard is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "output-limit",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so the WASIX streaming output limit guard is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "input-limit",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so the WASIX stdin input limit guard is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "memory-limit",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX memory limit enforcement is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "instruction-budget",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX instruction metering is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "process-tree-timeout",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX process-tree timeout signaling is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "process-tree-cancellation",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX process-tree cancellation signaling is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "wall-timeout",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX timeout enforcement is unavailable",
        },
        SandboxRuntimeBudgetCapability {
            capability: "cancellation",
            supported: false,
            detail: "this binary was built without the `wasix-runtime` feature, so WASIX cancellation is unavailable",
        },
    ]
}
