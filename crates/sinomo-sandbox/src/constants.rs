use sinomo_core::Id;

pub type SandboxId = Id;
pub type SandboxCommandId = Id;
pub type SandboxCheckpointId = Id;
pub type SandboxToolchainId = Id;
pub type SandboxNetworkRequestId = Id;

pub const SIFS_SANDBOX_ROOT: &str = "/services/sandbox";
pub const SANDBOX_POLICY_CONFIG_KEY: &str = "sandbox.policy";
pub const SANDBOX_POLICY_CONFIG_KV_KEY: &str = "/config/sandbox/policy";
pub const SANDBOX_STORAGE_MODULE: &str = "sinomo-sandbox";
pub const SANDBOX_MANIFEST_OBJECT_VERSION: u16 = 1;
pub const SANDBOX_TOOLCHAIN_MANIFEST_OBJECT_VERSION: u16 = 1;
pub const SANDBOX_HTTP_EXCHANGE_OBJECT_VERSION: u16 = 1;
pub const SANDBOX_MODULE_ARTIFACT_REGISTRY_OBJECT_VERSION: u16 = 1;
pub const BUSYBOX_TOOLCHAIN_LEGACY_ID: &str = "toolchain_busybox";
pub const BASH_TOOLCHAIN_COMMAND: &str = "bash";
pub const SH_TOOLCHAIN_COMMAND: &str = "sh";
pub const BASH_TOOLCHAIN_RECIPE: &str = "crates/sinomo-sandbox/toolchains/bash/build-bash-wasix.sh";
pub const PYTHON_TOOLCHAIN_COMMAND: &str = "python";
pub const PIP_TOOLCHAIN_COMMAND: &str = "pip";
pub const PYTHON_TOOLCHAIN_RECIPE: &str =
    "crates/sinomo-sandbox/toolchains/python/build-cpython-wasix.sh";
pub const PYTHON_WASIX_NATIVE_VERIFIER: &str =
    "crates/sinomo-sandbox/toolchains/python/verify-python-wasix-native.sh";
pub const BUILD_TOOLS_TOOLCHAIN_PLAN: &str =
    "crates/sinomo-sandbox/toolchains/build-tools/README.md";
pub const TCC_TOOLCHAIN_RECIPE: &str =
    "crates/sinomo-sandbox/toolchains/build-tools/build-tcc-wasix.sh";
pub const TCC_TOOLCHAIN_COMMAND: &str = "tcc";
pub const MAKE_TOOLCHAIN_RECIPE: &str =
    "crates/sinomo-sandbox/toolchains/build-tools/build-make-wasix.sh";
pub const MAKE_TOOLCHAIN_COMMAND: &str = "make";
pub const MAKEPKG_TOOLCHAIN_RECIPE: &str =
    "crates/sinomo-sandbox/toolchains/build-tools/build-makepkg-wasix.sh";
pub const MAKEPKG_TOOLCHAIN_COMMAND: &str = "makepkg";
pub const CLANG_TOOLCHAIN_RECIPE: &str =
    "crates/sinomo-sandbox/toolchains/build-tools/build-clang-wasix.sh";
pub const CLANG_TOOLCHAIN_COMMAND: &str = "clang";
pub const SANDBOX_OUTPUT_CHUNK_BYTES: usize = 64 * 1024;
pub const SANDBOX_WASIX_MAX_TASKS: usize = 2048;
pub const SANDBOX_NETWORK_HTTP_CAPABILITY: &str = "network.http";
pub const SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY: &str = "child-process-exec";
pub const SANDBOX_WASIX_ASYNCIFIED_FORK_CAPABILITY: &str = "wasix.asyncified-fork";
pub const SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY: &str = "wasix.unpinned-child-exec";
pub const SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY: &str = "pinned-child-exec-containment";
pub const SANDBOX_MVP_VIRTUAL_DEVICES: &[&str] = &[
    "/dev/null",
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/dev/full",
    "/dev/stdin",
    "/dev/stdout",
    "/dev/stderr",
    "/dev/tty",
    "/dev/console",
    "/dev/ptmx",
    "/dev/fd",
    "/dev/fd/0",
    "/dev/fd/1",
    "/dev/fd/2",
    "/dev/pts",
    "/dev/shm",
];
pub const SANDBOX_WASI_ZERO_DEVICE_BYTES: usize = 1024 * 1024;

pub const SANDBOX_RICH_TOOLCHAIN_TARGETS: &[SandboxRichToolchainTarget] = &[
    SandboxRichToolchainTarget {
        command: BASH_TOOLCHAIN_COMMAND,
        recipe: Some(BASH_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Required,
        note: "Bash is required for the coding sandbox; the maintained WASIX package passes local and daemon-local SIFS verification when child-process-exec and sandbox-local WASIX child exec are allowed by policy",
    },
    SandboxRichToolchainTarget {
        command: PYTHON_TOOLCHAIN_COMMAND,
        recipe: Some(PYTHON_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Required,
        note: "Python is required for the coding sandbox; the maintained CPython WASIX package has standard library support plus native-extension loading needed by numpy and matplotlib",
    },
    SandboxRichToolchainTarget {
        command: PIP_TOOLCHAIN_COMMAND,
        recipe: Some(PYTHON_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Required,
        note: "pip is required for the coding sandbox; offline wheel installs, local wheelhouse installs, SIFS-backed cache state, and native-extension package installs for numpy and matplotlib are required, while broader resolver edge cases remain pending",
    },
    SandboxRichToolchainTarget {
        command: TCC_TOOLCHAIN_COMMAND,
        recipe: Some(TCC_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Implemented,
        note: "TinyCC has a maintained WASIX build recipe and passes the compiler fidelity suite for version output, preprocessing, object generation in /work, diagnostics, and explicit unsupported execution",
    },
    SandboxRichToolchainTarget {
        command: MAKE_TOOLCHAIN_COMMAND,
        recipe: Some(MAKE_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Implemented,
        note: "GNU Make has a maintained WASIX build recipe and passes the combined local plus daemon-local SIFS verify-build-tools gate for Makefile parsing, dependency ordering, failure status, and recipe execution through pinned sandbox commands",
    },
    SandboxRichToolchainTarget {
        command: MAKEPKG_TOOLCHAIN_COMMAND,
        recipe: Some(MAKEPKG_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Required,
        note: "makepkg is packaged from upstream pacman scripts with sandbox /usr, /etc, and /var paths. The command starts, reports its upstream version, and can load Bash programmable-completion features such as compgen, but broad PKGBUILD execution remains gated on package-build dependencies such as pacman/libalpm, libarchive, file, gettext, fakeroot, compression tools, and mediated fetchers",
    },
    SandboxRichToolchainTarget {
        command: CLANG_TOOLCHAIN_COMMAND,
        recipe: Some(CLANG_TOOLCHAIN_RECIPE),
        status: SandboxRichToolchainStatus::Required,
        note: "Clang/LLVM is the maintained compiler target for the coding sandbox. The recipe builds LLVM's Clang, LLD, and binutils-style helpers as WASIX command modules and packages the shared sysroot/resource artifacts; the heavyweight verifier is opt-in with VERIFY_CLANG=1 until the full LLVM build is part of the default gate",
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxRichToolchainStatus {
    Implemented,
    Required,
    Planned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxRichToolchainTarget {
    pub command: &'static str,
    pub recipe: Option<&'static str>,
    pub status: SandboxRichToolchainStatus,
    pub note: &'static str,
}
