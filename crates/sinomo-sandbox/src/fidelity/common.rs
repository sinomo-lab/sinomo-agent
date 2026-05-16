use crate::SandboxResourceLimits;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxBusyBoxFidelityCase {
    pub name: &'static str,
    pub command: &'static str,
    pub argv: &'static [&'static str],
    pub stdin: Option<&'static str>,
    pub expected_stdout: &'static str,
    pub expected_exit_status: i32,
}

impl SandboxBusyBoxFidelityCase {
    pub fn argv_vec(&self) -> Vec<String> {
        self.argv.iter().map(|value| value.to_string()).collect()
    }

    pub fn stdin_bytes(&self) -> Option<Vec<u8>> {
        self.stdin.map(|value| value.as_bytes().to_vec())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxQuickJsFidelityCase {
    pub name: &'static str,
    pub argv: &'static [&'static str],
    pub setup_files: &'static [(&'static str, &'static str)],
    pub expected_stdout: &'static str,
    pub expected_stderr_contains: Option<&'static str>,
    pub expected_exit_status: i32,
}

impl SandboxQuickJsFidelityCase {
    pub fn argv_vec(&self) -> Vec<String> {
        self.argv.iter().map(|value| value.to_string()).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCommandFidelityCase {
    pub name: &'static str,
    pub command: &'static str,
    pub argv: &'static [&'static str],
    pub stdin: Option<&'static str>,
    pub setup_files: &'static [(&'static str, &'static str)],
    pub expected_stdout: Option<&'static str>,
    pub expected_stdout_contains: Option<&'static str>,
    pub expected_stderr_contains: Option<&'static str>,
    pub expected_exit_status: i32,
}

impl SandboxCommandFidelityCase {
    pub fn argv_vec(&self) -> Vec<String> {
        self.argv.iter().map(|value| value.to_string()).collect()
    }

    pub fn stdin_bytes(&self) -> Option<Vec<u8>> {
        self.stdin.map(|value| value.as_bytes().to_vec())
    }

    pub fn timeout_ms(&self) -> u64 {
        match self.name {
            "python-m-pip-version"
            | "pip-version"
            | "pip-wheel-local-pure-package"
            | "pip-install-local-pure-package"
            | "pip-install-local-simple-index"
            | "pip-native-extension-unsupported"
            | "make-version"
            | "make-dry-run-variable-dependency"
            | "make-recipe-pinned-shell-exec"
            | "make-failed-recipe-status"
            | "tcc-version"
            | "tcc-preprocess-define"
            | "tcc-compile-object"
            | "tcc-invalid-c-diagnostics"
            | "tcc-run-unsupported" => 120_000,
            _ => SandboxResourceLimits::default().wall_time_ms,
        }
    }
}
