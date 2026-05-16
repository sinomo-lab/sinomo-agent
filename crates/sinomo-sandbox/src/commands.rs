use crate::{
    BASH_TOOLCHAIN_COMMAND, PIP_TOOLCHAIN_COMMAND, PYTHON_TOOLCHAIN_COMMAND, SH_TOOLCHAIN_COMMAND,
};

pub const BUSYBOX_TOOLCHAIN_COMMANDS: &[&str] = &[
    "awk",
    "cat",
    "chmod",
    "cp",
    "cmp",
    "date",
    "diff",
    "dirname",
    "echo",
    "env",
    "false",
    "find",
    "grep",
    "gzip",
    "head",
    "ln",
    "ls",
    "mkdir",
    "mv",
    "patch",
    "printf",
    "pwd",
    "rm",
    "rmdir",
    "sed",
    "sha256sum",
    "sort",
    "tail",
    "tar",
    "test",
    "touch",
    "true",
    "tr",
    "uname",
    "uniq",
    "wc",
];

pub const SANDBOX_MVP_COMMANDS: &[&str] = &[
    "sh",
    "cat",
    "echo",
    "printf",
    "pwd",
    "ls",
    "mkdir",
    "rmdir",
    "rm",
    "cp",
    "mv",
    "touch",
    "head",
    "tail",
    "wc",
    "sort",
    "uniq",
    "grep",
    "find",
    "sed",
    "awk",
    "cmp",
    "diff",
    "patch",
    "chmod",
    "ln",
    "tar",
    "gzip",
    "sha256sum",
    "date",
    "env",
    "true",
    "false",
    "test",
    "dirname",
    "tr",
    "uname",
];

pub const SANDBOX_MVP_COMMAND_GAPS: &[SandboxMvpCommandGap] = &[SandboxMvpCommandGap {
    command: "sh",
    reason: "real shell support is provided by the maintained BusyBox ash WASIX package when imported into the pinned toolchain and allowed by child-process-exec plus the current WASIX child-exec gap acknowledgement",
}];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SandboxMvpCommandGap {
    pub command: &'static str,
    pub reason: &'static str,
}

pub fn sandbox_mvp_command_gap(command: &str) -> Option<&'static SandboxMvpCommandGap> {
    SANDBOX_MVP_COMMAND_GAPS
        .iter()
        .find(|gap| gap.command == command)
}

pub fn sandbox_command_requires_child_process_substrate(command_name: &str) -> bool {
    matches!(
        command_name,
        BASH_TOOLCHAIN_COMMAND
            | SH_TOOLCHAIN_COMMAND
            | PYTHON_TOOLCHAIN_COMMAND
            | PIP_TOOLCHAIN_COMMAND
    )
}

pub fn sandbox_env_name_is_valid(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('=')
        && value
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && !value.chars().next().is_some_and(|ch| ch.is_ascii_digit())
}
