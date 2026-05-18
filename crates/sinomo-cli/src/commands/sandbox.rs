use super::*;
use sinomo_runtime::events::EventStore;
use sinomo_runtime::sandbox::SandboxFileSystem;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SandboxCliCommand {
    Help,
    Config(SandboxConfigOptions),
    Create(SandboxCreateOptions),
    Cat(SandboxCatOptions),
    Checkpoint(SandboxCheckpointOptions),
    Destroy(SandboxDestroyOptions),
    Doctor(SandboxDoctorOptions),
    Fetch(SandboxFetchOptions),
    Ls(SandboxLsOptions),
    Logs(SandboxLogsOptions),
    Policy(SandboxPolicyCliCommand),
    ProcessPlan(SandboxProcessPlanOptions),
    Reboot(SandboxRebootOptions),
    Resume(SandboxResumeOptions),
    Run(SandboxRunOptions),
    Shell(SandboxShellOptions),
    Status(SandboxStatusOptions),
    Write(SandboxWriteOptions),
    ToolchainAddCommand(SandboxToolchainAddCommandOptions),
    ToolchainAddArtifact(SandboxToolchainAddArtifactOptions),
    ToolchainBashPlan,
    ToolchainBuildToolsPlan,
    ToolchainBusyBoxPlan,
    ToolchainHash(SandboxToolchainHashOptions),
    ToolchainImportBusyBox(SandboxToolchainImportBusyBoxOptions),
    ToolchainImportPackage(SandboxToolchainImportPackageOptions),
    ToolchainPythonPlan,
    ToolchainQuickJsPlan,
    ToolchainVerifyPackage(SandboxToolchainVerifyPackageOptions),
    ToolchainVerifyBash(SandboxToolchainVerifyBashOptions),
    ToolchainVerifyBusyBox(SandboxToolchainVerifyBusyBoxOptions),
    ToolchainVerifyCoding(SandboxToolchainVerifyCodingOptions),
    ToolchainVerifyMake(SandboxToolchainVerifyMakeOptions),
    ToolchainVerifyPython(SandboxToolchainVerifyPythonOptions),
    ToolchainVerifyTcc(SandboxToolchainVerifyTccOptions),
    ToolchainVerifyQuickJs(SandboxToolchainVerifyQuickJsOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxConfigOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) cwd: Option<sinomo_runtime::sandbox::SandboxPath>,
    pub(crate) env_set: BTreeMap<String, String>,
    pub(crate) env_unset: Vec<String>,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SandboxPolicyCliCommand {
    Show,
    Set(SandboxPolicySetOptions),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct SandboxPolicySetOptions {
    pub(crate) allowed_mount_prefixes: Vec<String>,
    pub(crate) allow_unsigned_toolchains: Option<bool>,
    pub(crate) allow_remote_local_owned_mounts: Option<bool>,
    pub(crate) max_memory_mb: Option<u64>,
    pub(crate) max_wall_time_ms: Option<u64>,
    pub(crate) max_input_bytes: Option<u64>,
    pub(crate) max_output_bytes: Option<u64>,
    pub(crate) max_workspace_bytes: Option<u64>,
    pub(crate) max_written_bytes_per_command: Option<u64>,
    pub(crate) max_file_count: Option<u64>,
    pub(crate) max_http_timeout_ms: Option<u64>,
    pub(crate) max_concurrent_commands_per_sandbox: Option<u64>,
    pub(crate) max_concurrent_sandboxes_per_node: Option<u64>,
    pub(crate) allowed_capabilities: Vec<String>,
    pub(crate) allowed_toolchains: Vec<sinomo_runtime::sandbox::SandboxToolchainId>,
    pub(crate) allowed_toolchain_provenance_prefixes: Vec<String>,
    pub(crate) allowed_package_cache_prefixes: Vec<String>,
    pub(crate) allowed_http_hosts: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxCreateOptions {
    pub(crate) sandbox_id: Option<sinomo_runtime::sandbox::SandboxId>,
    pub(crate) toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId,
    pub(crate) temp_policy: sinomo_runtime::sandbox::SandboxTempPolicy,
    pub(crate) memory_mb: Option<u64>,
    pub(crate) wall_time_ms: Option<u64>,
    pub(crate) max_input_bytes: Option<u64>,
    pub(crate) max_output_bytes: Option<u64>,
    pub(crate) max_workspace_bytes: Option<u64>,
    pub(crate) max_written_bytes_per_command: Option<u64>,
    pub(crate) max_file_count: Option<u64>,
    pub(crate) mounts: Vec<sinomo_runtime::sandbox::SandboxMount>,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxStatusOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxDoctorOptions {
    pub(crate) sandbox_id: Option<sinomo_runtime::sandbox::SandboxId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxDoctorFinding {
    pub(crate) severity: SandboxDoctorSeverity,
    pub(crate) code: String,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SandboxDoctorSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxLsOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) path: sinomo_runtime::sandbox::SandboxPath,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxCatOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) path: sinomo_runtime::sandbox::SandboxPath,
    pub(crate) range: Option<sinomo_runtime::sandbox::ByteRange>,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SandboxWriteInput {
    Literal(Vec<u8>),
    HostFile(PathBuf),
    Stdin,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxWriteOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) path: sinomo_runtime::sandbox::SandboxPath,
    pub(crate) input: SandboxWriteInput,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxFetchOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) url: String,
    pub(crate) output_path: sinomo_runtime::sandbox::SandboxPath,
    pub(crate) timeout_ms: u64,
    pub(crate) max_response_bytes: u64,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxCheckpointOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxResumeOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) checkpoint_id: sinomo_runtime::sandbox::SandboxCheckpointId,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxRebootOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxDestroyOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxRunOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) target: SandboxRunTarget,
    pub(crate) argv: Vec<String>,
    pub(crate) timeout_ms: u64,
    pub(crate) stdin: bool,
    pub(crate) allow_unsigned: bool,
    pub(crate) allowed_capabilities: Vec<String>,
    pub(crate) execution: SandboxRunExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxProcessPlanOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) plan_path: PathBuf,
    pub(crate) timeout_ms: u64,
    pub(crate) execution: SandboxRunExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxShellOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) script: Option<String>,
    pub(crate) timeout_ms: u64,
    pub(crate) execution: SandboxRunExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum SandboxRunTarget {
    ToolchainCommand(String),
    WasmModule(PathBuf),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SandboxRunExecution {
    Local,
    Daemon,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxLogsOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) command_id: Option<sinomo_runtime::sandbox::SandboxCommandId>,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainAddCommandOptions {
    pub(crate) toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId,
    pub(crate) version: String,
    pub(crate) target_abi: sinomo_runtime::sandbox::SandboxWasiAbi,
    pub(crate) provenance: String,
    pub(crate) command_name: String,
    pub(crate) module_path: PathBuf,
    pub(crate) execution: SandboxMetadataExecution,
    pub(crate) required_capabilities: Vec<String>,
    pub(crate) default_limits: sinomo_runtime::sandbox::SandboxResourceLimits,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainAddArtifactOptions {
    pub(crate) toolchain_id: sinomo_runtime::sandbox::SandboxToolchainId,
    pub(crate) version: String,
    pub(crate) provenance: String,
    pub(crate) relative_path: String,
    pub(crate) source_path: PathBuf,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainHashOptions {
    pub(crate) path: PathBuf,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainImportBusyBoxOptions {
    pub(crate) version: String,
    pub(crate) provenance: String,
    pub(crate) module_path: PathBuf,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainImportPackageOptions {
    pub(crate) package_path: PathBuf,
    pub(crate) target_toolchain_id: Option<sinomo_runtime::sandbox::SandboxToolchainId>,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyPackageOptions {
    pub(crate) package_path: PathBuf,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum SandboxMetadataExecution {
    Local,
    Daemon,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyBusyBoxOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyBashOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyCodingOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
    pub(crate) full: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyMakeOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyTccOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyPythonOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct SandboxToolchainVerifyQuickJsOptions {
    pub(crate) sandbox_id: sinomo_runtime::sandbox::SandboxId,
    pub(crate) allow_unsigned: bool,
    pub(crate) execution: SandboxMetadataExecution,
}

pub(crate) fn parse_sandbox_command(args: &[String]) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        None => Ok(SandboxCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(SandboxCliCommand::Help)
        }
        Some((command, rest)) if command == "create" => Ok(SandboxCliCommand::Create(
            parse_sandbox_create_options(rest)?,
        )),
        Some((command, rest)) if command == "config" => Ok(SandboxCliCommand::Config(
            parse_sandbox_config_options(rest)?,
        )),
        Some((command, rest)) if command == "run" => {
            Ok(SandboxCliCommand::Run(parse_sandbox_run_options(rest)?))
        }
        Some((command, rest)) if command == "process-plan" => Ok(SandboxCliCommand::ProcessPlan(
            parse_sandbox_process_plan_options(rest)?,
        )),
        Some((command, rest)) if command == "shell" => {
            Ok(SandboxCliCommand::Shell(parse_sandbox_shell_options(rest)?))
        }
        Some((command, rest)) if command == "logs" => {
            Ok(SandboxCliCommand::Logs(parse_sandbox_logs_options(rest)?))
        }
        Some((command, rest)) if command == "doctor" => Ok(SandboxCliCommand::Doctor(
            parse_sandbox_doctor_options(rest)?,
        )),
        Some((command, rest)) if command == "policy" => parse_sandbox_policy_command(rest),
        Some((command, rest)) if command == "ls" => {
            Ok(SandboxCliCommand::Ls(parse_sandbox_ls_options(rest)?))
        }
        Some((command, rest)) if command == "cat" => {
            Ok(SandboxCliCommand::Cat(parse_sandbox_cat_options(rest)?))
        }
        Some((command, rest)) if command == "write" => {
            Ok(SandboxCliCommand::Write(parse_sandbox_write_options(rest)?))
        }
        Some((command, rest)) if command == "fetch" => {
            Ok(SandboxCliCommand::Fetch(parse_sandbox_fetch_options(rest)?))
        }
        Some((command, rest)) if command == "checkpoint" => Ok(SandboxCliCommand::Checkpoint(
            parse_sandbox_checkpoint_options(rest)?,
        )),
        Some((command, rest)) if command == "resume" => Ok(SandboxCliCommand::Resume(
            parse_sandbox_resume_options(rest)?,
        )),
        Some((command, rest)) if command == "reboot" => Ok(SandboxCliCommand::Reboot(
            parse_sandbox_reboot_options(rest)?,
        )),
        Some((command, rest)) if command == "destroy" => Ok(SandboxCliCommand::Destroy(
            parse_sandbox_destroy_options(rest)?,
        )),
        Some((command, rest)) if command == "status" => {
            let (sandbox_id, rest) = rest.split_first().ok_or(CliError::Usage)?;
            let mut execution = SandboxMetadataExecution::Local;
            for arg in rest {
                match arg.as_str() {
                    "--daemon" => execution = SandboxMetadataExecution::Daemon,
                    _ => return Err(CliError::Usage),
                }
            }
            Ok(SandboxCliCommand::Status(SandboxStatusOptions {
                sandbox_id: parse_sandbox_id(sandbox_id)?,
                execution,
            }))
        }
        Some((command, rest)) if command == "toolchain" => parse_sandbox_toolchain_command(rest),
        Some((command, _)) => Err(CliError::Parse {
            detail: format!(
                "sandbox command `{command}` is not implemented yet; run `sinomo sandbox help`"
            ),
        }),
    }
}

pub(crate) fn parse_sandbox_policy_command(args: &[String]) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        Some((command, [])) if command == "show" => {
            Ok(SandboxCliCommand::Policy(SandboxPolicyCliCommand::Show))
        }
        Some((command, rest)) if command == "set" => Ok(SandboxCliCommand::Policy(
            SandboxPolicyCliCommand::Set(parse_sandbox_policy_set_options(rest)?),
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_sandbox_doctor_options(
    args: &[String],
) -> Result<SandboxDoctorOptions, CliError> {
    match args {
        [] => Ok(SandboxDoctorOptions { sandbox_id: None }),
        [sandbox_id] => Ok(SandboxDoctorOptions {
            sandbox_id: Some(parse_sandbox_id(sandbox_id)?),
        }),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_sandbox_config_options(
    args: &[String],
) -> Result<SandboxConfigOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut options = SandboxConfigOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        cwd: None,
        env_set: BTreeMap::new(),
        env_unset: Vec::new(),
        execution: SandboxMetadataExecution::Local,
    };
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => options.execution = SandboxMetadataExecution::Daemon,
            "--cwd" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cwd = Some(sinomo_runtime::sandbox::SandboxPath::new(value.clone())?);
            }
            "--env" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                let (key, env_value) = parse_sandbox_env_assignment(value)?;
                options.env_set.insert(key, env_value);
            }
            "--unset-env" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_env_name(value)?;
                options.env_unset.push(value.clone());
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    if options.cwd.is_none() && options.env_set.is_empty() && options.env_unset.is_empty() {
        return Err(CliError::Usage);
    }
    Ok(options)
}

pub(crate) fn parse_sandbox_policy_set_options(
    args: &[String],
) -> Result<SandboxPolicySetOptions, CliError> {
    let mut options = SandboxPolicySetOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--allow-unsigned-toolchains" => options.allow_unsigned_toolchains = Some(true),
            "--require-signed-toolchains" => options.allow_unsigned_toolchains = Some(false),
            "--allow-remote-local-owned-mounts" => {
                options.allow_remote_local_owned_mounts = Some(true)
            }
            "--deny-remote-local-owned-mounts" => {
                options.allow_remote_local_owned_mounts = Some(false)
            }
            "--max-memory-mb" => {
                index += 1;
                options.max_memory_mb = Some(parse_sandbox_memory_policy_value(
                    args.get(index),
                    "max memory",
                )?);
            }
            "--max-wall-time-ms" => {
                index += 1;
                options.max_wall_time_ms = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max wall time",
                )?);
            }
            "--max-output-bytes" => {
                index += 1;
                options.max_output_bytes = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max output bytes",
                )?);
            }
            "--max-input-bytes" => {
                index += 1;
                options.max_input_bytes = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max input bytes",
                )?);
            }
            "--max-workspace-bytes" => {
                index += 1;
                options.max_workspace_bytes = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max workspace bytes",
                )?);
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                options.max_written_bytes_per_command = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max written bytes per command",
                )?);
            }
            "--max-file-count" => {
                index += 1;
                options.max_file_count = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max file count",
                )?);
            }
            "--max-http-timeout-ms" => {
                index += 1;
                options.max_http_timeout_ms = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "max HTTP timeout",
                )?);
            }
            "--max-concurrent-commands-per-sandbox" => {
                index += 1;
                options.max_concurrent_commands_per_sandbox = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max concurrent commands per sandbox",
                )?);
            }
            "--max-concurrent-sandboxes-per-node" => {
                index += 1;
                options.max_concurrent_sandboxes_per_node = Some(parse_u64_sandbox_policy_value(
                    args.get(index),
                    "max concurrent sandboxes per node",
                )?);
            }
            "--allow-capability" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_command_capability(value, "sandbox capability")?;
                options.allowed_capabilities.push(value.clone());
            }
            "--allow-toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.allowed_toolchains.push(parse_toolchain_id(value)?);
            }
            "--allow-toolchain-provenance-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain provenance prefix cannot be empty".to_string(),
                    });
                }
                if value.contains('\0') {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain provenance prefix contains an embedded NUL byte"
                            .to_string(),
                    });
                }
                options
                    .allowed_toolchain_provenance_prefixes
                    .push(value.clone());
            }
            "--allow-package-cache-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options
                    .allowed_package_cache_prefixes
                    .push(parse_sifs_mount_prefix(value)?);
            }
            "--allow-http-host" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.allowed_http_hosts.push(parse_http_host(value)?);
            }
            "--allow-mount-prefix" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options
                    .allowed_mount_prefixes
                    .push(parse_sifs_mount_prefix(value)?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_u64_sandbox_policy_value(value: Option<&String>, name: &str) -> Result<u64, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    if value == "unlimited" {
        return Ok(u64::MAX);
    }
    value.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox {name} `{value}`: {source}"),
    })
}

fn parse_sandbox_memory_policy_value(value: Option<&String>, name: &str) -> Result<u64, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    if matches!(value.as_str(), "host" | "available" | "all") {
        return Ok(sinomo_runtime::sandbox::SandboxResourceLimits::default().memory_mb);
    }
    parse_u64_sandbox_policy_value(Some(value), name)
}

fn parse_nonzero_u64_sandbox_cli_value(
    value: Option<&String>,
    name: &str,
) -> Result<u64, CliError> {
    let parsed = parse_u64_sandbox_policy_value(value, name)?;
    if parsed == 0 {
        return Err(CliError::Parse {
            detail: format!("sandbox {name} must be non-zero"),
        });
    }
    Ok(parsed)
}

pub(crate) fn parse_sandbox_checkpoint_options(
    args: &[String],
) -> Result<SandboxCheckpointOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxCheckpointOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        execution,
    })
}

pub(crate) fn parse_sandbox_resume_options(
    args: &[String],
) -> Result<SandboxResumeOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let (checkpoint_id, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxResumeOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        checkpoint_id: parse_sandbox_checkpoint_id(checkpoint_id)?,
        execution,
    })
}

pub(crate) fn parse_sandbox_reboot_options(
    args: &[String],
) -> Result<SandboxRebootOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxRebootOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        execution,
    })
}

pub(crate) fn parse_sandbox_destroy_options(
    args: &[String],
) -> Result<SandboxDestroyOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxDestroyOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        execution,
    })
}

pub(crate) fn parse_sandbox_ls_options(args: &[String]) -> Result<SandboxLsOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut path = None;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            value if path.is_none() => path = Some(value),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxLsOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        path: parse_sandbox_path(path.unwrap_or("/"))?,
        execution,
    })
}

pub(crate) fn parse_sandbox_cat_options(args: &[String]) -> Result<SandboxCatOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut path = None;
    let mut range = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxMetadataExecution::Daemon;
                index += 1;
            }
            "--range" => {
                index += 1;
                range = Some(parse_sandbox_byte_range(rest.get(index))?);
                index += 1;
            }
            value if path.is_none() => {
                path = Some(value);
                index += 1;
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxCatOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        path: parse_sandbox_path(path.ok_or(CliError::Usage)?)?,
        range,
        execution,
    })
}

fn parse_sandbox_byte_range(
    value: Option<&String>,
) -> Result<sinomo_runtime::sandbox::ByteRange, CliError> {
    let Some(value) = value else {
        return Err(CliError::Usage);
    };
    let (start, end) = value.split_once(':').ok_or_else(|| CliError::Parse {
        detail: format!("invalid sandbox byte range `{value}`; expected start:end"),
    })?;
    if start.is_empty() {
        return Err(CliError::Parse {
            detail: format!("invalid sandbox byte range `{value}`; start is required"),
        });
    }
    let start = start.parse::<u64>().map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox byte range start `{start}`: {source}"),
    })?;
    let end = if end.is_empty() {
        None
    } else {
        let end = end.parse::<u64>().map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox byte range end `{end}`: {source}"),
        })?;
        if end < start {
            return Err(CliError::Parse {
                detail: format!("invalid sandbox byte range `{value}`; end is before start"),
            });
        }
        Some(end)
    };
    Ok(sinomo_runtime::sandbox::ByteRange { start, end })
}

pub(crate) fn parse_sandbox_write_options(
    args: &[String],
) -> Result<SandboxWriteOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let (path, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut input = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxMetadataExecution::Daemon;
                index += 1;
            }
            "--stdin" => {
                if input.is_some() {
                    return Err(CliError::Usage);
                }
                input = Some(SandboxWriteInput::Stdin);
                index += 1;
            }
            "--file" => {
                let Some(path) = rest.get(index + 1) else {
                    return Err(CliError::Usage);
                };
                if input.is_some() {
                    return Err(CliError::Usage);
                }
                input = Some(SandboxWriteInput::HostFile(PathBuf::from(path)));
                index += 2;
            }
            value => {
                if input.is_some() {
                    return Err(CliError::Usage);
                }
                input = Some(SandboxWriteInput::Literal(value.as_bytes().to_vec()));
                index += 1;
            }
        }
    }
    Ok(SandboxWriteOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        path: parse_sandbox_path(path)?,
        input: input.unwrap_or(SandboxWriteInput::Stdin),
        execution,
    })
}

pub(crate) fn parse_sandbox_fetch_options(
    args: &[String],
) -> Result<SandboxFetchOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let (url, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let (output_path, rest) = rest.split_first().ok_or(CliError::Usage)?;
    let mut timeout_ms = sinomo_runtime::sandbox::default_sandbox_http_timeout_ms();
    let mut max_response_bytes = sinomo_runtime::sandbox::default_sandbox_http_response_bytes();
    let mut headers = BTreeMap::new();
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxMetadataExecution::Daemon;
            }
            "--timeout-ms" => {
                index += 1;
                timeout_ms = parse_u64_sandbox_policy_value(rest.get(index), "HTTP timeout")?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox HTTP timeout must be non-zero".to_string(),
                    });
                }
            }
            "--max-response-bytes" => {
                index += 1;
                max_response_bytes =
                    parse_u64_sandbox_policy_value(rest.get(index), "HTTP response bytes")?;
                if max_response_bytes == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox HTTP max response bytes must be non-zero".to_string(),
                    });
                }
            }
            "--header" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                let (name, value) = parse_http_header(value)?;
                headers.insert(name, value);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxFetchOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        url: url.clone(),
        output_path: parse_sandbox_path(output_path)?,
        timeout_ms,
        max_response_bytes,
        headers,
        execution,
    })
}

pub(crate) fn parse_sandbox_shell_options(
    args: &[String],
) -> Result<SandboxShellOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    let mut timeout_ms = 0;
    let mut script = None;
    let mut execution = SandboxRunExecution::Local;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxRunExecution::Daemon;
                index += 1;
            }
            "--timeout-ms" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                timeout_ms = value.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox timeout `{value}`: {source}"),
                })?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox timeout must be non-zero".to_string(),
                    });
                }
                index += 1;
            }
            "--" => {
                script = Some(rest[index + 1..].join(" "));
                index = rest.len();
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxShellOptions {
        sandbox_id,
        script,
        timeout_ms,
        execution,
    })
}

pub(crate) fn parse_sandbox_run_options(args: &[String]) -> Result<SandboxRunOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    let mut timeout_ms = 0;
    let mut target = None;
    let mut execution = SandboxRunExecution::Local;
    let mut stdin = false;
    let mut allow_unsigned = false;
    let mut allowed_capabilities = Vec::new();
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--daemon" => {
                execution = SandboxRunExecution::Daemon;
                index += 1;
            }
            "--timeout-ms" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                timeout_ms = value.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox timeout `{value}`: {source}"),
                })?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox timeout must be non-zero".to_string(),
                    });
                }
                index += 1;
            }
            "--stdin" => {
                stdin = true;
                index += 1;
            }
            "--allow-unsigned" => {
                allow_unsigned = true;
                index += 1;
            }
            "--allow-capability" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox run command capability must be non-empty".to_string(),
                    });
                }
                allowed_capabilities.push(value.to_string());
                index += 1;
            }
            "--wasm" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                target = Some(SandboxRunTarget::WasmModule(PathBuf::from(value)));
                index += 1;
            }
            "--" => {
                index += 1;
                break;
            }
            value if target.is_none() => {
                target = Some(SandboxRunTarget::ToolchainCommand(value.to_string()));
                index += 1;
            }
            _ => return Err(CliError::Usage),
        }
    }
    let target = target.ok_or(CliError::Usage)?;
    if execution == SandboxRunExecution::Daemon && matches!(target, SandboxRunTarget::WasmModule(_))
    {
        return Err(CliError::Parse {
            detail: "sandbox daemon execution requires a pinned toolchain command, not --wasm"
                .to_string(),
        });
    }
    if execution == SandboxRunExecution::Daemon
        && (allow_unsigned || !allowed_capabilities.is_empty())
    {
        return Err(CliError::Parse {
            detail: "--allow-unsigned and --allow-capability only apply to local sandbox runs; daemon-backed runs use the cluster sandbox policy (`sinomo sandbox policy set ...`)"
                .to_string(),
        });
    }
    if matches!(&target, SandboxRunTarget::WasmModule(_)) && !allowed_capabilities.is_empty() {
        return Err(CliError::Parse {
            detail: "--allow-capability applies to pinned toolchain commands, not explicit --wasm modules"
                .to_string(),
        });
    }
    let argv0 = match &target {
        SandboxRunTarget::ToolchainCommand(command) => command.clone(),
        SandboxRunTarget::WasmModule(module_path) => module_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sandbox-command.wasm")
            .to_string(),
    };
    let mut argv = vec![argv0];
    argv.extend(rest[index..].iter().cloned());
    Ok(SandboxRunOptions {
        sandbox_id,
        target,
        argv,
        timeout_ms,
        stdin,
        allow_unsigned: allow_unsigned || execution == SandboxRunExecution::Local,
        allowed_capabilities,
        execution,
    })
}

pub(crate) fn parse_sandbox_process_plan_options(
    args: &[String],
) -> Result<SandboxProcessPlanOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    let mut timeout_ms = sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms;
    let mut execution = SandboxRunExecution::Local;
    let mut plan_path = None;
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--timeout-ms" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                timeout_ms = value.parse::<u64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox timeout `{value}`: {source}"),
                })?;
                if timeout_ms == 0 {
                    return Err(CliError::Parse {
                        detail: "sandbox timeout must be non-zero".to_string(),
                    });
                }
                index += 1;
            }
            "--daemon" => {
                execution = SandboxRunExecution::Daemon;
                index += 1;
            }
            value if plan_path.is_none() => {
                plan_path = Some(PathBuf::from(value));
                index += 1;
            }
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxProcessPlanOptions {
        sandbox_id,
        plan_path: plan_path.ok_or(CliError::Usage)?,
        timeout_ms,
        execution,
    })
}

pub(crate) fn parse_sandbox_logs_options(args: &[String]) -> Result<SandboxLogsOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut execution = SandboxMetadataExecution::Local;
    let mut command_id = None;
    for arg in rest {
        match arg.as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            value if command_id.is_none() => command_id = Some(parse_sandbox_command_id(value)?),
            _ => return Err(CliError::Usage),
        }
    }
    Ok(SandboxLogsOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        command_id,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_command(
    args: &[String],
) -> Result<SandboxCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "add-command" => {
            Ok(SandboxCliCommand::ToolchainAddCommand(
                parse_sandbox_toolchain_add_command_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "add-artifact" => {
            Ok(SandboxCliCommand::ToolchainAddArtifact(
                parse_sandbox_toolchain_add_artifact_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "import-busybox" => {
            Ok(SandboxCliCommand::ToolchainImportBusyBox(
                parse_sandbox_toolchain_import_busybox_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "import-package" => {
            Ok(SandboxCliCommand::ToolchainImportPackage(
                parse_sandbox_toolchain_import_package_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-package" => {
            Ok(SandboxCliCommand::ToolchainVerifyPackage(
                parse_sandbox_toolchain_verify_package_options(rest)?,
            ))
        }
        Some((command, [])) if command == "busybox-plan" => {
            Ok(SandboxCliCommand::ToolchainBusyBoxPlan)
        }
        Some((command, [])) if command == "bash-plan" => Ok(SandboxCliCommand::ToolchainBashPlan),
        Some((command, [])) if command == "build-tools-plan" => {
            Ok(SandboxCliCommand::ToolchainBuildToolsPlan)
        }
        Some((command, [])) if command == "quickjs-plan" => {
            Ok(SandboxCliCommand::ToolchainQuickJsPlan)
        }
        Some((command, [])) if command == "python-plan" => {
            Ok(SandboxCliCommand::ToolchainPythonPlan)
        }
        Some((command, rest)) if command == "hash" => Ok(SandboxCliCommand::ToolchainHash(
            parse_sandbox_toolchain_hash_options(rest)?,
        )),
        Some((command, rest)) if command == "verify-bash" => {
            Ok(SandboxCliCommand::ToolchainVerifyBash(
                parse_sandbox_toolchain_verify_bash_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-busybox" => {
            Ok(SandboxCliCommand::ToolchainVerifyBusyBox(
                parse_sandbox_toolchain_verify_busybox_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-coding" => {
            Ok(SandboxCliCommand::ToolchainVerifyCoding(
                parse_sandbox_toolchain_verify_coding_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-make" => {
            Ok(SandboxCliCommand::ToolchainVerifyMake(
                parse_sandbox_toolchain_verify_make_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-tcc" => {
            Ok(SandboxCliCommand::ToolchainVerifyTcc(
                parse_sandbox_toolchain_verify_tcc_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-quickjs" => {
            Ok(SandboxCliCommand::ToolchainVerifyQuickJs(
                parse_sandbox_toolchain_verify_quickjs_options(rest)?,
            ))
        }
        Some((command, rest)) if command == "verify-python" => {
            Ok(SandboxCliCommand::ToolchainVerifyPython(
                parse_sandbox_toolchain_verify_python_options(rest)?,
            ))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_sandbox_toolchain_import_package_options(
    args: &[String],
) -> Result<SandboxToolchainImportPackageOptions, CliError> {
    let mut package_path = None;
    let mut target_toolchain_id = None;
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--into" | "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                target_toolchain_id = Some(parse_toolchain_id(value)?);
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value if package_path.is_none() => package_path = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxToolchainImportPackageOptions {
        package_path: package_path.ok_or(CliError::Usage)?,
        target_toolchain_id,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_hash_options(
    args: &[String],
) -> Result<SandboxToolchainHashOptions, CliError> {
    let (path, rest) = args.split_first().ok_or(CliError::Usage)?;
    if !rest.is_empty() || path.starts_with('-') {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainHashOptions {
        path: PathBuf::from(path),
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_package_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyPackageOptions, CliError> {
    let (path, rest) = args.split_first().ok_or(CliError::Usage)?;
    if !rest.is_empty() || path.starts_with('-') {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainVerifyPackageOptions {
        package_path: PathBuf::from(path),
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_busybox_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyBusyBoxOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyBusyBoxOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_bash_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyBashOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyBashOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_quickjs_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyQuickJsOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyQuickJsOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_python_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyPythonOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyPythonOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_coding_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyCodingOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    let mut full = false;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--full" => full = true,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyCodingOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
        full,
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_make_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyMakeOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyMakeOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_verify_tcc_options(
    args: &[String],
) -> Result<SandboxToolchainVerifyTccOptions, CliError> {
    let (sandbox_id, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut allow_unsigned = false;
    let mut execution = SandboxMetadataExecution::Local;
    for arg in rest {
        match arg.as_str() {
            "--allow-unsigned" => allow_unsigned = true,
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            _ => return Err(CliError::Usage),
        }
    }
    validate_sandbox_verify_unsigned_scope(allow_unsigned, execution)?;
    Ok(SandboxToolchainVerifyTccOptions {
        sandbox_id: parse_sandbox_id(sandbox_id)?,
        allow_unsigned,
        execution,
    })
}

fn validate_sandbox_verify_unsigned_scope(
    allow_unsigned: bool,
    execution: SandboxMetadataExecution,
) -> Result<(), CliError> {
    if allow_unsigned && execution == SandboxMetadataExecution::Daemon {
        return Err(CliError::Parse {
            detail: "--allow-unsigned only applies to local verification; daemon-backed verification uses the cluster sandbox policy (`sinomo sandbox policy set --allow-unsigned-toolchains`)"
                .to_string(),
        });
    }
    Ok(())
}

pub(crate) fn parse_sandbox_toolchain_import_busybox_options(
    args: &[String],
) -> Result<SandboxToolchainImportBusyBoxOptions, CliError> {
    let mut version = None;
    let mut provenance = "Sinomo-maintained BusyBox WASI module import".to_string();
    let mut module_path = None;
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "BusyBox toolchain version must be non-empty".to_string(),
                    });
                }
                version = Some(value.clone());
            }
            "--provenance" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                provenance = value.clone();
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value if module_path.is_none() => module_path = Some(PathBuf::from(value)),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxToolchainImportBusyBoxOptions {
        version: version.ok_or(CliError::Usage)?,
        provenance,
        module_path: module_path.ok_or(CliError::Usage)?,
        execution,
    })
}

pub(crate) fn parse_sandbox_toolchain_add_command_options(
    args: &[String],
) -> Result<SandboxToolchainAddCommandOptions, CliError> {
    let mut toolchain_id =
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("static toolchain id");
    let mut version = "local".to_string();
    let mut target_abi = sinomo_runtime::sandbox::default_sandbox_wasi_abi();
    let mut provenance = "local CLI import".to_string();
    let mut execution = SandboxMetadataExecution::Local;
    let mut required_capabilities = Vec::new();
    let mut default_limits = sinomo_runtime::sandbox::SandboxResourceLimits::default();
    let mut positionals = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                toolchain_id = parse_toolchain_id(value)?;
            }
            "--version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain version must be non-empty".to_string(),
                    });
                }
                version = value.clone();
            }
            "--target-abi" | "--abi" => {
                index += 1;
                target_abi = parse_sandbox_wasi_abi(args.get(index))?;
            }
            "--provenance" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                provenance = value.clone();
            }
            "--capability" | "--require-capability" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                validate_sandbox_command_capability(value, "sandbox toolchain command capability")?;
                required_capabilities.push(value.clone());
            }
            "--memory-mb" => {
                index += 1;
                default_limits.memory_mb =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command memory mb")?;
            }
            "--wall-time-ms" => {
                index += 1;
                default_limits.wall_time_ms =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command wall time")?;
            }
            "--max-output-bytes" => {
                index += 1;
                default_limits.max_output_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command output bytes")?;
            }
            "--max-input-bytes" => {
                index += 1;
                default_limits.max_input_bytes =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command input bytes")?;
            }
            "--max-workspace-bytes" => {
                index += 1;
                default_limits.max_workspace_bytes = parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "command workspace bytes",
                )?;
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                default_limits.max_written_bytes_per_command =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command written bytes")?;
            }
            "--max-file-count" => {
                index += 1;
                default_limits.max_file_count =
                    parse_nonzero_u64_sandbox_cli_value(args.get(index), "command file count")?;
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }
    if positionals.len() != 2 {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainAddCommandOptions {
        toolchain_id,
        version,
        target_abi,
        provenance,
        command_name: positionals[0].clone(),
        module_path: PathBuf::from(&positionals[1]),
        execution,
        required_capabilities,
        default_limits,
    })
}

fn parse_sandbox_wasi_abi(
    value: Option<&String>,
) -> Result<sinomo_runtime::sandbox::SandboxWasiAbi, CliError> {
    match value.map(String::as_str) {
        Some("preview1") | Some("wasi-preview1") | Some("wasi") => {
            Ok(sinomo_runtime::sandbox::SandboxWasiAbi::Preview1)
        }
        Some("wasix") => Ok(sinomo_runtime::sandbox::SandboxWasiAbi::Wasix),
        Some(value) => Err(CliError::Parse {
            detail: format!("unsupported sandbox WASI target ABI `{value}`"),
        }),
        None => Err(CliError::Usage),
    }
}

pub(crate) fn parse_sandbox_toolchain_add_artifact_options(
    args: &[String],
) -> Result<SandboxToolchainAddArtifactOptions, CliError> {
    let mut toolchain_id =
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("static toolchain id");
    let mut version = "local".to_string();
    let mut provenance = "local CLI import".to_string();
    let mut execution = SandboxMetadataExecution::Local;
    let mut positionals = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                toolchain_id = parse_toolchain_id(value)?;
            }
            "--version" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                if value.trim().is_empty() {
                    return Err(CliError::Parse {
                        detail: "sandbox toolchain version must be non-empty".to_string(),
                    });
                }
                version = value.clone();
            }
            "--provenance" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                provenance = value.clone();
            }
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => positionals.push(value.to_string()),
        }
        index += 1;
    }
    if positionals.len() != 2 {
        return Err(CliError::Usage);
    }
    Ok(SandboxToolchainAddArtifactOptions {
        toolchain_id,
        version,
        provenance,
        relative_path: positionals[0].clone(),
        source_path: PathBuf::from(&positionals[1]),
        execution,
    })
}

pub(crate) fn parse_sandbox_create_options(
    args: &[String],
) -> Result<SandboxCreateOptions, CliError> {
    let mut sandbox_id = None;
    let mut toolchain_id =
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("toolchain_busybox")
            .expect("static toolchain id");
    let mut temp_policy = sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart;
    let mut memory_mb = None;
    let mut wall_time_ms = None;
    let mut max_input_bytes = None;
    let mut max_output_bytes = None;
    let mut max_workspace_bytes = None;
    let mut max_written_bytes_per_command = None;
    let mut max_file_count = None;
    let mut mounts = Vec::new();
    let mut execution = SandboxMetadataExecution::Local;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--daemon" => execution = SandboxMetadataExecution::Daemon,
            "--id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                sandbox_id = Some(parse_sandbox_id(value)?);
            }
            "--toolchain" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                toolchain_id = parse_toolchain_id(value)?;
            }
            "--memory-mb" => {
                index += 1;
                memory_mb = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "memory mb",
                )?);
            }
            "--wall-time-ms" => {
                index += 1;
                wall_time_ms = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "wall time",
                )?);
            }
            "--max-output-bytes" => {
                index += 1;
                max_output_bytes = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "output bytes",
                )?);
            }
            "--max-input-bytes" => {
                index += 1;
                max_input_bytes = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "input bytes",
                )?);
            }
            "--max-workspace-bytes" => {
                index += 1;
                max_workspace_bytes = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "workspace bytes",
                )?);
            }
            "--max-written-bytes-per-command" => {
                index += 1;
                max_written_bytes_per_command = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "written bytes",
                )?);
            }
            "--max-file-count" => {
                index += 1;
                max_file_count = Some(parse_nonzero_u64_sandbox_cli_value(
                    args.get(index),
                    "file count",
                )?);
            }
            "--tmp" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                temp_policy = parse_sandbox_temp_policy(value)?;
            }
            "--keep-tmp" => {
                temp_policy = sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions;
            }
            "--mount" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                mounts.push(parse_sandbox_mount(value)?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(SandboxCreateOptions {
        sandbox_id,
        toolchain_id,
        temp_policy,
        memory_mb,
        wall_time_ms,
        max_input_bytes,
        max_output_bytes,
        max_workspace_bytes,
        max_written_bytes_per_command,
        max_file_count,
        mounts,
        execution,
    })
}

pub(crate) fn run_sandbox_cli_command(command: SandboxCliCommand) -> Result<(), CliError> {
    match command {
        SandboxCliCommand::Help => {
            print_sandbox_help();
            Ok(())
        }
        SandboxCliCommand::Create(options) => sandbox_create(options),
        SandboxCliCommand::Config(options) => sandbox_config(options),
        SandboxCliCommand::Cat(options) => sandbox_cat(options),
        SandboxCliCommand::Checkpoint(options) => sandbox_checkpoint(options),
        SandboxCliCommand::Destroy(options) => sandbox_destroy(options),
        SandboxCliCommand::Doctor(options) => sandbox_doctor(options),
        SandboxCliCommand::Fetch(options) => sandbox_fetch(options),
        SandboxCliCommand::Ls(options) => sandbox_ls(options),
        SandboxCliCommand::Logs(options) => sandbox_logs(options),
        SandboxCliCommand::Policy(command) => sandbox_policy(command),
        SandboxCliCommand::ProcessPlan(options) => sandbox_process_plan(options),
        SandboxCliCommand::Reboot(options) => sandbox_reboot(options),
        SandboxCliCommand::Resume(options) => sandbox_resume(options),
        SandboxCliCommand::Run(options) => sandbox_run(options),
        SandboxCliCommand::Shell(options) => sandbox_shell(options),
        SandboxCliCommand::Status(options) => sandbox_status(options),
        SandboxCliCommand::Write(options) => sandbox_write(options),
        SandboxCliCommand::ToolchainAddCommand(options) => sandbox_toolchain_add_command(options),
        SandboxCliCommand::ToolchainAddArtifact(options) => sandbox_toolchain_add_artifact(options),
        SandboxCliCommand::ToolchainBashPlan => {
            sandbox_toolchain_bash_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainBusyBoxPlan => {
            sandbox_toolchain_busybox_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainBuildToolsPlan => {
            sandbox_toolchain_build_tools_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainHash(options) => sandbox_toolchain_hash(options),
        SandboxCliCommand::ToolchainImportBusyBox(options) => {
            sandbox_toolchain_import_busybox(options)
        }
        SandboxCliCommand::ToolchainImportPackage(options) => {
            sandbox_toolchain_import_package(options)
        }
        SandboxCliCommand::ToolchainVerifyPackage(options) => {
            sandbox_toolchain_verify_package(options)
        }
        SandboxCliCommand::ToolchainPythonPlan => {
            sandbox_toolchain_python_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainQuickJsPlan => {
            sandbox_toolchain_quickjs_plan();
            Ok(())
        }
        SandboxCliCommand::ToolchainVerifyBash(options) => sandbox_toolchain_verify_bash(options),
        SandboxCliCommand::ToolchainVerifyBusyBox(options) => {
            sandbox_toolchain_verify_busybox(options)
        }
        SandboxCliCommand::ToolchainVerifyCoding(options) => {
            sandbox_toolchain_verify_coding(options)
        }
        SandboxCliCommand::ToolchainVerifyMake(options) => sandbox_toolchain_verify_make(options),
        SandboxCliCommand::ToolchainVerifyPython(options) => {
            sandbox_toolchain_verify_python(options)
        }
        SandboxCliCommand::ToolchainVerifyTcc(options) => sandbox_toolchain_verify_tcc(options),
        SandboxCliCommand::ToolchainVerifyQuickJs(options) => {
            sandbox_toolchain_verify_quickjs(options)
        }
    }
}

pub(crate) fn sandbox_create(options: SandboxCreateOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_create_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_create_with_daemon(options),
    }
}

pub(crate) fn sandbox_status(options: SandboxStatusOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_status_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_status_with_daemon(options),
    }
}

pub(crate) fn sandbox_config(options: SandboxConfigOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_config_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_config_with_daemon(options),
    }
}

pub(crate) fn sandbox_run(options: SandboxRunOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let stdin = if options.stdin {
        let mut input = Vec::new();
        io::stdin()
            .read_to_end(&mut input)
            .map_err(|source| CliError::Parse {
                detail: format!("failed to read sandbox command stdin: {source}"),
            })?;
        Some(input)
    } else {
        None
    };
    match &options.execution {
        SandboxRunExecution::Local => sandbox_run_with_stores_and_stdin(
            options,
            stdin,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxRunExecution::Daemon => sandbox_run_with_daemon_and_stdin(options, stdin),
    }
}

pub(crate) fn sandbox_process_plan(options: SandboxProcessPlanOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxRunExecution::Local => sandbox_process_plan_with_stores(
            options,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxRunExecution::Daemon => sandbox_process_plan_with_daemon(options),
    }
}

pub(crate) fn sandbox_shell(options: SandboxShellOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let stdin = match &options.script {
        Some(script) => script.as_bytes().to_vec(),
        None => {
            let mut bytes = Vec::new();
            io::stdin()
                .lock()
                .read_to_end(&mut bytes)
                .map_err(CliError::stdio)?;
            bytes
        }
    };
    match options.execution {
        SandboxRunExecution::Local => sandbox_shell_with_stores(
            options,
            stdin,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxRunExecution::Daemon => sandbox_shell_with_daemon(options, stdin),
    }
}

pub(crate) fn sandbox_logs(options: SandboxLogsOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_logs_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_logs_with_daemon(options),
    }
}

pub(crate) fn sandbox_ls(options: SandboxLsOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_ls_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_ls_with_daemon(options),
    }
}

pub(crate) fn sandbox_cat(options: SandboxCatOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_cat_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_cat_with_daemon(options),
    }
}

pub(crate) fn sandbox_write(options: SandboxWriteOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let bytes = sandbox_write_input_bytes(&options.input)?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            let mut event_store = sinomo_runtime::events::JsonlEventStore::new(
                data_dir.root().join("events/event-log.jsonl"),
            );
            sandbox_write_with_store_and_event_store(
                options,
                bytes,
                &sandbox_store_from_data_dir(&data_dir),
                &mut event_store,
            )
        }
        SandboxMetadataExecution::Daemon => sandbox_write_with_daemon(options, bytes),
    }
}

fn sandbox_write_input_bytes(input: &SandboxWriteInput) -> Result<Vec<u8>, CliError> {
    match input {
        SandboxWriteInput::Literal(bytes) => Ok(bytes.clone()),
        SandboxWriteInput::HostFile(path) => std::fs::read(path).map_err(|source| CliError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
        SandboxWriteInput::Stdin => {
            let mut bytes = Vec::new();
            io::stdin()
                .lock()
                .read_to_end(&mut bytes)
                .map_err(CliError::stdio)?;
            Ok(bytes)
        }
    }
}

pub(crate) fn sandbox_fetch(options: SandboxFetchOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    if options.execution == SandboxMetadataExecution::Daemon {
        return sandbox_fetch_with_daemon(options);
    }
    let (config, _source) = load_sandbox_policy_config_for_node(&data_dir)?;
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox fetch requires the local daemon for mediated network access"
                .to_string(),
        });
    };
    let transport = DaemonMediatedSandboxHttpTransport {
        address: status.address,
    };
    let mut event_store = sinomo_runtime::events::JsonlEventStore::new(
        data_dir.root().join("events/event-log.jsonl"),
    );
    let _ = sandbox_fetch_with_store_transport_and_event_store(
        options,
        &sandbox_store_from_data_dir(&data_dir),
        &config.policy,
        &transport,
        &mut event_store,
    )?;
    Ok(())
}

pub(crate) fn sandbox_fetch_with_daemon(options: SandboxFetchOptions) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon fetch requires the local daemon".to_string(),
        });
    };
    let mut request = sinomo_runtime::sandbox::SandboxHttpRequest::get(options.url);
    request.timeout_ms = options.timeout_ms;
    request.max_response_bytes = options.max_response_bytes;
    request.headers = options.headers;
    let request = sinomo_runtime::sandbox::SandboxHttpFetchToFileRequest {
        sandbox_id: options.sandbox_id,
        request,
        output_path: options.output_path,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_HTTP_FETCH_TO_FILE_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let timeout =
        std::time::Duration::from_millis(request.request.timeout_ms.saturating_add(1_000));
    let response = sinomo_runtime::daemon::send_daemon_rpc(&status.address, &rpc, timeout)?;
    let result: sinomo_runtime::sandbox::SandboxHttpFetchFileResult =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_http_fetch_file_result(&result);
    Ok(())
}

pub(crate) fn sandbox_checkpoint(options: SandboxCheckpointOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_checkpoint_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_checkpoint_with_daemon(options),
    }
}

pub(crate) fn sandbox_resume(options: SandboxResumeOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_resume_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_resume_with_daemon(options),
    }
}

pub(crate) fn sandbox_reboot(options: SandboxRebootOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_reboot_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_reboot_with_daemon(options),
    }
}

pub(crate) fn sandbox_destroy(options: SandboxDestroyOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_destroy_with_store(options, &sandbox_store_from_data_dir(&data_dir))
        }
        SandboxMetadataExecution::Daemon => sandbox_destroy_with_daemon(options),
    }
}

pub(crate) fn sandbox_doctor(options: SandboxDoctorOptions) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let findings = sandbox_doctor_findings_with_stores_and_events(
        options,
        &sandbox_store_from_data_dir(&data_dir),
        &sandbox_toolchain_store_from_data_dir(&data_dir),
        &sinomo_runtime::sandbox::SandboxPolicy::default(),
        &sandbox_doctor_audit_events(&data_dir)?,
    )?;
    print_sandbox_doctor_findings(&findings);
    Ok(())
}

#[cfg(test)]
pub(crate) fn sandbox_doctor_findings_with_stores(
    options: SandboxDoctorOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
    policy: &sinomo_runtime::sandbox::SandboxPolicy,
) -> Result<Vec<SandboxDoctorFinding>, CliError> {
    sandbox_doctor_findings_with_stores_and_events(options, store, toolchain_store, policy, &[])
}

pub(crate) fn sandbox_doctor_findings_with_stores_and_events(
    options: SandboxDoctorOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
    policy: &sinomo_runtime::sandbox::SandboxPolicy,
    resource_events: &[sinomo_runtime::events::StoredEvent],
) -> Result<Vec<SandboxDoctorFinding>, CliError> {
    let mut findings = Vec::new();
    findings.push(SandboxDoctorFinding::info(
        "local-staging-backend",
        "local CLI sandbox commands use local staging by default; pass --daemon on supported commands for SIFS-backed daemon execution",
    ));
    append_sandbox_doctor_runtime_capability_findings(&mut findings);
    append_sandbox_doctor_policy_posture_findings(&mut findings, policy);

    let busybox_id = sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named(
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_LEGACY_ID,
    )
    .expect("static toolchain id");
    match toolchain_store.load_manifest(&busybox_id) {
        Ok((manifest, _layout)) => {
            for command in ["cat", "grep", "find", "printf"] {
                if !manifest.commands.contains_key(command) {
                    findings.push(SandboxDoctorFinding::warning(
                        "busybox-command-missing",
                        format!("default BusyBox toolchain is missing `{command}`"),
                    ));
                }
            }
        }
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. }) => {
            findings.push(SandboxDoctorFinding::warning(
                "busybox-toolchain-missing",
                "default BusyBox toolchain is not installed in the local toolchain cache; import it with `sinomo sandbox toolchain import-busybox`",
            ));
        }
        Err(error) => return Err(error.into()),
    }

    let Some(sandbox_id) = options.sandbox_id else {
        append_sandbox_doctor_audit_findings(&mut findings, None, resource_events);
        return Ok(findings);
    };
    let (manifest, layout) = store.load(&sandbox_id)?;
    for dir in layout.required_dirs() {
        if !dir.is_dir() {
            findings.push(SandboxDoctorFinding::error(
                "sandbox-directory-missing",
                format!("required sandbox directory is missing: {}", dir.display()),
            ));
        }
    }

    match toolchain_store.load_manifest(&manifest.toolchain_id) {
        Ok((toolchain, _layout)) => {
            append_sandbox_doctor_coding_toolchain_findings(&mut findings, &manifest, &toolchain);
            append_sandbox_doctor_rich_toolchain_findings(&mut findings, &manifest, &toolchain);
            append_sandbox_doctor_mvp_toolchain_findings(&mut findings, &manifest, &toolchain);
            match toolchain.command("sh") {
                Ok(command) => {
                    let command_abi = command.effective_target_abi(&toolchain);
                    if sinomo_runtime::sandbox::sandbox_command_requires_child_process_substrate(
                        &command.name,
                    ) && !sinomo_runtime::sandbox::sandbox_runtime_supports_child_processes_for_abi(
                        &command_abi,
                    )
                    {
                        findings.push(SandboxDoctorFinding::error(
                            "shell-process-substrate-missing",
                            format!(
                                "sandbox {} is pinned to {:?} toolchain {}, and `sh` requires a real child-process execution substrate but the current runtime does not support `{}` for that ABI",
                                manifest.sandbox_id,
                                command_abi,
                                manifest.toolchain_id,
                                sinomo_runtime::sandbox::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY
                            ),
                        ));
                    }
                    if matches!(command_abi, sinomo_runtime::sandbox::SandboxWasiAbi::Wasix)
                        && command.required_capabilities.iter().any(|capability| {
                            capability
                            == sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                        })
                    {
                        findings.push(SandboxDoctorFinding::warning(
                            "wasix-unpinned-child-exec-acknowledged",
                            format!(
                                "sandbox {} is pinned to WASIX `sh` from toolchain {}, and that command relies on `{}` so shell commands can execute sandbox-local WASM modules and scripts from writable mounts such as /work",
                                manifest.sandbox_id,
                                manifest.toolchain_id,
                                sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                            ),
                        ));
                    }
                }
                Err(_) => {
                    findings.push(SandboxDoctorFinding::error(
                        "shell-command-missing",
                        format!(
                            "sandbox {} is pinned to toolchain {}, but that toolchain does not provide real `sh`; the sandbox must not claim shell support until a genuine shell/process substrate is available",
                            manifest.sandbox_id, manifest.toolchain_id
                        ),
                    ));
                }
            }
        }
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. }) => {
            findings.push(SandboxDoctorFinding::error(
                "sandbox-toolchain-missing",
                format!(
                    "sandbox {} is pinned to missing toolchain {}",
                    manifest.sandbox_id, manifest.toolchain_id
                ),
            ));
        }
        Err(error) => return Err(error.into()),
    }

    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    if let Err(denial) = policy.validate_manifest(&manifest, &execution_node_id) {
        findings.push(SandboxDoctorFinding::error(
            "policy-denied",
            denial.to_string(),
        ));
    }
    if !manifest.mounts.is_empty() {
        findings.push(SandboxDoctorFinding::warning(
            "local-cli-mounts-not-staged",
            "local CLI runs validate declared SIFS mounts but do not stage them into WASI; use the SIFS staged runner path for real mounted execution",
        ));
    }
    if let Some(placement) = &manifest.last_placement {
        findings.push(SandboxDoctorFinding::info(
            "last-placement",
            format!(
                "last SIFS placement selected node {} for {:?}",
                placement.node_id, placement.reason
            ),
        ));
    }
    append_sandbox_doctor_audit_findings(&mut findings, Some(&sandbox_id), resource_events);
    Ok(findings)
}

fn append_sandbox_doctor_runtime_capability_findings(findings: &mut Vec<SandboxDoctorFinding>) {
    if sinomo_runtime::sandbox::sandbox_runtime_disables_ambient_network() {
        findings.push(SandboxDoctorFinding::info(
            "ambient-wasi-network-disabled",
            "WASI command execution has no ambient TCP, UDP, or name-lookup access; use mediated sandbox fetch for network egress",
        ));
    }
    for abi in [
        sinomo_runtime::sandbox::SandboxWasiAbi::Wasix,
        sinomo_runtime::sandbox::SandboxWasiAbi::Preview1,
    ] {
        for capability in
            sinomo_runtime::sandbox::sandbox_runtime_process_capabilities_for_abi(&abi)
        {
            if capability.supported {
                findings.push(SandboxDoctorFinding::info(
                    "process-substrate-available",
                    format!(
                        "{abi:?} sandbox runtime capability `{}` is available: {}",
                        capability.capability, capability.detail
                    ),
                ));
                continue;
            }
            findings.push(SandboxDoctorFinding::warning(
                "process-substrate-missing",
                format!(
                    "{abi:?} sandbox runtime capability `{}` is not available yet: {}",
                    capability.capability, capability.detail
                ),
            ));
        }
        for capability in sinomo_runtime::sandbox::sandbox_runtime_budget_capabilities_for_abi(&abi)
        {
            if capability.supported {
                findings.push(SandboxDoctorFinding::info(
                    "runtime-budget-supported",
                    format!(
                        "{abi:?} sandbox runtime budget capability `{}` is available: {}",
                        capability.capability, capability.detail
                    ),
                ));
                continue;
            }
            findings.push(SandboxDoctorFinding::warning(
                "runtime-budget-missing",
                format!(
                    "{abi:?} sandbox runtime budget capability `{}` is not available yet: {}",
                    capability.capability, capability.detail
                ),
            ));
        }
    }
    for capability in sinomo_runtime::sandbox::sandbox_runtime_mvp_virtual_device_capabilities() {
        if capability.supported {
            continue;
        }
        findings.push(SandboxDoctorFinding::warning(
            "mvp-wasi-device-missing",
            format!(
                "sandbox filesystem exposes `{}`, but WASI command execution cannot open it through the runtime device layer yet: {}",
                capability.device, capability.detail
            ),
        ));
    }
}

fn append_sandbox_doctor_policy_posture_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    policy: &sinomo_runtime::sandbox::SandboxPolicy,
) {
    if policy
        .allowed_command_capabilities
        .contains(sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
    {
        findings.push(SandboxDoctorFinding::warning(
            "policy-allows-wasix-unpinned-child-exec",
            format!(
                "sandbox policy allows `{}`; WASIX shell, Python, and build-tool commands that use this capability can execute sandbox-local WASM modules and scripts from writable mounts such as /work",
                sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
            ),
        ));
    }
}

fn append_sandbox_doctor_mvp_toolchain_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    manifest: &sinomo_runtime::sandbox::SandboxInstanceManifest,
    toolchain: &sinomo_runtime::sandbox::SandboxToolchainManifest,
) {
    let mut missing = Vec::new();
    for command in sinomo_runtime::sandbox::SANDBOX_MVP_COMMANDS {
        if toolchain.command(command).is_err() {
            missing.push(*command);
            let reason = sinomo_runtime::sandbox::sandbox_mvp_command_gap(command)
                .map(|gap| format!(": {}", gap.reason))
                .unwrap_or_default();
            findings.push(SandboxDoctorFinding::warning(
                "mvp-command-missing",
                format!(
                    "sandbox {} is pinned to toolchain {}, but that toolchain is missing `{command}` from the planned minimal POSIX command environment{reason}",
                    manifest.sandbox_id, manifest.toolchain_id
                ),
            ));
        }
    }
    if !missing.is_empty() {
        findings.push(SandboxDoctorFinding::error(
            "mvp-command-surface-incomplete",
            format!(
                "sandbox {} is pinned to toolchain {}, but the planned minimal POSIX command surface is incomplete; missing commands: {}",
                manifest.sandbox_id,
                manifest.toolchain_id,
                missing
                    .iter()
                    .map(|command| format!("`{command}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
}

fn append_sandbox_doctor_rich_toolchain_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    manifest: &sinomo_runtime::sandbox::SandboxInstanceManifest,
    toolchain: &sinomo_runtime::sandbox::SandboxToolchainManifest,
) {
    for target in sinomo_runtime::sandbox::SANDBOX_RICH_TOOLCHAIN_TARGETS {
        match target.status {
            sinomo_runtime::sandbox::SandboxRichToolchainStatus::Implemented => {
                if toolchain.command(target.command).is_err() {
                    let recipe = target
                        .recipe
                        .map(|recipe| format!("; recipe: {recipe}"))
                        .unwrap_or_default();
                    findings.push(SandboxDoctorFinding::warning(
                        "rich-toolchain-command-missing",
                        format!(
                            "sandbox {} is pinned to toolchain {}, but maintained richer toolchain target `{}` is missing{}",
                            manifest.sandbox_id, manifest.toolchain_id, target.command, recipe
                        ),
                    ));
                }
            }
            sinomo_runtime::sandbox::SandboxRichToolchainStatus::Planned => {
                findings.push(SandboxDoctorFinding::info(
                    "rich-toolchain-target-planned",
                    format!(
                        "planned richer toolchain target `{}`: {}",
                        target.command, target.note
                    ),
                ));
            }
            sinomo_runtime::sandbox::SandboxRichToolchainStatus::Required => {
                if toolchain.command(target.command).is_err() {
                    let recipe = target
                        .recipe
                        .map(|recipe| format!("; target doc: {recipe}"))
                        .unwrap_or_default();
                    findings.push(SandboxDoctorFinding::error(
                        "required-toolchain-command-missing",
                        format!(
                            "sandbox {} is pinned to toolchain {}, but required coding sandbox command `{}` is missing{}; {}",
                            manifest.sandbox_id,
                            manifest.toolchain_id,
                            target.command,
                            recipe,
                            target.note
                        ),
                    ));
                }
            }
        }
    }
}

fn append_sandbox_doctor_coding_toolchain_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    manifest: &sinomo_runtime::sandbox::SandboxInstanceManifest,
    toolchain: &sinomo_runtime::sandbox::SandboxToolchainManifest,
) {
    let commands = sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_COMMANDS
        .iter()
        .copied()
        .chain(std::iter::once(
            sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND,
        ));
    for command in commands {
        if toolchain.command(command).is_err() {
            findings.push(SandboxDoctorFinding::warning(
                "coding-command-missing",
                format!(
                    "sandbox {} is pinned to toolchain {}, but that toolchain is missing `{command}` required by `sinomo sandbox toolchain verify-coding`",
                    manifest.sandbox_id, manifest.toolchain_id
                ),
            ));
        }
    }
}

fn sandbox_doctor_audit_events(
    data_dir: &SinomoDataDir,
) -> Result<Vec<sinomo_runtime::events::StoredEvent>, CliError> {
    let event_path = data_dir.root().join("events").join("event-log.jsonl");
    if !event_path.exists() {
        return Ok(Vec::new());
    }
    let event_store = sinomo_runtime::events::JsonlEventStore::new(event_path);
    let mut query = sinomo_runtime::events::ReplayQuery::by_kind_prefix("sandbox.").limit(32);
    query.direction = sinomo_runtime::events::ReplayDirection::Reverse;
    event_store.replay(query).map_err(CliError::from)
}

fn append_sandbox_doctor_audit_findings(
    findings: &mut Vec<SandboxDoctorFinding>,
    sandbox_id: Option<&sinomo_runtime::sandbox::SandboxId>,
    events: &[sinomo_runtime::events::StoredEvent],
) {
    for event in events {
        if !sandbox_doctor_event_matches_sandbox(event, sandbox_id) {
            continue;
        }
        match event.event.kind.as_str() {
            "sandbox.policy.denied" => append_sandbox_doctor_policy_finding(findings, event),
            "sandbox.resource.exhausted" => append_sandbox_doctor_resource_finding(findings, event),
            _ => {}
        }
    }
}

fn sandbox_doctor_event_matches_sandbox(
    event: &sinomo_runtime::events::StoredEvent,
    sandbox_id: Option<&sinomo_runtime::sandbox::SandboxId>,
) -> bool {
    let Some(sandbox_id) = sandbox_id else {
        return true;
    };
    event.event.payload["sandbox_id"].as_str() == Some(sandbox_id.to_string().as_str())
}

fn append_sandbox_doctor_policy_finding(
    findings: &mut Vec<SandboxDoctorFinding>,
    event: &sinomo_runtime::events::StoredEvent,
) {
    let event_sandbox_id = event.event.payload["sandbox_id"].as_str();
    let scope = event_sandbox_id
        .map(|sandbox_id| format!("sandbox {sandbox_id}"))
        .unwrap_or_else(|| "cluster sandbox service".to_string());
    let phase = event.event.payload["phase"].as_str().unwrap_or("unknown");
    let code = event.event.payload["code"].as_str().unwrap_or("unknown");
    let detail = event.event.payload["detail"].as_str().unwrap_or("unknown");
    findings.push(SandboxDoctorFinding::warning(
        "recent-policy-denied",
        format!("recent sandbox policy denial in {scope} during {phase}: {code}: {detail}"),
    ));
}

fn append_sandbox_doctor_resource_finding(
    findings: &mut Vec<SandboxDoctorFinding>,
    event: &sinomo_runtime::events::StoredEvent,
) {
    let event_sandbox_id = event.event.payload["sandbox_id"]
        .as_str()
        .unwrap_or("unknown");
    let phase = event.event.payload["phase"].as_str().unwrap_or("unknown");
    let kind = event.event.payload["kind"].as_str().unwrap_or("unknown");
    let actual = event.event.payload["actual"]
        .as_u64()
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let limit = event.event.payload["limit"]
        .as_u64()
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    findings.push(SandboxDoctorFinding::warning(
        "recent-resource-exhausted",
        format!(
            "recent sandbox resource exhaustion in sandbox {event_sandbox_id} during {phase}: {kind} actual {actual}, limit {limit}"
        ),
    ));
}

impl SandboxDoctorFinding {
    fn info(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: SandboxDoctorSeverity::Info,
            code: code.into(),
            detail: detail.into(),
        }
    }

    fn warning(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: SandboxDoctorSeverity::Warning,
            code: code.into(),
            detail: detail.into(),
        }
    }

    fn error(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: SandboxDoctorSeverity::Error,
            code: code.into(),
            detail: detail.into(),
        }
    }
}

fn print_sandbox_doctor_findings(findings: &[SandboxDoctorFinding]) {
    println!(
        "sandbox doctor: {}",
        if findings
            .iter()
            .all(|finding| finding.severity == SandboxDoctorSeverity::Info)
        {
            "ok"
        } else {
            "findings"
        }
    );
    if findings.is_empty() {
        println!("no findings");
        return;
    }
    for finding in findings {
        println!(
            "- [{}] {}: {}",
            sandbox_doctor_severity_label(&finding.severity),
            finding.code,
            finding.detail
        );
    }
}

fn sandbox_doctor_severity_label(severity: &SandboxDoctorSeverity) -> &'static str {
    match severity {
        SandboxDoctorSeverity::Info => "info",
        SandboxDoctorSeverity::Warning => "warning",
        SandboxDoctorSeverity::Error => "error",
    }
}

pub(crate) fn sandbox_policy(command: SandboxPolicyCliCommand) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match command {
        SandboxPolicyCliCommand::Show => {
            let (config, source) = load_sandbox_policy_config_for_node(&data_dir)?;
            print_sandbox_policy_config(&config, &source);
            Ok(())
        }
        SandboxPolicyCliCommand::Set(options) => {
            let mut config = load_sandbox_policy_config_for_node(&data_dir)?.0;
            apply_sandbox_policy_set_options(&mut config.policy, options);
            let path = write_sandbox_policy_config_for_node(&data_dir, &config)?;
            println!("sandbox policy written: {}", path.display());
            print_sandbox_policy_config(&config, "cluster kv");
            Ok(())
        }
    }
}

pub(crate) fn sandbox_policy_uses_cluster(data_dir: &SinomoDataDir) -> bool {
    network::read_cluster_state(&ClusterPaths::new(data_dir)).is_ok()
}

pub(crate) fn load_sandbox_policy_config_for_node(
    data_dir: &SinomoDataDir,
) -> Result<(sinomo_runtime::sandbox::SandboxClusterPolicyConfig, String), CliError> {
    if sandbox_policy_uses_cluster(data_dir) {
        let paths = ClusterPaths::new(data_dir);
        let snapshot = sandbox_policy_cluster_snapshot(&paths)?;
        let config =
            sinomo_runtime::sandbox::sandbox_policy_config_from_cluster_kv_snapshot(&snapshot)?
                .unwrap_or_default();
        return Ok((config, "cluster kv".to_string()));
    }
    Ok((
        sinomo_runtime::sandbox::SandboxClusterPolicyConfig::default(),
        "built-in default".to_string(),
    ))
}

pub(crate) fn write_sandbox_policy_config_for_node(
    data_dir: &SinomoDataDir,
    config: &sinomo_runtime::sandbox::SandboxClusterPolicyConfig,
) -> Result<PathBuf, CliError> {
    if !sandbox_policy_uses_cluster(data_dir) {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox policy is cluster scoped and requires an initialized local cluster"
                .to_string(),
        });
    }
    let paths = ClusterPaths::new(data_dir);
    paths.ensure()?;
    let txn = sinomo_runtime::sandbox::sandbox_policy_config_cluster_kv_txn(config)?;
    commit_sandbox_policy_cluster_txn(data_dir, txn)?;
    Ok(paths.kv_snapshot)
}

fn sandbox_policy_cluster_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    let _ = paths;
    if let Some(status) = default_main_daemon_status()?
        && let Ok(snapshot) =
            daemon_fetch_cluster_kv_snapshot(&status.address, DAEMON_PROBE_TIMEOUT)
    {
        return Ok(snapshot);
    }
    #[cfg(test)]
    {
        return network::read_cluster_kv_snapshot(paths).map_err(Into::into);
    }
    #[cfg(not(test))]
    {
        Err(CliError::DaemonNotRunning {
            detail: "clustered sandbox policy requires the local daemon".to_string(),
        })
    }
}

#[cfg(test)]
fn commit_sandbox_policy_cluster_txn(data_dir: &SinomoDataDir, txn: KvTxn) -> Result<(), CliError> {
    let daemon_paths = DaemonPaths::new(data_dir);
    daemon_handle_daemon_cluster_kv_txn_request(&daemon_paths, txn)?;
    Ok(())
}

#[cfg(not(test))]
fn commit_sandbox_policy_cluster_txn(
    _data_dir: &SinomoDataDir,
    txn: KvTxn,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "clustered sandbox policy writes require the local daemon".to_string(),
        });
    };
    let request = network::ObjectRpcRequest::encode_body(
        network::CLUSTER_KV_TXN_ROUTE,
        network::CLUSTER_KV_RPC_CATEGORY,
        &txn,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &request,
        DAEMON_CLUSTER_KV_TXN_TIMEOUT,
    )?;
    let _: KvTxnResponse = response.decode_body().map_err(CliError::from)?;
    Ok(())
}

pub(crate) fn apply_sandbox_policy_set_options(
    policy: &mut sinomo_runtime::sandbox::SandboxPolicy,
    options: SandboxPolicySetOptions,
) {
    if let Some(value) = options.allow_unsigned_toolchains {
        policy.allow_unsigned_toolchains = value;
    }
    if let Some(value) = options.allow_remote_local_owned_mounts {
        policy.allow_remote_local_owned_mounts = value;
    }
    policy
        .allowed_mount_prefixes
        .extend(options.allowed_mount_prefixes);
    if let Some(value) = options.max_memory_mb {
        policy.max_limits.memory_mb = value;
    }
    if let Some(value) = options.max_wall_time_ms {
        policy.max_limits.wall_time_ms = value;
    }
    if let Some(value) = options.max_output_bytes {
        policy.max_limits.max_output_bytes = value;
    }
    if let Some(value) = options.max_input_bytes {
        policy.max_limits.max_input_bytes = value;
    }
    if let Some(value) = options.max_workspace_bytes {
        policy.max_limits.max_workspace_bytes = value;
    }
    if let Some(value) = options.max_written_bytes_per_command {
        policy.max_limits.max_written_bytes_per_command = value;
    }
    if let Some(value) = options.max_file_count {
        policy.max_limits.max_file_count = value;
    }
    if let Some(value) = options.max_http_timeout_ms {
        policy.max_http_timeout_ms = value;
    }
    if let Some(value) = options.max_concurrent_commands_per_sandbox {
        policy.max_concurrent_commands_per_sandbox = value;
    }
    if let Some(value) = options.max_concurrent_sandboxes_per_node {
        policy.max_concurrent_sandboxes_per_node = value;
    }
    policy
        .allowed_command_capabilities
        .extend(options.allowed_capabilities);
    policy
        .allowed_toolchain_ids
        .extend(options.allowed_toolchains);
    policy
        .allowed_toolchain_provenance_prefixes
        .extend(options.allowed_toolchain_provenance_prefixes);
    policy
        .allowed_package_cache_prefixes
        .extend(options.allowed_package_cache_prefixes);
    policy.allowed_http_hosts.extend(options.allowed_http_hosts);
}

fn print_sandbox_policy_config(
    config: &sinomo_runtime::sandbox::SandboxClusterPolicyConfig,
    source: &str,
) {
    let policy = &config.policy;
    println!("source: {source}");
    println!(
        "key: {}",
        sinomo_runtime::sandbox::SANDBOX_POLICY_CONFIG_KV_KEY
    );
    println!(
        "allow unsigned toolchains: {}",
        policy.allow_unsigned_toolchains
    );
    println!(
        "allow remote local-owned mounts: {}",
        policy.allow_remote_local_owned_mounts
    );
    println!("allowed mount prefixes:");
    for prefix in &policy.allowed_mount_prefixes {
        println!("  {prefix}");
    }
    println!("allowed command capabilities:");
    for capability in &policy.allowed_command_capabilities {
        println!("  {capability}");
    }
    println!("allowed toolchains:");
    for toolchain_id in &policy.allowed_toolchain_ids {
        println!("  {toolchain_id}");
    }
    println!("allowed toolchain provenance prefixes:");
    for prefix in &policy.allowed_toolchain_provenance_prefixes {
        println!("  {prefix}");
    }
    println!("allowed package cache prefixes:");
    for prefix in &policy.allowed_package_cache_prefixes {
        println!("  {prefix}");
    }
    println!("allowed http hosts:");
    for host in &policy.allowed_http_hosts {
        println!("  {host}");
    }
    println!("max memory mb: {}", policy.max_limits.memory_mb);
    println!(
        "max wall time ms: {}",
        format_sandbox_wall_time_ms(policy.max_limits.wall_time_ms)
    );
    println!("max input bytes: {}", policy.max_limits.max_input_bytes);
    println!("max output bytes: {}", policy.max_limits.max_output_bytes);
    println!(
        "max workspace bytes: {}",
        policy.max_limits.max_workspace_bytes
    );
    println!(
        "max written bytes per command: {}",
        policy.max_limits.max_written_bytes_per_command
    );
    println!("max file count: {}", policy.max_limits.max_file_count);
    println!("max http timeout ms: {}", policy.max_http_timeout_ms);
    println!(
        "max concurrent commands per sandbox: {}",
        policy.max_concurrent_commands_per_sandbox
    );
    println!(
        "max concurrent sandboxes per node: {}",
        policy.max_concurrent_sandboxes_per_node
    );
}

fn format_sandbox_wall_time_ms(value: u64) -> String {
    if value == sinomo_runtime::sandbox::SANDBOX_UNLIMITED_WALL_TIME_MS {
        "unlimited".to_string()
    } else {
        value.to_string()
    }
}

pub(crate) fn sandbox_toolchain_add_command(
    options: SandboxToolchainAddCommandOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_add_command_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_add_command_with_daemon(options),
    }
}

pub(crate) fn sandbox_toolchain_busybox_plan() {
    print!("{}", sandbox_toolchain_busybox_plan_text());
}

pub(crate) fn sandbox_toolchain_build_tools_plan() {
    print!("{}", sandbox_toolchain_build_tools_plan_text());
}

pub(crate) fn sandbox_toolchain_bash_plan() {
    print!("{}", sandbox_toolchain_bash_plan_text());
}

pub(crate) fn sandbox_toolchain_bash_plan_text() -> String {
    let mut output = String::new();
    output.push_str("commands: ");
    output.push_str(sinomo_runtime::sandbox::BASH_TOOLCHAIN_COMMAND);
    output.push_str(", ");
    output.push_str(sinomo_runtime::sandbox::SH_TOOLCHAIN_COMMAND);
    output.push('\n');
    output.push_str("target ABI: wasix\n");
    output.push_str("recipe:\n");
    output.push_str(&format!(
        "  {}\n",
        sinomo_runtime::sandbox::BASH_TOOLCHAIN_RECIPE
    ));
    output.push_str("status:\n");
    output.push_str(
        "  required for coding-task readiness; the maintained Bash WASIX package passes local and daemon-local SIFS verification when cluster policy allows child-process-exec plus wasix.unpinned-child-exec for sandbox-local script and WASM execution\n",
    );
    output.push_str("import:\n");
    output.push_str(&format!(
        "  sinomo sandbox toolchain import-package [--daemon] --into {} <bash-package.json>\n",
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_LEGACY_ID
    ));
    output.push_str("verification:\n");
    output.push_str(
        "  sinomo sandbox toolchain verify-bash <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str("fidelity cases:\n");
    for case in sinomo_runtime::sandbox::BASH_TOOLCHAIN_FIDELITY_CASES {
        output.push_str(&format!("  {}: {}\n", case.name, case.command));
    }
    output
}

pub(crate) fn sandbox_toolchain_busybox_plan_text() -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "toolchain: {}",
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_LEGACY_ID
    ));
    output.push('\n');
    output.push_str("core applets:\n");
    for command in sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_COMMANDS {
        output.push_str(&format!("  {command}\n"));
    }
    output.push_str("fidelity cases:\n");
    for case in sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_FIDELITY_CASES {
        output.push_str(&format!("  {}: {}\n", case.name, case.command));
    }
    output.push_str("known MVP gaps:\n");
    for gap in sinomo_runtime::sandbox::SANDBOX_MVP_COMMAND_GAPS {
        output.push_str(&format!("  {}: {}\n", gap.command, gap.reason));
    }
    output
}

pub(crate) fn sandbox_toolchain_build_tools_plan_text() -> String {
    let mut output = String::new();
    output.push_str("target doc:\n");
    output.push_str(&format!(
        "  {}\n",
        sinomo_runtime::sandbox::BUILD_TOOLS_TOOLCHAIN_PLAN
    ));
    output.push_str("tracked build tool commands:\n");
    for target in sinomo_runtime::sandbox::SANDBOX_RICH_TOOLCHAIN_TARGETS {
        if matches!(
            target.command,
            sinomo_runtime::sandbox::TCC_TOOLCHAIN_COMMAND
                | sinomo_runtime::sandbox::MAKE_TOOLCHAIN_COMMAND
        ) {
            let status = match target.status {
                sinomo_runtime::sandbox::SandboxRichToolchainStatus::Implemented => "implemented",
                sinomo_runtime::sandbox::SandboxRichToolchainStatus::Required => "required",
                sinomo_runtime::sandbox::SandboxRichToolchainStatus::Planned => "planned",
            };
            output.push_str(&format!(
                "  {} ({status}): {}\n",
                target.command, target.note
            ));
            if let Some(recipe) = target.recipe {
                output.push_str(&format!("    recipe: {recipe}\n"));
            }
        }
    }
    output.push_str("availability rule:\n");
    output.push_str(
        "  build tools stay absent until real WASI packages and fidelity suites exist; do not expose host fallbacks, fake compilers, fake make, or parser substitutes under these command names\n",
    );
    output.push_str("required gates:\n");
    output.push_str("  real upstream or maintained source build to WASI/WASIX\n");
    output.push_str("  generic import-package manifest with hashes and provenance\n");
    output.push_str("  local and daemon/SIFS verifier coverage for compile/build workflows\n");
    output.push_str(
        "  sinomo sandbox toolchain verify-make <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str(
        "  sinomo sandbox toolchain verify-tcc <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str("  sandbox doctor reports missing implemented build tools as warnings\n");
    output
}

pub(crate) fn sandbox_toolchain_quickjs_plan() {
    println!(
        "toolchain: {}",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_ID
    );
    println!(
        "command: {}",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND
    );
    println!(
        "version: {}",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_VERSION
    );
    println!("build recipe:");
    println!("  {}", sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_RECIPE);
    println!("package output:");
    println!(
        "  {}",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_PACKAGE_MANIFEST
    );
    println!("import:");
    println!(
        "  sinomo sandbox toolchain import-package [--daemon] {}",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_PACKAGE_MANIFEST
    );
    println!("coding toolchain import:");
    println!(
        "  sinomo sandbox toolchain import-package [--daemon] --into {} {}",
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_LEGACY_ID,
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_PACKAGE_MANIFEST
    );
    println!("coding toolchain verification:");
    println!("  sinomo sandbox toolchain verify-coding <sandbox-id> [--daemon] [--allow-unsigned]");
    println!(
        "    --allow-unsigned is local-only; daemon verification uses the cluster sandbox policy"
    );
    println!("fidelity cases:");
    for case in sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_FIDELITY_CASES {
        println!(
            "  {}: {}",
            case.name,
            sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND
        );
    }
}

pub(crate) fn sandbox_toolchain_python_plan() {
    print!("{}", sandbox_toolchain_python_plan_text());
}

pub(crate) fn sandbox_toolchain_python_plan_text() -> String {
    let mut output = String::new();
    output.push_str("commands:\n");
    output.push_str(&format!(
        "  {}\n",
        sinomo_runtime::sandbox::PYTHON_TOOLCHAIN_COMMAND
    ));
    output.push_str(&format!(
        "  {}\n",
        sinomo_runtime::sandbox::PIP_TOOLCHAIN_COMMAND
    ));
    output.push_str("target ABI: wasix\n");
    output.push_str("recipe:\n");
    output.push_str(&format!(
        "  {}\n",
        sinomo_runtime::sandbox::PYTHON_TOOLCHAIN_RECIPE
    ));
    output.push_str("status:\n");
    output.push_str(
        "  required for coding-task readiness; the maintained CPython WASIX package is the active POSIX/process gate for Python, pip, native-extension loading, numpy array execution, and matplotlib Agg rendering inside the sandbox\n",
    );
    output.push_str("import:\n");
    output.push_str(&format!(
        "  sinomo sandbox toolchain import-package [--daemon] --into {} <python-package.json>\n",
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_LEGACY_ID
    ));
    output.push_str("verification:\n");
    output.push_str(
        "  sinomo sandbox toolchain verify-python <sandbox-id> [--daemon] [--allow-unsigned]\n",
    );
    output.push_str(&format!(
        "  {}\n",
        sinomo_runtime::sandbox::PYTHON_WASIX_NATIVE_VERIFIER
    ));
    output.push_str("fidelity cases:\n");
    for case in sinomo_runtime::sandbox::PYTHON_TOOLCHAIN_FIDELITY_CASES
        .iter()
        .chain(sinomo_runtime::sandbox::PIP_TOOLCHAIN_FIDELITY_CASES.iter())
    {
        output.push_str(&format!("  {}: {}\n", case.name, case.command));
    }
    output
}

pub(crate) fn sandbox_toolchain_verify_busybox(
    options: SandboxToolchainVerifyBusyBoxOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_busybox_with_stores(
            options,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_busybox_with_daemon(options),
    }
}

pub(crate) fn sandbox_toolchain_verify_busybox_with_stores(
    options: SandboxToolchainVerifyBusyBoxOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let runner = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?;
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = options.allow_unsigned;
    match runner.verify_busybox_toolchain_fidelity(
        store,
        toolchain_store,
        &options.sandbox_id,
        &node_id,
        &policy,
        sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_FIDELITY_CASES,
    ) {
        Ok(results) => {
            print_busybox_fidelity_results(&results);
            Ok(())
        }
        Err(sinomo_runtime::sandbox::SandboxToolFidelityError::Failed { results, .. }) => {
            print_busybox_fidelity_results(&results);
            Err(CliError::Parse {
                detail: "BusyBox fidelity verification failed".to_string(),
            })
        }
        Err(sinomo_runtime::sandbox::SandboxToolFidelityError::Command(error)) => Err(error.into()),
    }
}

pub(crate) fn sandbox_toolchain_verify_busybox_with_daemon(
    options: SandboxToolchainVerifyBusyBoxOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon BusyBox verification requires the local daemon".to_string(),
        });
    };
    let mut results = Vec::new();
    for case in sinomo_runtime::sandbox::BUSYBOX_TOOLCHAIN_FIDELITY_CASES {
        let outcome = daemon_run_toolchain_command(
            &status.address,
            options.sandbox_id.clone(),
            case.command.to_string(),
            case.argv_vec(),
            case.stdin_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
        )?;
        let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
        let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
        let mut mismatch = None;
        if outcome.response.exit_status != case.expected_exit_status {
            mismatch = Some(format!(
                "expected exit {}, got {}",
                case.expected_exit_status, outcome.response.exit_status
            ));
        } else if stdout != case.expected_stdout {
            mismatch = Some(format!(
                "expected stdout {:?}, got {:?}",
                case.expected_stdout, stdout
            ));
        }
        results.push(sinomo_runtime::sandbox::SandboxToolFidelityResult {
            case_name: case.name.to_string(),
            command: case.command.to_string(),
            passed: mismatch.is_none(),
            exit_status: outcome.response.exit_status,
            stdout,
            stderr,
            mismatch,
        });
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "BusyBox fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

fn print_busybox_fidelity_results(results: &[sinomo_runtime::sandbox::SandboxToolFidelityResult]) {
    for result in results {
        let status = if result.passed { "ok" } else { "failed" };
        println!("{}: {} ({})", result.case_name, result.command, status);
        if let Some(mismatch) = &result.mismatch {
            println!("  {mismatch}");
        }
    }
}

pub(crate) fn sandbox_toolchain_verify_quickjs(
    options: SandboxToolchainVerifyQuickJsOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_quickjs_with_stores(
            options,
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_quickjs_with_daemon(options),
    }
}

pub(crate) fn sandbox_toolchain_verify_coding(
    options: SandboxToolchainVerifyCodingOptions,
) -> Result<(), CliError> {
    println!("BusyBox file/search fidelity:");
    sandbox_toolchain_verify_busybox(SandboxToolchainVerifyBusyBoxOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("QuickJS runtime fidelity:");
    sandbox_toolchain_verify_quickjs(SandboxToolchainVerifyQuickJsOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("Coding workflow fidelity:");
    match options.execution {
        SandboxMetadataExecution::Local => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            sandbox_toolchain_verify_coding_workflow_with_stores(
                options.clone(),
                &sandbox_store_from_data_dir(&data_dir),
                &sandbox_toolchain_store_from_data_dir(&data_dir),
            )
        }
        SandboxMetadataExecution::Daemon => {
            sandbox_toolchain_verify_coding_workflow_with_daemon(options.clone())
        }
    }?;
    if options.full {
        sandbox_toolchain_verify_full_coding_surface(options)?;
    }
    Ok(())
}

fn sandbox_toolchain_verify_full_coding_surface(
    options: SandboxToolchainVerifyCodingOptions,
) -> Result<(), CliError> {
    println!("Bash shell fidelity:");
    sandbox_toolchain_verify_bash(SandboxToolchainVerifyBashOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("Python and pip fidelity:");
    sandbox_toolchain_verify_python(SandboxToolchainVerifyPythonOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("GNU Make fidelity:");
    sandbox_toolchain_verify_make(SandboxToolchainVerifyMakeOptions {
        sandbox_id: options.sandbox_id.clone(),
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })?;
    println!("TinyCC fidelity:");
    sandbox_toolchain_verify_tcc(SandboxToolchainVerifyTccOptions {
        sandbox_id: options.sandbox_id,
        allow_unsigned: options.allow_unsigned,
        execution: options.execution,
    })
}

fn sandbox_toolchain_verify_coding_workflow_with_stores(
    options: SandboxToolchainVerifyCodingOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let runner = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?;
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = options.allow_unsigned;
    let mut results = Vec::new();
    for case in CODING_WORKFLOW_BUSYBOX_CASES {
        let outcome = runner.run_toolchain_command_with_stdin_and_policy(
            store,
            toolchain_store,
            &options.sandbox_id,
            case.command,
            case.argv_vec(),
            case.stdin_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
            &node_id,
            &policy,
        )?;
        results.push(sandbox_tool_fidelity_result_from_busybox_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    let script = coding_workflow_quickjs_script();
    let qjs = runner.run_toolchain_command_with_stdin_and_policy(
        store,
        toolchain_store,
        &options.sandbox_id,
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND,
        vec![
            "qjs".to_string(),
            "--std".to_string(),
            "-e".to_string(),
            script,
        ],
        None,
        sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
        &node_id,
        &policy,
    )?;
    results.push(sandbox_tool_fidelity_result_from_outcome(
        "quickjs-writes-work-file",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND,
        qjs.response.exit_status,
        &qjs.stdout,
        &qjs.stderr,
        0,
        "",
    ));
    let grep = runner.run_toolchain_command_with_stdin_and_policy(
        store,
        toolchain_store,
        &options.sandbox_id,
        "grep",
        vec![
            "grep".to_string(),
            "workflow".to_string(),
            "/work/sinomo-coding-workflow.txt".to_string(),
        ],
        None,
        sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
        &node_id,
        &policy,
    )?;
    results.push(sandbox_tool_fidelity_result_from_outcome(
        "busybox-grep-reads-quickjs-output",
        "grep",
        grep.response.exit_status,
        &grep.stdout,
        &grep.stderr,
        0,
        "sinomo coding workflow\n",
    ));
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "Coding workflow fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

fn sandbox_toolchain_verify_coding_workflow_with_daemon(
    options: SandboxToolchainVerifyCodingOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon coding verification requires the local daemon".to_string(),
        });
    };
    let mut results = Vec::new();
    for case in CODING_WORKFLOW_BUSYBOX_CASES {
        let outcome = daemon_run_toolchain_command(
            &status.address,
            options.sandbox_id.clone(),
            case.command.to_string(),
            case.argv_vec(),
            case.stdin_bytes(),
            sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
        )?;
        results.push(sandbox_tool_fidelity_result_from_busybox_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    let qjs = daemon_run_toolchain_command(
        &status.address,
        options.sandbox_id.clone(),
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND.to_string(),
        vec![
            "qjs".to_string(),
            "--std".to_string(),
            "-e".to_string(),
            coding_workflow_quickjs_script(),
        ],
        None,
        sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
    )?;
    results.push(sandbox_tool_fidelity_result_from_outcome(
        "quickjs-writes-work-file",
        sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND,
        qjs.response.exit_status,
        &qjs.stdout,
        &qjs.stderr,
        0,
        "",
    ));
    let grep = daemon_run_toolchain_command(
        &status.address,
        options.sandbox_id,
        "grep".to_string(),
        vec![
            "grep".to_string(),
            "workflow".to_string(),
            "/work/sinomo-coding-workflow.txt".to_string(),
        ],
        None,
        sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
    )?;
    results.push(sandbox_tool_fidelity_result_from_outcome(
        "busybox-grep-reads-quickjs-output",
        "grep",
        grep.response.exit_status,
        &grep.stdout,
        &grep.stderr,
        0,
        "sinomo coding workflow\n",
    ));
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "Coding workflow fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

pub(crate) const CODING_WORKFLOW_BUSYBOX_CASES:
    &[sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase] = &[
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-cleanup-work-dir",
        command: "rm",
        argv: &["rm", "-rf", "/work/phase3"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-mkdir-work-dir",
        command: "mkdir",
        argv: &["mkdir", "/work/phase3"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-touch-file",
        command: "touch",
        argv: &["touch", "/work/phase3/file"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-printf-stdout",
        command: "printf",
        argv: &["printf", "hello"],
        stdin: None,
        expected_stdout: "hello",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-grep-stdin",
        command: "grep",
        argv: &["grep", "hell"],
        stdin: Some("hello\n"),
        expected_stdout: "hello\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-find-work-file",
        command: "find",
        argv: &["find", "/work/phase3", "-type", "f"],
        stdin: None,
        expected_stdout: "/work/phase3/file\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-diff-file-with-self",
        command: "diff",
        argv: &["diff", "/work/phase3/file", "/work/phase3/file"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-mkdir-src-dir",
        command: "mkdir",
        argv: &["mkdir", "/work/phase3/src"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-cp-work-file",
        command: "cp",
        argv: &["cp", "/work/phase3/file", "/work/phase3/src/copy"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-cmp-copy",
        command: "cmp",
        argv: &["cmp", "/work/phase3/file", "/work/phase3/src/copy"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-mv-copy",
        command: "mv",
        argv: &["mv", "/work/phase3/src/copy", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-test-renamed-file",
        command: "test",
        argv: &["test", "-f", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-chmod-renamed-file",
        command: "chmod",
        argv: &["chmod", "600", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-tar-create",
        command: "tar",
        argv: &[
            "tar",
            "-cf",
            "/work/phase3/archive.tar",
            "-C",
            "/work/phase3/src",
            "renamed",
        ],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-tar-list",
        command: "tar",
        argv: &["tar", "-tf", "/work/phase3/archive.tar"],
        stdin: None,
        expected_stdout: "renamed\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-sed-stdin",
        command: "sed",
        argv: &["sed", "s/beta/gamma/"],
        stdin: Some("alpha\nbeta\n"),
        expected_stdout: "alpha\ngamma\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-awk-stdin",
        command: "awk",
        argv: &["awk", "{sum += $1} END {print sum}"],
        stdin: Some("2\n3\n"),
        expected_stdout: "5\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-sort-stdin",
        command: "sort",
        argv: &["sort"],
        stdin: Some("b\na\n"),
        expected_stdout: "a\nb\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-wc-stdin",
        command: "wc",
        argv: &["wc", "-l"],
        stdin: Some("one\ntwo\n"),
        expected_stdout: "2\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-head-stdin",
        command: "head",
        argv: &["head", "-n", "1"],
        stdin: Some("first\nsecond\n"),
        expected_stdout: "first\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-tail-stdin",
        command: "tail",
        argv: &["tail", "-n", "1"],
        stdin: Some("first\nsecond\n"),
        expected_stdout: "second\n",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-rm-renamed-file",
        command: "rm",
        argv: &["rm", "/work/phase3/src/renamed"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
    sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase {
        name: "phase3-rmdir-src-dir",
        command: "rmdir",
        argv: &["rmdir", "/work/phase3/src"],
        stdin: None,
        expected_stdout: "",
        expected_exit_status: 0,
    },
];

fn coding_workflow_quickjs_script() -> String {
    "let f=std.open('/work/sinomo-coding-workflow.txt','w'); f.puts('sinomo coding workflow\\n'); f.close();".to_string()
}

fn sandbox_tool_fidelity_result_from_busybox_case(
    case: &sinomo_runtime::sandbox::SandboxBusyBoxFidelityCase,
    exit_status: i32,
    stdout: &[u8],
    stderr: &[u8],
) -> sinomo_runtime::sandbox::SandboxToolFidelityResult {
    sandbox_tool_fidelity_result_from_outcome(
        case.name,
        case.command,
        exit_status,
        stdout,
        stderr,
        case.expected_exit_status,
        case.expected_stdout,
    )
}

fn sandbox_tool_fidelity_result_from_outcome(
    case_name: impl Into<String>,
    command: impl Into<String>,
    exit_status: i32,
    stdout: &[u8],
    stderr: &[u8],
    expected_exit_status: i32,
    expected_stdout: &str,
) -> sinomo_runtime::sandbox::SandboxToolFidelityResult {
    let stdout = String::from_utf8_lossy(stdout).to_string();
    let stderr = String::from_utf8_lossy(stderr).to_string();
    let mismatch = if exit_status != expected_exit_status {
        Some(format!(
            "expected exit {expected_exit_status}, got {exit_status}"
        ))
    } else if stdout != expected_stdout {
        Some(format!(
            "expected stdout {:?}, got {:?}",
            expected_stdout, stdout
        ))
    } else {
        None
    };
    sinomo_runtime::sandbox::SandboxToolFidelityResult {
        case_name: case_name.into(),
        command: command.into(),
        passed: mismatch.is_none(),
        exit_status,
        stdout,
        stderr,
        mismatch,
    }
}

fn sandbox_tool_fidelity_result_from_command_case(
    case: &sinomo_runtime::sandbox::SandboxCommandFidelityCase,
    exit_status: i32,
    stdout: &[u8],
    stderr: &[u8],
) -> sinomo_runtime::sandbox::SandboxToolFidelityResult {
    let stdout = String::from_utf8_lossy(stdout).to_string();
    let stderr = String::from_utf8_lossy(stderr).to_string();
    let mismatch = if exit_status != case.expected_exit_status {
        Some(format!(
            "expected exit {}, got {exit_status}",
            case.expected_exit_status
        ))
    } else if let Some(expected) = case.expected_stdout {
        (stdout != expected).then(|| format!("expected stdout {:?}, got {:?}", expected, stdout))
    } else if let Some(expected) = case.expected_stdout_contains {
        (!stdout.contains(expected)).then(|| {
            format!(
                "expected stdout to contain {:?}, got {:?}",
                expected, stdout
            )
        })
    } else if let Some(expected) = case.expected_stderr_contains {
        (!stderr.contains(expected)).then(|| {
            format!(
                "expected stderr to contain {:?}, got {:?}",
                expected, stderr
            )
        })
    } else {
        None
    }
    .or_else(|| {
        case.expected_stderr_contains.and_then(|expected| {
            (!stderr.contains(expected)).then(|| {
                format!(
                    "expected stderr to contain {:?}, got {:?}",
                    expected, stderr
                )
            })
        })
    });
    sinomo_runtime::sandbox::SandboxToolFidelityResult {
        case_name: case.name.to_string(),
        command: case.command.to_string(),
        passed: mismatch.is_none(),
        exit_status,
        stdout,
        stderr,
        mismatch,
    }
}

fn sandbox_toolchain_verify_command_cases_with_stores(
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    allow_unsigned: bool,
    allowed_capabilities: &[&str],
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
    cases: &[sinomo_runtime::sandbox::SandboxCommandFidelityCase],
    failure_detail: &str,
) -> Result<(), CliError> {
    let runner = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?;
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = allow_unsigned;
    for capability in allowed_capabilities {
        policy
            .allowed_command_capabilities
            .insert((*capability).to_string());
    }
    let mut results = Vec::new();
    for case in cases {
        if !case.setup_files.is_empty() {
            let (_manifest, layout) = store.load(sandbox_id)?;
            let mut sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
            for (path, contents) in case.setup_files {
                sandbox_fs.write(
                    &sinomo_runtime::sandbox::SandboxPath::new((*path).to_string())?,
                    contents.as_bytes(),
                )?;
            }
        }
        let outcome = runner.run_toolchain_command_with_stdin_and_policy(
            store,
            toolchain_store,
            sandbox_id,
            case.command,
            case.argv_vec(),
            case.stdin_bytes(),
            case.timeout_ms(),
            &node_id,
            &policy,
        )?;
        results.push(sandbox_tool_fidelity_result_from_command_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: failure_detail.to_string(),
        });
    }
    Ok(())
}

fn sandbox_toolchain_verify_command_cases_with_daemon(
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    cases: &[sinomo_runtime::sandbox::SandboxCommandFidelityCase],
    failure_detail: &str,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon command verification requires the local daemon".to_string(),
        });
    };
    sandbox_toolchain_verify_command_cases_with_daemon_address(
        &status.address,
        sandbox_id,
        cases,
        failure_detail,
    )
}

fn sandbox_toolchain_verify_command_cases_with_daemon_address(
    daemon_address: &str,
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    cases: &[sinomo_runtime::sandbox::SandboxCommandFidelityCase],
    failure_detail: &str,
) -> Result<(), CliError> {
    let mut results = Vec::new();
    for case in cases {
        for (path, contents) in case.setup_files {
            daemon_sandbox_write_file_to_address(
                daemon_address,
                sandbox_id,
                sinomo_runtime::sandbox::SandboxPath::new((*path).to_string())?,
                contents.as_bytes().to_vec(),
            )?;
        }
        let outcome = daemon_run_toolchain_command(
            daemon_address,
            sandbox_id.clone(),
            case.command.to_string(),
            case.argv_vec(),
            case.stdin_bytes(),
            case.timeout_ms().max(120_000),
        )?;
        results.push(sandbox_tool_fidelity_result_from_command_case(
            case,
            outcome.response.exit_status,
            &outcome.stdout,
            &outcome.stderr,
        ));
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: failure_detail.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn sandbox_toolchain_verify_bash(
    options: SandboxToolchainVerifyBashOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[
                sinomo_runtime::sandbox::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            ],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            sinomo_runtime::sandbox::BASH_TOOLCHAIN_FIDELITY_CASES,
            "Bash fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            sinomo_runtime::sandbox::BASH_TOOLCHAIN_FIDELITY_CASES,
            "Bash fidelity verification failed",
        ),
    }
}

pub(crate) fn sandbox_toolchain_verify_make(
    options: SandboxToolchainVerifyMakeOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[
                sinomo_runtime::sandbox::SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY,
                sinomo_runtime::sandbox::SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
            ],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            sinomo_runtime::sandbox::MAKE_TOOLCHAIN_FIDELITY_CASES,
            "GNU Make fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            sinomo_runtime::sandbox::MAKE_TOOLCHAIN_FIDELITY_CASES,
            "GNU Make fidelity verification failed",
        ),
    }
}

pub(crate) fn sandbox_toolchain_verify_tcc(
    options: SandboxToolchainVerifyTccOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_verify_command_cases_with_stores(
            &options.sandbox_id,
            options.allow_unsigned,
            &[],
            &sandbox_store_from_data_dir(&data_dir),
            &sandbox_toolchain_store_from_data_dir(&data_dir),
            sinomo_runtime::sandbox::TCC_TOOLCHAIN_FIDELITY_CASES,
            "TinyCC fidelity verification failed",
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_verify_command_cases_with_daemon(
            &options.sandbox_id,
            sinomo_runtime::sandbox::TCC_TOOLCHAIN_FIDELITY_CASES,
            "TinyCC fidelity verification failed",
        ),
    }
}

pub(crate) fn sandbox_toolchain_verify_python(
    options: SandboxToolchainVerifyPythonOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => {
            sandbox_toolchain_verify_command_cases_with_stores(
                &options.sandbox_id,
                options.allow_unsigned,
                &[sinomo_runtime::sandbox::SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY],
                &sandbox_store_from_data_dir(&data_dir),
                &sandbox_toolchain_store_from_data_dir(&data_dir),
                sinomo_runtime::sandbox::PYTHON_TOOLCHAIN_FIDELITY_CASES,
                "Python fidelity verification failed",
            )?;
            sandbox_toolchain_verify_command_cases_with_stores(
                &options.sandbox_id,
                options.allow_unsigned,
                &[sinomo_runtime::sandbox::SANDBOX_PREVIEW1_PINNED_SUBPROCESS_CAPABILITY],
                &sandbox_store_from_data_dir(&data_dir),
                &sandbox_toolchain_store_from_data_dir(&data_dir),
                sinomo_runtime::sandbox::PIP_TOOLCHAIN_FIDELITY_CASES,
                "pip fidelity verification failed",
            )
        }
        SandboxMetadataExecution::Daemon => {
            let Some(status) = default_main_daemon_status()? else {
                return Err(CliError::DaemonNotRunning {
                    detail: "sandbox daemon command verification requires the local daemon"
                        .to_string(),
                });
            };
            sandbox_toolchain_verify_command_cases_with_daemon_address(
                &status.address,
                &options.sandbox_id,
                sinomo_runtime::sandbox::PYTHON_TOOLCHAIN_FIDELITY_CASES,
                "Python fidelity verification failed",
            )?;
            sandbox_toolchain_verify_command_cases_with_daemon_address(
                &status.address,
                &options.sandbox_id,
                sinomo_runtime::sandbox::PIP_TOOLCHAIN_FIDELITY_CASES,
                "pip fidelity verification failed",
            )
        }
    }
}

pub(crate) fn sandbox_toolchain_verify_quickjs_with_stores(
    options: SandboxToolchainVerifyQuickJsOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let runner = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?;
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = options.allow_unsigned;
    let mut results = Vec::new();
    for case in sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_FIDELITY_CASES {
        if !case.setup_files.is_empty() {
            let (_manifest, layout) = store.load(&options.sandbox_id)?;
            let mut sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
            for (path, contents) in case.setup_files {
                sandbox_fs.write(
                    &sinomo_runtime::sandbox::SandboxPath::new((*path).to_string())?,
                    contents.as_bytes(),
                )?;
            }
        }
        let outcome = runner.run_toolchain_command_with_stdin_and_policy(
            store,
            toolchain_store,
            &options.sandbox_id,
            sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND,
            case.argv_vec(),
            None,
            sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
            &node_id,
            &policy,
        )?;
        let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
        let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
        let mut mismatch = None;
        if outcome.response.exit_status != case.expected_exit_status {
            mismatch = Some(format!(
                "expected exit {}, got {}",
                case.expected_exit_status, outcome.response.exit_status
            ));
        } else if stdout != case.expected_stdout {
            mismatch = Some(format!(
                "expected stdout {:?}, got {:?}",
                case.expected_stdout, stdout
            ));
        } else if let Some(expected) = case.expected_stderr_contains
            && !stderr.contains(expected)
        {
            mismatch = Some(format!(
                "expected stderr to contain {:?}, got {:?}",
                expected, stderr
            ));
        }
        results.push(sinomo_runtime::sandbox::SandboxToolFidelityResult {
            case_name: case.name.to_string(),
            command: sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND.to_string(),
            passed: mismatch.is_none(),
            exit_status: outcome.response.exit_status,
            stdout,
            stderr,
            mismatch,
        });
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "QuickJS fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn sandbox_toolchain_verify_quickjs_with_daemon(
    options: SandboxToolchainVerifyQuickJsOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon QuickJS verification requires the local daemon".to_string(),
        });
    };
    let mut results = Vec::new();
    for case in sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_FIDELITY_CASES {
        if !case.setup_files.is_empty() {
            let setup_script = quickjs_setup_script(case.setup_files)?;
            let setup_outcome = daemon_run_toolchain_command(
                &status.address,
                options.sandbox_id.clone(),
                sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND.to_string(),
                vec![
                    "qjs".to_string(),
                    "--std".to_string(),
                    "-e".to_string(),
                    setup_script,
                ],
                None,
                sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
            )?;
            if setup_outcome.response.exit_status != 0 {
                results.push(sinomo_runtime::sandbox::SandboxToolFidelityResult {
                    case_name: format!("{}-setup", case.name),
                    command: sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND.to_string(),
                    passed: false,
                    exit_status: setup_outcome.response.exit_status,
                    stdout: String::from_utf8_lossy(&setup_outcome.stdout).to_string(),
                    stderr: String::from_utf8_lossy(&setup_outcome.stderr).to_string(),
                    mismatch: Some(format!(
                        "expected setup exit 0, got {}",
                        setup_outcome.response.exit_status
                    )),
                });
                continue;
            }
        }
        let outcome = daemon_run_toolchain_command(
            &status.address,
            options.sandbox_id.clone(),
            sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND.to_string(),
            case.argv_vec(),
            None,
            sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms,
        )?;
        let stdout = String::from_utf8_lossy(&outcome.stdout).to_string();
        let stderr = String::from_utf8_lossy(&outcome.stderr).to_string();
        let mut mismatch = None;
        if outcome.response.exit_status != case.expected_exit_status {
            mismatch = Some(format!(
                "expected exit {}, got {}",
                case.expected_exit_status, outcome.response.exit_status
            ));
        } else if stdout != case.expected_stdout {
            mismatch = Some(format!(
                "expected stdout {:?}, got {:?}",
                case.expected_stdout, stdout
            ));
        } else if let Some(expected) = case.expected_stderr_contains
            && !stderr.contains(expected)
        {
            mismatch = Some(format!(
                "expected stderr to contain {:?}, got {:?}",
                expected, stderr
            ));
        }
        results.push(sinomo_runtime::sandbox::SandboxToolFidelityResult {
            case_name: case.name.to_string(),
            command: sinomo_runtime::sandbox::QUICKJS_TOOLCHAIN_COMMAND.to_string(),
            passed: mismatch.is_none(),
            exit_status: outcome.response.exit_status,
            stdout,
            stderr,
            mismatch,
        });
    }
    print_busybox_fidelity_results(&results);
    if results.iter().any(|result| !result.passed) {
        return Err(CliError::Parse {
            detail: "QuickJS fidelity verification failed".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn quickjs_setup_script(setup_files: &[(&str, &str)]) -> Result<String, CliError> {
    let mut script = String::new();
    for (path, contents) in setup_files {
        let path = serde_json::to_string(path).map_err(|source| CliError::Parse {
            detail: format!("failed to encode QuickJS setup path: {source}"),
        })?;
        let contents = serde_json::to_string(contents).map_err(|source| CliError::Parse {
            detail: format!("failed to encode QuickJS setup contents: {source}"),
        })?;
        script.push_str("let f=std.open(");
        script.push_str(&path);
        script.push_str(",'w'); f.puts(");
        script.push_str(&contents);
        script.push_str("); f.close();\n");
    }
    Ok(script)
}

fn daemon_run_toolchain_command(
    daemon_address: &str,
    sandbox_id: sinomo_runtime::sandbox::SandboxId,
    command_name: String,
    argv: Vec<String>,
    stdin: Option<Vec<u8>>,
    timeout_ms: u64,
) -> Result<sinomo_runtime::sandbox::SandboxLocalCommandOutcome, CliError> {
    let request = daemon_run_toolchain_request(sandbox_id, command_name, argv, stdin, timeout_ms);
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_RUN_TOOLCHAIN_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        daemon_address,
        &rpc,
        sandbox_daemon_execution_rpc_timeout(timeout_ms),
    )?;
    response.decode_body().map_err(CliError::from)
}

fn sandbox_daemon_execution_rpc_timeout(timeout_ms: u64) -> std::time::Duration {
    let timeout_ms = if timeout_ms == 0 {
        15 * 60 * 1000
    } else {
        timeout_ms
    };
    std::time::Duration::from_millis(timeout_ms.saturating_add(10_000))
}

fn sandbox_daemon_large_metadata_rpc_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(15 * 60)
}

pub(crate) fn daemon_run_toolchain_request(
    sandbox_id: sinomo_runtime::sandbox::SandboxId,
    command_name: String,
    argv: Vec<String>,
    stdin: Option<Vec<u8>>,
    timeout_ms: u64,
) -> sinomo_runtime::sandbox::SandboxRunToolchainRequest {
    sinomo_runtime::sandbox::SandboxRunToolchainRequest {
        sandbox_id,
        command_name,
        argv,
        stdin,
        timeout_ms,
    }
}

pub(crate) fn sandbox_toolchain_import_busybox(
    options: SandboxToolchainImportBusyBoxOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_import_busybox_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_import_busybox_with_daemon(options),
    }
}

pub(crate) fn sandbox_toolchain_import_busybox_with_store(
    options: SandboxToolchainImportBusyBoxOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let spec = sinomo_runtime::sandbox::SandboxBusyBoxToolchainSpec::new(
        options.version,
        options.provenance,
        OffsetDateTime::now_utc(),
    )?;
    let manifest = sinomo_runtime::sandbox::install_busybox_toolchain_to_local_store(
        store,
        &spec,
        &module_bytes,
    )?;
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("commands: {}", manifest.commands.len());
    println!(
        "hash: {}",
        sinomo_runtime::sandbox::sandbox_module_hash(&module_bytes)
    );
    Ok(())
}

pub(crate) fn sandbox_toolchain_import_busybox_with_daemon(
    options: SandboxToolchainImportBusyBoxOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox BusyBox import requires the local daemon".to_string(),
        });
    };
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let request = sinomo_runtime::sandbox::SandboxImportBusyBoxToolchainRequest {
        version: options.version,
        provenance: options.provenance,
        module_bytes,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_IMPORT_BUSYBOX_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(600),
    )?;
    let response: sinomo_runtime::sandbox::SandboxImportBusyBoxToolchainResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("commands: {}", response.manifest.commands.len());
    println!("hash: {}", response.module_hash);
    println!(
        "sifs manifest: {}",
        sinomo_runtime::sandbox::sandbox_toolchain_manifest_path(&response.manifest.toolchain_id)
    );
    Ok(())
}

pub(crate) fn sandbox_toolchain_add_command_with_store(
    options: SandboxToolchainAddCommandOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let mut manifest = match store.load_manifest(&options.toolchain_id) {
        Ok((manifest, _layout)) => manifest,
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. }) => {
            sinomo_runtime::sandbox::SandboxToolchainManifest::new(
                options.toolchain_id.clone(),
                options.version.clone(),
                options.target_abi.clone(),
                options.provenance.clone(),
                OffsetDateTime::now_utc(),
            )?
        }
        Err(error) => return Err(error.into()),
    };
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    store.put_command_module_with_target_abi_capabilities_and_argv_prefix(
        &mut manifest,
        &options.command_name,
        &module_bytes,
        options.default_limits,
        Some(options.target_abi.clone()),
        options.required_capabilities,
        Vec::new(),
        BTreeMap::new(),
    )?;
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("command: {}", options.command_name);
    println!(
        "module: {}",
        sinomo_runtime::sandbox::sandbox_toolchain_command_path(
            &manifest.toolchain_id,
            &options.command_name
        )
    );
    println!(
        "hash: {}",
        sinomo_runtime::sandbox::sandbox_module_hash(&module_bytes)
    );
    Ok(())
}

pub(crate) fn sandbox_toolchain_add_command_with_daemon(
    options: SandboxToolchainAddCommandOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox toolchain command import requires the local daemon".to_string(),
        });
    };
    let module_bytes = std::fs::read(&options.module_path).map_err(|source| CliError::Io {
        path: options.module_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let request = sinomo_runtime::sandbox::SandboxAddToolchainCommandRequest {
        toolchain_id: options.toolchain_id,
        version: options.version,
        target_abi: options.target_abi,
        provenance: options.provenance,
        command_name: options.command_name,
        module_bytes,
        required_capabilities: options.required_capabilities,
        default_limits: options.default_limits,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_TOOLCHAIN_ADD_COMMAND_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        sandbox_daemon_large_metadata_rpc_timeout(),
    )?;
    let response: sinomo_runtime::sandbox::SandboxAddToolchainCommandResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("commands: {}", response.manifest.commands.len());
    println!("hash: {}", response.module_hash);
    println!(
        "sifs manifest: {}",
        sinomo_runtime::sandbox::sandbox_toolchain_manifest_path(&response.manifest.toolchain_id)
    );
    Ok(())
}

pub(crate) fn sandbox_toolchain_hash(options: SandboxToolchainHashOptions) -> Result<(), CliError> {
    let bytes = std::fs::read(&options.path).map_err(|source| CliError::Io {
        path: options.path.display().to_string(),
        detail: source.to_string(),
    })?;
    println!("{}", sinomo_runtime::sandbox::sandbox_module_hash(&bytes));
    Ok(())
}

pub(crate) fn sandbox_toolchain_add_artifact(
    options: SandboxToolchainAddArtifactOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_add_artifact_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_add_artifact_with_daemon(options),
    }
}

pub(crate) fn sandbox_toolchain_add_artifact_with_store(
    options: SandboxToolchainAddArtifactOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let mut manifest = match store.load_manifest(&options.toolchain_id) {
        Ok((manifest, _layout)) => manifest,
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. }) => {
            sinomo_runtime::sandbox::SandboxToolchainManifest::new(
                options.toolchain_id.clone(),
                options.version.clone(),
                sinomo_runtime::sandbox::SandboxWasiAbi::Preview1,
                options.provenance.clone(),
                OffsetDateTime::now_utc(),
            )?
        }
        Err(error) => return Err(error.into()),
    };
    let bytes = std::fs::read(&options.source_path).map_err(|source| CliError::Io {
        path: options.source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    store.put_artifact(&mut manifest, &options.relative_path, &bytes)?;
    let artifact = manifest.artifact(&options.relative_path)?;
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("artifact: {}", artifact.relative_path);
    println!("path: {}", artifact.sifs_path);
    println!("hash: {}", artifact.content_hash);
    println!("bytes: {}", artifact.byte_len);
    Ok(())
}

pub(crate) fn sandbox_toolchain_add_artifact_with_daemon(
    options: SandboxToolchainAddArtifactOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox toolchain artifact import requires the local daemon".to_string(),
        });
    };
    let bytes = std::fs::read(&options.source_path).map_err(|source| CliError::Io {
        path: options.source_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let request = sinomo_runtime::sandbox::SandboxAddToolchainArtifactRequest {
        toolchain_id: options.toolchain_id,
        version: options.version,
        provenance: options.provenance,
        relative_path: options.relative_path,
        bytes,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_TOOLCHAIN_ADD_ARTIFACT_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        sandbox_daemon_large_metadata_rpc_timeout(),
    )?;
    let response: sinomo_runtime::sandbox::SandboxAddToolchainArtifactResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("artifacts: {}", response.manifest.artifacts.len());
    println!("hash: {}", response.artifact_hash);
    println!(
        "sifs manifest: {}",
        sinomo_runtime::sandbox::sandbox_toolchain_manifest_path(&response.manifest.toolchain_id)
    );
    Ok(())
}

pub(crate) fn sandbox_toolchain_import_package(
    options: SandboxToolchainImportPackageOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    match options.execution {
        SandboxMetadataExecution::Local => sandbox_toolchain_import_package_with_store(
            options,
            &sandbox_toolchain_store_from_data_dir(&data_dir),
        ),
        SandboxMetadataExecution::Daemon => sandbox_toolchain_import_package_with_daemon(options),
    }
}

pub(crate) fn sandbox_toolchain_verify_package(
    options: SandboxToolchainVerifyPackageOptions,
) -> Result<(), CliError> {
    let loaded = load_sandbox_toolchain_package(&options.package_path)?;
    println!("package: {}", options.package_path.display());
    println!("toolchain: {}", loaded.spec.toolchain_id);
    println!("version: {}", loaded.spec.version);
    println!("commands: {}", loaded.commands.len());
    for command in &loaded.commands {
        let target_abi = command
            .target_abi
            .as_ref()
            .unwrap_or(&loaded.spec.target_abi);
        println!("  command: {} ({target_abi:?})", command.name);
    }
    println!("artifacts: {}", loaded.artifacts.len());
    for artifact in &loaded.artifacts {
        println!("  artifact: {}", artifact.relative_path);
    }
    Ok(())
}

pub(crate) fn sandbox_toolchain_import_package_with_store(
    options: SandboxToolchainImportPackageOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let package = load_sandbox_toolchain_package(&options.package_path)?;
    let target_toolchain_id = options
        .target_toolchain_id
        .clone()
        .unwrap_or_else(|| package.spec.toolchain_id.clone());
    let mut manifest = match store.load_manifest(&target_toolchain_id) {
        Ok((manifest, _layout)) => manifest,
        Err(sinomo_runtime::sandbox::SandboxToolchainError::NotFound { .. }) => {
            sinomo_runtime::sandbox::SandboxToolchainManifest::new(
                target_toolchain_id,
                package.spec.version.clone(),
                package.spec.target_abi.clone(),
                package.spec.provenance.clone(),
                OffsetDateTime::now_utc(),
            )?
        }
        Err(error) => return Err(error.into()),
    };
    validate_sandbox_toolchain_package_merge(&manifest, &package)?;
    for command in &package.commands {
        store.put_command_module_with_target_abi_capabilities_and_argv_prefix(
            &mut manifest,
            &command.name,
            &command.bytes,
            command.default_limits.clone(),
            Some(
                command
                    .target_abi
                    .clone()
                    .unwrap_or_else(|| package.spec.target_abi.clone()),
            ),
            command.required_capabilities.clone(),
            command.argv_prefix.clone(),
            command.default_env.clone(),
        )?;
        if let Some(exec_name) = &command.exec_name {
            manifest
                .commands
                .get_mut(&command.name)
                .expect("inserted command")
                .exec_name = Some(exec_name.clone());
            store.save_manifest(&manifest)?;
        }
    }
    for artifact in &package.artifacts {
        store.put_artifact(&mut manifest, &artifact.relative_path, &artifact.bytes)?;
    }
    println!("toolchain: {}", manifest.toolchain_id);
    println!("version: {}", manifest.version);
    println!("commands: {}", manifest.commands.len());
    println!("artifacts: {}", manifest.artifacts.len());
    Ok(())
}

fn validate_sandbox_toolchain_package_merge(
    manifest: &sinomo_runtime::sandbox::SandboxToolchainManifest,
    package: &LoadedSandboxToolchainPackage,
) -> Result<(), CliError> {
    for command in &package.commands {
        if manifest.commands.contains_key(&command.name) {
            return Err(
                sinomo_runtime::sandbox::SandboxToolchainError::DuplicateCommand {
                    name: command.name.clone(),
                }
                .into(),
            );
        }
    }
    for artifact in &package.artifacts {
        if manifest.artifacts.contains_key(&artifact.relative_path) {
            return Err(
                sinomo_runtime::sandbox::SandboxToolchainError::DuplicateArtifact {
                    path: artifact.relative_path.clone(),
                }
                .into(),
            );
        }
    }
    Ok(())
}

pub(crate) fn sandbox_toolchain_import_package_with_daemon(
    options: SandboxToolchainImportPackageOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox toolchain package import requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxImportToolchainPackageFromPathRequest {
        package_path: options.package_path.display().to_string(),
        toolchain_id: options.target_toolchain_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_TOOLCHAIN_IMPORT_PACKAGE_FROM_PATH_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(600),
    )?;
    let response: sinomo_runtime::sandbox::SandboxImportToolchainPackageResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("toolchain: {}", response.manifest.toolchain_id);
    println!("version: {}", response.manifest.version);
    println!("commands: {}", response.manifest.commands.len());
    println!("artifacts: {}", response.manifest.artifacts.len());
    println!("hash: {}", response.package_hash);
    Ok(())
}

struct LoadedSandboxToolchainPackage {
    spec: sinomo_runtime::sandbox::SandboxToolchainPackageSpec,
    commands: Vec<LoadedSandboxToolchainPackageCommand>,
    artifacts: Vec<LoadedSandboxToolchainPackageArtifact>,
}

struct LoadedSandboxToolchainPackageCommand {
    name: String,
    bytes: Vec<u8>,
    target_abi: Option<sinomo_runtime::sandbox::SandboxWasiAbi>,
    argv_prefix: Vec<String>,
    exec_name: Option<String>,
    default_env: BTreeMap<String, String>,
    required_capabilities: Vec<String>,
    default_limits: sinomo_runtime::sandbox::SandboxResourceLimits,
}

struct LoadedSandboxToolchainPackageArtifact {
    relative_path: String,
    bytes: Vec<u8>,
}

fn load_sandbox_toolchain_package(
    package_path: &Path,
) -> Result<LoadedSandboxToolchainPackage, CliError> {
    let (spec, package_dir) = read_sandbox_toolchain_package_spec(package_path)?;
    let mut command_names = BTreeSet::new();
    let mut commands = Vec::new();
    for command in &spec.commands {
        if !command_names.insert(command.name.clone()) {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate sandbox toolchain package command `{}`",
                    command.name
                ),
            });
        }
        let module_path = sandbox_toolchain_package_file_path(&package_dir, &command.module)?;
        let module_bytes = std::fs::read(&module_path).map_err(|source| CliError::Io {
            path: module_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_toolchain_package_hash(&module_path, &module_bytes, &command.content_hash)?;
        sinomo_runtime::sandbox::SandboxToolchainCommand::new(
            &spec.toolchain_id,
            &command.name,
            &module_bytes,
            command.default_limits.clone(),
        )?;
        if let Some(exec_name) = &command.exec_name {
            validate_sandbox_toolchain_package_exec_name(&command.name, exec_name)?;
        }
        for (index, arg) in command.argv_prefix.iter().enumerate() {
            if arg.contains('\0') {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package command `{}` argv_prefix[{index}] contains an embedded NUL byte",
                        command.name
                    ),
                });
            }
        }
        for capability in &command.required_capabilities {
            validate_sandbox_command_capability(
                capability,
                "sandbox toolchain command capability",
            )?;
        }
        for (name, value) in &command.default_env {
            if !sinomo_runtime::sandbox::sandbox_env_name_is_valid(name) {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package command `{}` has invalid default environment variable name `{name}`",
                        command.name
                    ),
                });
            }
            if value.contains('\0') {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package command `{}` default environment variable `{name}` contains an embedded NUL byte",
                        command.name
                    ),
                });
            }
        }
        commands.push(LoadedSandboxToolchainPackageCommand {
            name: command.name.clone(),
            bytes: module_bytes,
            target_abi: command.target_abi.clone(),
            argv_prefix: command.argv_prefix.clone(),
            exec_name: command.exec_name.clone(),
            default_env: command.default_env.clone(),
            required_capabilities: command.required_capabilities.clone(),
            default_limits: command.default_limits.clone(),
        });
    }

    let mut artifact_paths = BTreeSet::new();
    let mut artifacts = Vec::new();
    for artifact in &spec.artifacts {
        if !artifact_paths.insert(artifact.relative_path.clone()) {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate sandbox toolchain package artifact `{}`",
                    artifact.relative_path
                ),
            });
        }
        let source_path = sandbox_toolchain_package_file_path(&package_dir, &artifact.file)?;
        let bytes = std::fs::read(&source_path).map_err(|source| CliError::Io {
            path: source_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_toolchain_package_hash(&source_path, &bytes, &artifact.content_hash)?;
        sinomo_runtime::sandbox::SandboxToolchainArtifact::new(
            &spec.toolchain_id,
            &artifact.relative_path,
            &bytes,
        )?;
        artifacts.push(LoadedSandboxToolchainPackageArtifact {
            relative_path: artifact.relative_path.clone(),
            bytes,
        });
    }

    Ok(LoadedSandboxToolchainPackage {
        spec,
        commands,
        artifacts,
    })
}

fn validate_sandbox_toolchain_package_exec_name(
    command_name: &str,
    exec_name: &str,
) -> Result<(), CliError> {
    if exec_name.is_empty() {
        return Err(CliError::Parse {
            detail: format!(
                "sandbox toolchain package command `{command_name}` exec_name must be non-empty"
            ),
        });
    }
    if exec_name.contains('\0') {
        return Err(CliError::Parse {
            detail: format!(
                "sandbox toolchain package command `{command_name}` exec_name contains an embedded NUL byte"
            ),
        });
    }
    if let Some(path) = exec_name.strip_prefix('/') {
        let mut parts = Vec::new();
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    if parts.pop().is_none() {
                        return Err(CliError::Parse {
                            detail: format!(
                                "sandbox toolchain package command `{command_name}` exec_name escapes the sandbox root"
                            ),
                        });
                    }
                }
                part => parts.push(part),
            }
        }
        if parts.is_empty() {
            return Err(CliError::Parse {
                detail: format!(
                    "sandbox toolchain package command `{command_name}` exec_name must name a file"
                ),
            });
        }
        return Ok(());
    }
    let toolchain_id =
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named("exec_name_validation")
            .map_err(|source| CliError::Parse {
                detail: source.to_string(),
            })?;
    sinomo_runtime::sandbox::SandboxToolchainCommand::new(
        &toolchain_id,
        exec_name,
        &[],
        sinomo_runtime::sandbox::SandboxResourceLimits::default(),
    )?;
    Ok(())
}

fn read_sandbox_toolchain_package_spec(
    package_path: &Path,
) -> Result<
    (
        sinomo_runtime::sandbox::SandboxToolchainPackageSpec,
        PathBuf,
    ),
    CliError,
> {
    let bytes = std::fs::read(package_path).map_err(|source| CliError::Io {
        path: package_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let spec =
        serde_json::from_slice::<sinomo_runtime::sandbox::SandboxToolchainPackageSpec>(&bytes)
            .map_err(|source| CliError::Parse {
                detail: format!(
                    "invalid sandbox toolchain package manifest {}: {source}",
                    package_path.display()
                ),
            })?;
    let package_dir = package_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok((spec, package_dir))
}

fn sandbox_toolchain_package_file_path(
    package_dir: &Path,
    relative_path: &str,
) -> Result<PathBuf, CliError> {
    let path = Path::new(relative_path);
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(CliError::Parse {
            detail: format!(
                "sandbox toolchain package file path `{relative_path}` must be relative"
            ),
        });
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(CliError::Parse {
                    detail: format!(
                        "sandbox toolchain package file path `{relative_path}` cannot escape the package directory"
                    ),
                });
            }
        }
    }
    Ok(package_dir.join(path))
}

fn verify_sandbox_toolchain_package_hash(
    path: &Path,
    bytes: &[u8],
    expected_hash: &str,
) -> Result<(), CliError> {
    let actual_hash = sinomo_runtime::sandbox::sandbox_module_hash(bytes);
    if actual_hash == expected_hash {
        return Ok(());
    }
    Err(CliError::Parse {
        detail: format!(
            "sandbox toolchain package file {} hash mismatch: expected {expected_hash}, actual {actual_hash}",
            path.display()
        ),
    })
}

pub(crate) fn sandbox_create_with_store(
    options: SandboxCreateOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let sandbox_id = options
        .sandbox_id
        .clone()
        .unwrap_or_else(sinomo_runtime::sandbox::SandboxId::generate);
    let (mut manifest, layout) = store.create(
        sandbox_id,
        options.toolchain_id.clone(),
        OffsetDateTime::now_utc(),
    )?;
    manifest.temp_policy = options.temp_policy.clone();
    if let Some(memory_mb) = options.memory_mb {
        manifest.limits.memory_mb = memory_mb;
    }
    apply_sandbox_create_limit_options(&mut manifest.limits, &options);
    manifest.mounts = options.mounts;
    store.save_manifest(&manifest)?;

    println!("sandbox created: {}", manifest.sandbox_id);
    println!("workspace: {}", manifest.workspace_root);
    println!("local root: {}", layout.root.display());
    println!("manifest: {}", layout.manifest.display());
    println!("toolchain: {}", manifest.toolchain_id);
    println!(
        "tmp policy: {}",
        sandbox_temp_policy_label(&manifest.temp_policy)
    );
    if !manifest.mounts.is_empty() {
        println!("mounts:");
        for mount in &manifest.mounts {
            println!(
                "  {} -> {} ({})",
                mount.source_sifs_path,
                mount.mount_path,
                sandbox_mount_mode_label(&mount.mode)
            );
        }
    }
    Ok(())
}

pub(crate) fn sandbox_create_with_daemon(options: SandboxCreateOptions) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon create requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxCreateInstanceRequest {
        sandbox_id: options.sandbox_id,
        toolchain_id: options.toolchain_id,
        temp_policy: options.temp_policy,
        memory_mb: options.memory_mb,
        wall_time_ms: options.wall_time_ms,
        max_input_bytes: options.max_input_bytes,
        max_output_bytes: options.max_output_bytes,
        max_workspace_bytes: options.max_workspace_bytes,
        max_written_bytes_per_command: options.max_written_bytes_per_command,
        max_file_count: options.max_file_count,
        mounts: options.mounts,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_CREATE_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(60),
    )?;
    let response: sinomo_runtime::sandbox::SandboxCreateInstanceResponse =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_manifest_summary(&response.manifest, None);
    Ok(())
}

fn apply_sandbox_create_limit_options(
    limits: &mut sinomo_runtime::sandbox::SandboxResourceLimits,
    options: &SandboxCreateOptions,
) {
    if let Some(wall_time_ms) = options.wall_time_ms {
        limits.wall_time_ms = wall_time_ms;
    }
    if let Some(max_input_bytes) = options.max_input_bytes {
        limits.max_input_bytes = max_input_bytes;
    }
    if let Some(max_output_bytes) = options.max_output_bytes {
        limits.max_output_bytes = max_output_bytes;
    }
    if let Some(max_workspace_bytes) = options.max_workspace_bytes {
        limits.max_workspace_bytes = max_workspace_bytes;
    }
    if let Some(max_written_bytes_per_command) = options.max_written_bytes_per_command {
        limits.max_written_bytes_per_command = max_written_bytes_per_command;
    }
    if let Some(max_file_count) = options.max_file_count {
        limits.max_file_count = max_file_count;
    }
}

#[cfg(test)]
pub(crate) fn sandbox_run_with_store(
    options: SandboxRunOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    sandbox_run_with_stores(
        options,
        store,
        &sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(std::env::temp_dir()),
    )
}

#[cfg(test)]
pub(crate) fn sandbox_run_with_stores(
    options: SandboxRunOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    sandbox_run_with_stores_and_stdin(options, None, store, toolchain_store)
}

pub(crate) fn sandbox_process_plan_with_stores(
    options: SandboxProcessPlanOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let plan = read_sandbox_process_plan_file(&options.plan_path)?;
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
    policy.allow_unsigned_toolchains = true;
    let outcome = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?
        .run_toolchain_process_plan_with_policy_and_output_sink(
            store,
            toolchain_store,
            &options.sandbox_id,
            &plan,
            options.timeout_ms,
            &execution_node_id,
            &policy,
            Some(sandbox_cli_streaming_output_sink()),
        )?;
    print_sandbox_process_plan_outcome(&outcome, false)
}

pub(crate) fn sandbox_process_plan_with_daemon(
    options: SandboxProcessPlanOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon execution requires the local daemon".to_string(),
        });
    };
    let plan = read_sandbox_process_plan_file(&options.plan_path)?;
    let request = sinomo_runtime::sandbox::SandboxRunProcessPlanRequest {
        sandbox_id: options.sandbox_id,
        plan,
        timeout_ms: options.timeout_ms,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let timeout = sandbox_daemon_execution_rpc_timeout(options.timeout_ms);
    let mut final_outcome = None;
    sinomo_runtime::daemon::send_daemon_rpc_stream(&status.address, &rpc, timeout, |response| {
        if response.route != network::SANDBOX_RUN_PROCESS_PLAN_STREAM_ROUTE {
            return Err(sinomo_runtime::daemon::DaemonError::Parse {
                detail: format!(
                    "unexpected sandbox process-plan stream route {}",
                    response.route
                ),
            });
        }
        let frame: sinomo_runtime::sandbox::SandboxRunProcessPlanStreamFrame = response
            .decode_body()
            .map_err(sinomo_runtime::daemon::DaemonError::from)?;
        match frame {
            sinomo_runtime::sandbox::SandboxRunProcessPlanStreamFrame::Output { chunk } => {
                write_sandbox_output_chunk(&chunk).map_err(|source| {
                    sinomo_runtime::daemon::DaemonError::Network {
                        address: "stdout".to_string(),
                        detail: source.to_string(),
                    }
                })?;
                Ok(true)
            }
            sinomo_runtime::sandbox::SandboxRunProcessPlanStreamFrame::Complete { outcome } => {
                final_outcome = Some(outcome);
                Ok(false)
            }
            sinomo_runtime::sandbox::SandboxRunProcessPlanStreamFrame::Error { detail } => {
                Err(sinomo_runtime::daemon::DaemonError::Parse { detail })
            }
        }
    })?;
    let outcome = final_outcome.ok_or_else(|| CliError::Parse {
        detail: "sandbox process-plan stream ended without final outcome".to_string(),
    })?;
    print_sandbox_process_plan_outcome(&outcome, false)
}

fn read_sandbox_process_plan_file(
    plan_path: &Path,
) -> Result<sinomo_runtime::sandbox::SandboxProcessPlan, CliError> {
    let bytes = std::fs::read(plan_path).map_err(|source| CliError::Io {
        path: plan_path.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_slice(&bytes).map_err(|source| CliError::Parse {
        detail: format!(
            "invalid sandbox process plan {}: {source}",
            plan_path.display()
        ),
    })
}

pub(crate) fn sandbox_shell_with_stores(
    options: SandboxShellOptions,
    stdin: Vec<u8>,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    sandbox_run_with_stores_and_stdin(
        SandboxRunOptions {
            sandbox_id: options.sandbox_id,
            target: SandboxRunTarget::ToolchainCommand("sh".to_string()),
            argv: vec!["sh".to_string()],
            timeout_ms: options.timeout_ms,
            stdin: false,
            allow_unsigned: true,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Local,
        },
        Some(stdin),
        store,
        toolchain_store,
    )
}

pub(crate) fn sandbox_shell_with_daemon(
    options: SandboxShellOptions,
    stdin: Vec<u8>,
) -> Result<(), CliError> {
    sandbox_run_with_daemon_and_stdin(
        SandboxRunOptions {
            sandbox_id: options.sandbox_id,
            target: SandboxRunTarget::ToolchainCommand("sh".to_string()),
            argv: vec!["sh".to_string()],
            timeout_ms: options.timeout_ms,
            stdin: false,
            allow_unsigned: false,
            allowed_capabilities: Vec::new(),
            execution: SandboxRunExecution::Daemon,
        },
        Some(stdin),
    )
}

pub(crate) fn sandbox_run_with_stores_and_stdin(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    toolchain_store: &sinomo_runtime::sandbox::LocalSandboxToolchainStore,
) -> Result<(), CliError> {
    let (manifest, _layout) = store.load(&options.sandbox_id)?;
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    sinomo_runtime::sandbox::SandboxPolicy::default()
        .validate_manifest(&manifest, &execution_node_id)
        .map_err(|error| CliError::SandboxPolicyDenied {
            detail: error.to_string(),
        })?;
    match &options.target {
        SandboxRunTarget::WasmModule(module_path) => {
            let wasm = std::fs::read(module_path).map_err(|source| CliError::Io {
                path: module_path.display().to_string(),
                detail: source.to_string(),
            })?;
            let timeout_ms = if options.timeout_ms == 0 {
                sinomo_runtime::sandbox::SandboxResourceLimits::default().wall_time_ms
            } else {
                options.timeout_ms
            };
            let outcome = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?
                .run_module_with_stdin_limits_preopens_and_output_sink(
                    store,
                    &options.sandbox_id,
                    wasm,
                    options.argv,
                    stdin,
                    timeout_ms,
                    None,
                    Vec::new(),
                    Some(sandbox_cli_streaming_output_sink()),
                )?;
            print_sandbox_command_outcome(&outcome, false)
        }
        SandboxRunTarget::ToolchainCommand(command_name) => {
            let mut policy = sinomo_runtime::sandbox::SandboxPolicy::default();
            policy.allow_unsigned_toolchains = options.allow_unsigned;
            for capability in &options.allowed_capabilities {
                policy
                    .allowed_command_capabilities
                    .insert(capability.clone());
            }
            let outcome = sinomo_runtime::sandbox::LocalStagedSandboxCommandRunner::new()?
                .run_toolchain_command_with_stdin_policy_and_output_sink(
                    store,
                    toolchain_store,
                    &options.sandbox_id,
                    command_name,
                    options.argv,
                    stdin,
                    options.timeout_ms,
                    &execution_node_id,
                    &policy,
                    Some(sandbox_cli_streaming_output_sink()),
                )?;
            print_sandbox_command_outcome(&outcome, false)
        }
    }
}

fn sandbox_cli_streaming_output_sink()
-> std::sync::Arc<dyn sinomo_runtime::sandbox::SandboxOutputSink> {
    std::sync::Arc::new(
        |chunk: sinomo_runtime::sandbox::SandboxOutputChunk| match chunk.stream {
            sinomo_runtime::sandbox::SandboxOutputStream::Stdout => {
                let mut stdout = io::stdout().lock();
                let _ = stdout.write_all(&chunk.bytes);
                let _ = stdout.flush();
            }
            sinomo_runtime::sandbox::SandboxOutputStream::Stderr => {
                let mut stderr = io::stderr().lock();
                let _ = stderr.write_all(&chunk.bytes);
                let _ = stderr.flush();
            }
        },
    )
}

pub(crate) fn sandbox_run_with_daemon_and_stdin(
    options: SandboxRunOptions,
    stdin: Option<Vec<u8>>,
) -> Result<(), CliError> {
    let SandboxRunTarget::ToolchainCommand(command_name) = &options.target else {
        return Err(CliError::Parse {
            detail: "sandbox daemon execution requires a pinned toolchain command".to_string(),
        });
    };
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon execution requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxRunToolchainRequest {
        sandbox_id: options.sandbox_id,
        command_name: command_name.clone(),
        argv: options.argv,
        stdin,
        timeout_ms: options.timeout_ms,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let timeout = sandbox_daemon_execution_rpc_timeout(options.timeout_ms);
    let mut final_outcome = None;
    sinomo_runtime::daemon::send_daemon_rpc_stream(&status.address, &rpc, timeout, |response| {
        if response.route != network::SANDBOX_RUN_TOOLCHAIN_STREAM_ROUTE {
            return Err(sinomo_runtime::daemon::DaemonError::Parse {
                detail: format!("unexpected sandbox stream route {}", response.route),
            });
        }
        let frame: sinomo_runtime::sandbox::SandboxRunToolchainStreamFrame = response
            .decode_body()
            .map_err(sinomo_runtime::daemon::DaemonError::from)?;
        match frame {
            sinomo_runtime::sandbox::SandboxRunToolchainStreamFrame::Output { chunk } => {
                write_sandbox_output_chunk(&chunk).map_err(|source| {
                    sinomo_runtime::daemon::DaemonError::Network {
                        address: "stdout".to_string(),
                        detail: source.to_string(),
                    }
                })?;
                Ok(true)
            }
            sinomo_runtime::sandbox::SandboxRunToolchainStreamFrame::Complete { outcome } => {
                final_outcome = Some(outcome);
                Ok(false)
            }
            sinomo_runtime::sandbox::SandboxRunToolchainStreamFrame::Error { detail } => {
                Err(sinomo_runtime::daemon::DaemonError::Parse { detail })
            }
        }
    })?;
    let outcome = final_outcome.ok_or_else(|| CliError::Parse {
        detail: "sandbox stream ended without final outcome".to_string(),
    })?;
    print_sandbox_command_outcome(&outcome, false)
}

fn write_sandbox_output_chunk(
    chunk: &sinomo_runtime::sandbox::SandboxOutputChunk,
) -> Result<(), CliError> {
    match chunk.stream {
        sinomo_runtime::sandbox::SandboxOutputStream::Stdout => {
            let mut stdout = io::stdout().lock();
            stdout.write_all(&chunk.bytes).map_err(CliError::stdio)?;
            stdout.flush().map_err(CliError::stdio)
        }
        sinomo_runtime::sandbox::SandboxOutputStream::Stderr => {
            let mut stderr = io::stderr().lock();
            stderr.write_all(&chunk.bytes).map_err(CliError::stdio)?;
            stderr.flush().map_err(CliError::stdio)
        }
    }
}

fn print_sandbox_command_outcome(
    outcome: &sinomo_runtime::sandbox::SandboxLocalCommandOutcome,
    replay_output: bool,
) -> Result<(), CliError> {
    if replay_output {
        let mut stdout = io::stdout().lock();
        let mut stderr = io::stderr().lock();
        for chunk in &outcome.output_chunks {
            match chunk.stream {
                sinomo_runtime::sandbox::SandboxOutputStream::Stdout => {
                    stdout.write_all(&chunk.bytes).map_err(CliError::stdio)?;
                }
                sinomo_runtime::sandbox::SandboxOutputStream::Stderr => {
                    stderr.write_all(&chunk.bytes).map_err(CliError::stdio)?;
                }
            }
        }
        stdout.flush().map_err(CliError::stdio)?;
        stderr.flush().map_err(CliError::stdio)?;
    }

    println!("sandbox command: {}", outcome.response.command_id);
    println!("exit status: {}", outcome.response.exit_status);
    println!("stdout: {}", outcome.response.stdout_ref);
    println!("stderr: {}", outcome.response.stderr_ref);
    if !outcome.response.changed_paths.is_empty() {
        println!("changed paths:");
        for path in &outcome.response.changed_paths {
            println!("  {path}");
        }
    }
    sandbox_command_status_result(
        outcome.response.command_id.as_str(),
        outcome.response.exit_status,
        Some(outcome.response.stderr_ref.as_str()),
    )
}

fn print_sandbox_process_plan_outcome(
    outcome: &sinomo_runtime::sandbox::SandboxProcessPlanOutcome,
    replay_output: bool,
) -> Result<(), CliError> {
    if replay_output {
        let mut stdout = io::stdout().lock();
        let mut stderr = io::stderr().lock();
        stdout.write_all(&outcome.stdout).map_err(CliError::stdio)?;
        stderr.write_all(&outcome.stderr).map_err(CliError::stdio)?;
        stdout.flush().map_err(CliError::stdio)?;
        stderr.flush().map_err(CliError::stdio)?;
    }
    println!(
        "sandbox process plan steps: {}",
        outcome.step_outcomes.len()
    );
    println!("exit status: {}", outcome.exit_status);
    if let Some(last) = outcome.step_outcomes.last() {
        println!("final command: {}", last.response.command_id);
        println!("stdout: {}", last.response.stdout_ref);
        println!("stderr: {}", last.response.stderr_ref);
    }
    sandbox_command_status_result("sandbox process plan", outcome.exit_status, None)
}

fn sandbox_command_status_result(
    command: &str,
    exit_status: i32,
    stderr_ref: Option<&str>,
) -> Result<(), CliError> {
    if exit_status == 0 {
        return Ok(());
    }
    Err(CliError::Process {
        command: command.to_string(),
        status: exit_status,
        stderr: stderr_ref
            .map(|path| format!("sandbox command failed; stderr log: {path}"))
            .unwrap_or_else(|| "sandbox command failed".to_string()),
    })
}

pub(crate) fn sandbox_status_with_store(
    options: SandboxStatusOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (manifest, layout) = store.load(&options.sandbox_id)?;
    print_sandbox_manifest_summary(&manifest, Some(&layout));
    Ok(())
}

pub(crate) fn sandbox_status_with_daemon(options: SandboxStatusOptions) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon status requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxStatusRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_STATUS_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(10),
    )?;
    let response: sinomo_runtime::sandbox::SandboxStatusResponse =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_manifest_summary(&response.manifest, None);
    Ok(())
}

pub(crate) fn sandbox_config_with_store(
    options: SandboxConfigOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    validate_sandbox_config_env_options(&options)?;
    let (mut manifest, layout) = store.load(&options.sandbox_id)?;
    if let Some(cwd) = options.cwd.as_ref() {
        let fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
        sinomo_runtime::sandbox::validate_sandbox_working_directory(&fs, cwd)?;
    }
    apply_sandbox_config_options(&mut manifest, options);
    manifest.updated_at = OffsetDateTime::now_utc();
    store.save_manifest(&manifest)?;
    print_sandbox_manifest_summary(&manifest, Some(&store.layout(&manifest.sandbox_id)));
    Ok(())
}

pub(crate) fn sandbox_config_with_daemon(options: SandboxConfigOptions) -> Result<(), CliError> {
    validate_sandbox_config_env_options(&options)?;
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon config requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxUpdateInstanceRequest {
        sandbox_id: options.sandbox_id,
        cwd: options.cwd,
        env_set: options.env_set,
        env_unset: options.env_unset,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_UPDATE_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(10),
    )?;
    let response: sinomo_runtime::sandbox::SandboxUpdateInstanceResponse =
        response.decode_body().map_err(CliError::from)?;
    print_sandbox_manifest_summary(&response.manifest, None);
    Ok(())
}

fn apply_sandbox_config_options(
    manifest: &mut sinomo_runtime::sandbox::SandboxInstanceManifest,
    options: SandboxConfigOptions,
) {
    if let Some(cwd) = options.cwd {
        manifest.cwd = cwd;
    }
    for key in options.env_unset {
        manifest.env.remove(&key);
    }
    manifest.env.extend(options.env_set);
}

fn validate_sandbox_config_env_options(options: &SandboxConfigOptions) -> Result<(), CliError> {
    for (key, value) in &options.env_set {
        validate_sandbox_env_name(key)?;
        validate_sandbox_env_value(key, value)?;
    }
    for key in &options.env_unset {
        validate_sandbox_env_name(key)?;
    }
    Ok(())
}

fn print_sandbox_manifest_summary(
    manifest: &sinomo_runtime::sandbox::SandboxInstanceManifest,
    layout: Option<&sinomo_runtime::sandbox::LocalSandboxInstanceLayout>,
) {
    println!("sandbox: {}", manifest.sandbox_id);
    println!("workspace: {}", manifest.workspace_root);
    if let Some(layout) = layout {
        println!("local root: {}", layout.root.display());
        println!("manifest: {}", layout.manifest.display());
    } else {
        println!(
            "sifs manifest: {}",
            sinomo_runtime::sandbox::sandbox_manifest_path(&manifest.sandbox_id)
        );
    }
    println!("toolchain: {}", manifest.toolchain_id);
    println!("cwd: {}", manifest.cwd);
    println!("env vars: {}", manifest.env.len());
    println!(
        "tmp policy: {}",
        sandbox_temp_policy_label(&manifest.temp_policy)
    );
    println!("memory mb: {}", manifest.limits.memory_mb);
    println!("wall time ms: {}", manifest.limits.wall_time_ms);
    println!("mounts: {}", manifest.mounts.len());
    println!("command history: {}", manifest.command_history.len());
    if !manifest.command_history.is_empty() {
        println!("recent commands:");
        for command_id in manifest.command_history.iter().rev().take(5).rev() {
            println!("  {command_id}");
        }
    }
}

pub(crate) fn sandbox_logs_with_store(
    options: SandboxLogsOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (manifest, layout) = store.load(&options.sandbox_id)?;
    let sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    match options.command_id {
        Some(command_id) => {
            let stdout_path =
                sinomo_runtime::sandbox::sandbox_command_stdout_log_path(&command_id)?;
            let stderr_path =
                sinomo_runtime::sandbox::sandbox_command_stderr_log_path(&command_id)?;
            let record = sinomo_runtime::sandbox::load_command_record(&sandbox_fs, &command_id)?;
            print_sandbox_command_record_summary(&record);
            let stdout = sandbox_fs.read(&stdout_path, None)?;
            let stderr = sandbox_fs.read(&stderr_path, None)?;
            print!("{}", String::from_utf8_lossy(&stdout));
            eprint!("{}", String::from_utf8_lossy(&stderr));
        }
        None => {
            if manifest.command_history.is_empty() {
                for entry in sandbox_fs.list(
                    &sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("static path"),
                )? {
                    println!("{}", entry.path);
                }
            } else {
                print_sandbox_command_history(&sandbox_fs, &manifest.command_history)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn sandbox_logs_with_daemon(options: SandboxLogsOptions) -> Result<(), CliError> {
    match options.command_id {
        Some(command_id) => {
            let stdout_path =
                sinomo_runtime::sandbox::sandbox_command_stdout_log_path(&command_id)?;
            let stderr_path =
                sinomo_runtime::sandbox::sandbox_command_stderr_log_path(&command_id)?;
            let record_path =
                sinomo_runtime::sandbox::sandbox_command_record_log_path(&command_id)?;
            let record_bytes = daemon_sandbox_read_file(&options.sandbox_id, record_path, None)?;
            let record = sinomo_runtime::sandbox::decode_command_record(&record_bytes)?;
            print_sandbox_command_record_summary(&record);
            let stdout = daemon_sandbox_read_file(&options.sandbox_id, stdout_path, None)?;
            let stderr = daemon_sandbox_read_file(&options.sandbox_id, stderr_path, None)?;
            print!("{}", String::from_utf8_lossy(&stdout));
            eprint!("{}", String::from_utf8_lossy(&stderr));
        }
        None => {
            let manifest = daemon_sandbox_status(&options.sandbox_id)?;
            if manifest.command_history.is_empty() {
                for entry in daemon_sandbox_list(
                    &options.sandbox_id,
                    sinomo_runtime::sandbox::SandboxPath::new("/logs").expect("static path"),
                )? {
                    println!("{}", entry.path);
                }
            } else {
                for command_id in &manifest.command_history {
                    let record_path =
                        sinomo_runtime::sandbox::sandbox_command_record_log_path(command_id)?;
                    let record_bytes =
                        daemon_sandbox_read_file(&options.sandbox_id, record_path, None)?;
                    let record = sinomo_runtime::sandbox::decode_command_record(&record_bytes)?;
                    print_sandbox_command_history_line(&record);
                }
            }
        }
    }
    Ok(())
}

fn print_sandbox_command_history<F: sinomo_runtime::sandbox::SandboxFileSystem>(
    sandbox_fs: &F,
    command_history: &[sinomo_runtime::sandbox::SandboxCommandId],
) -> Result<(), CliError> {
    for command_id in command_history {
        let record = sinomo_runtime::sandbox::load_command_record(sandbox_fs, command_id)?;
        print_sandbox_command_history_line(&record);
    }
    Ok(())
}

fn print_sandbox_command_history_line(record: &sinomo_runtime::sandbox::SandboxCommandRecord) {
    let exit_status = record
        .response
        .as_ref()
        .map(|response| response.exit_status.to_string())
        .unwrap_or_else(|| "-".to_string());
    println!(
        "{}\t{}\texit={}\t{}",
        record.command_id,
        sandbox_command_record_status_label(&record.status),
        exit_status,
        record.argv.join(" ")
    );
}

pub(crate) fn sandbox_ls_with_store(
    options: SandboxLsOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (_manifest, layout) = store.load(&options.sandbox_id)?;
    let sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    for entry in sandbox_fs.list(&options.path)? {
        let kind = if entry.metadata.is_dir {
            "dir "
        } else {
            "file"
        };
        println!("{kind}\t{}\t{}", entry.metadata.len, entry.path);
    }
    Ok(())
}

pub(crate) fn sandbox_ls_with_daemon(options: SandboxLsOptions) -> Result<(), CliError> {
    for entry in daemon_sandbox_list(&options.sandbox_id, options.path)? {
        let kind = if entry.metadata.is_dir {
            "dir "
        } else {
            "file"
        };
        println!("{kind}\t{}\t{}", entry.metadata.len, entry.path);
    }
    Ok(())
}

pub(crate) fn sandbox_cat_with_store(
    options: SandboxCatOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (_manifest, layout) = store.load(&options.sandbox_id)?;
    let sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    let bytes = sandbox_fs.read(&options.path, options.range)?;
    io::stdout()
        .lock()
        .write_all(&bytes)
        .map_err(CliError::stdio)
}

pub(crate) fn sandbox_cat_with_daemon(options: SandboxCatOptions) -> Result<(), CliError> {
    let bytes = daemon_sandbox_read_file(&options.sandbox_id, options.path, options.range)?;
    io::stdout()
        .lock()
        .write_all(&bytes)
        .map_err(CliError::stdio)
}

#[cfg(test)]
pub(crate) fn sandbox_write_with_store(
    options: SandboxWriteOptions,
    bytes: Vec<u8>,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let mut event_store = sinomo_runtime::events::InMemoryEventStore::new();
    sandbox_write_with_store_and_event_store(options, bytes, store, &mut event_store)
}

pub(crate) fn sandbox_write_with_store_and_event_store<S>(
    options: SandboxWriteOptions,
    bytes: Vec<u8>,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    event_store: &mut S,
) -> Result<(), CliError>
where
    S: EventStore,
{
    let (manifest, layout) = store.load(&options.sandbox_id)?;
    let mut sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    if let Err(error) = sinomo_runtime::sandbox::validate_sandbox_write_resource_limits(
        &sandbox_fs,
        &options.path,
        bytes.len() as u64,
        &manifest.limits,
    ) {
        if let sinomo_runtime::sandbox::SandboxFsError::QuotaExceeded {
            kind,
            actual,
            limit,
        } = &error
        {
            let event = sinomo_runtime::sandbox::sandbox_resource_exhausted_event(
                NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
                    detail: format!("invalid sandbox CLI node id: {source}"),
                })?,
                &options.sandbox_id,
                "sandbox-write",
                kind.clone(),
                *actual,
                *limit,
            );
            event_store.append(event)?;
        }
        append_local_sandbox_write_denial_event(
            event_store,
            &options.sandbox_id,
            &options.path,
            &error,
        )?;
        return Err(error.into());
    }
    if let Err(error) = sandbox_fs.write(&options.path, &bytes) {
        append_local_sandbox_write_denial_event(
            event_store,
            &options.sandbox_id,
            &options.path,
            &error,
        )?;
        return Err(error.into());
    }
    if !sinomo_runtime::sandbox::sandbox_path_is_virtual_device_sink(&options.path) {
        let event = sinomo_runtime::sandbox::sandbox_file_written_event(
            NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
                detail: format!("invalid sandbox CLI node id: {source}"),
            })?,
            &options.sandbox_id,
            &options.path,
            bytes.len() as u64,
        );
        event_store.append(event)?;
    }
    println!("wrote {} bytes to {}", bytes.len(), options.path);
    Ok(())
}

fn append_local_sandbox_write_denial_event<S>(
    event_store: &mut S,
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    request_path: &sinomo_runtime::sandbox::SandboxPath,
    error: &sinomo_runtime::sandbox::SandboxFsError,
) -> Result<(), CliError>
where
    S: EventStore,
{
    let sinomo_runtime::sandbox::SandboxFsError::ReadOnly { path } = error else {
        return Ok(());
    };
    let node_id = NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox CLI node id: {source}"),
    })?;
    let denial = sinomo_runtime::sandbox::SandboxPolicyDenial::new(
        "mount-read-only-denied",
        format!("write to read-only sandbox path {path} denied for request path {request_path}"),
    );
    event_store.append(sinomo_runtime::sandbox::sandbox_policy_denied_event(
        node_id.clone(),
        Some(sandbox_id),
        "sandbox-write",
        &denial,
    ))?;
    event_store.append(sinomo_runtime::sandbox::sandbox_mount_denied_event(
        node_id,
        Some(sandbox_id),
        "sandbox-write",
        &denial,
    ))?;
    Ok(())
}

pub(crate) fn sandbox_write_with_daemon(
    options: SandboxWriteOptions,
    bytes: Vec<u8>,
) -> Result<(), CliError> {
    let response = daemon_sandbox_write_file(&options.sandbox_id, options.path, bytes)?;
    println!(
        "wrote {} bytes to {}",
        response.bytes_written, response.path
    );
    Ok(())
}

fn daemon_sandbox_list(
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    path: sinomo_runtime::sandbox::SandboxPath,
) -> Result<Vec<sinomo_runtime::sandbox::SandboxDirEntry>, CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon list requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxListRequest {
        sandbox_id: sandbox_id.clone(),
        path,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_LIST_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(10),
    )?;
    let response: sinomo_runtime::sandbox::SandboxListResponse =
        response.decode_body().map_err(CliError::from)?;
    Ok(response.entries)
}

fn daemon_sandbox_status(
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
) -> Result<sinomo_runtime::sandbox::SandboxInstanceManifest, CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon status requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxStatusRequest {
        sandbox_id: sandbox_id.clone(),
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_STATUS_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(10),
    )?;
    let response: sinomo_runtime::sandbox::SandboxStatusResponse =
        response.decode_body().map_err(CliError::from)?;
    Ok(response.manifest)
}

fn daemon_sandbox_read_file(
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    path: sinomo_runtime::sandbox::SandboxPath,
    range: Option<sinomo_runtime::sandbox::ByteRange>,
) -> Result<Vec<u8>, CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon read requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxReadRequest {
        sandbox_id: sandbox_id.clone(),
        path,
        range,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_READ_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(10),
    )?;
    let response: sinomo_runtime::sandbox::SandboxReadResponse =
        response.decode_body().map_err(CliError::from)?;
    Ok(response.bytes)
}

fn daemon_sandbox_write_file(
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    path: sinomo_runtime::sandbox::SandboxPath,
    bytes: Vec<u8>,
) -> Result<sinomo_runtime::sandbox::SandboxWriteResponse, CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon write requires the local daemon".to_string(),
        });
    };
    daemon_sandbox_write_file_to_address(&status.address, sandbox_id, path, bytes)
}

fn daemon_sandbox_write_file_to_address(
    daemon_address: &str,
    sandbox_id: &sinomo_runtime::sandbox::SandboxId,
    path: sinomo_runtime::sandbox::SandboxPath,
    bytes: Vec<u8>,
) -> Result<sinomo_runtime::sandbox::SandboxWriteResponse, CliError> {
    let request = sinomo_runtime::sandbox::SandboxWriteRequest {
        sandbox_id: sandbox_id.clone(),
        path,
        bytes,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_WRITE_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        daemon_address,
        &rpc,
        std::time::Duration::from_secs(60),
    )?;
    response.decode_body().map_err(CliError::from)
}

pub(crate) fn sandbox_fetch_with_store_transport_and_event_store<T, S>(
    options: SandboxFetchOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
    policy: &sinomo_runtime::sandbox::SandboxPolicy,
    transport: &T,
    event_store: &mut S,
) -> Result<sinomo_runtime::sandbox::SandboxHttpFetchFileResult, CliError>
where
    T: sinomo_runtime::sandbox::SandboxHttpTransport,
    S: EventStore,
{
    let (manifest, layout) = store.load(&options.sandbox_id)?;
    let execution_node_id =
        NodeId::from_legacy_named("sandbox_cli").map_err(|source| CliError::Parse {
            detail: format!("invalid sandbox CLI node id: {source}"),
        })?;
    let mut request = sinomo_runtime::sandbox::SandboxHttpRequest::get(options.url);
    request.timeout_ms = options.timeout_ms;
    request.max_response_bytes = options.max_response_bytes;
    request.headers = options.headers;
    if let Err(denial) = policy.validate_manifest(&manifest, &execution_node_id) {
        let event = sinomo_runtime::sandbox::sandbox_policy_denied_event(
            execution_node_id.clone(),
            Some(&manifest.sandbox_id),
            "sandbox-fetch",
            &denial,
        );
        event_store.append(event)?;
        return Err(CliError::SandboxPolicyDenied {
            detail: denial.to_string(),
        });
    }
    if let Err(denial) = policy.validate_http_request(&request) {
        let event = sinomo_runtime::sandbox::sandbox_policy_denied_event(
            execution_node_id.clone(),
            Some(&manifest.sandbox_id),
            "sandbox-fetch",
            &denial,
        );
        event_store.append(event)?;
        return Err(CliError::SandboxPolicyDenied {
            detail: denial.to_string(),
        });
    }
    let mut sandbox_fs = sinomo_runtime::sandbox::LocalSandboxFileSystem::new(layout);
    let command_id = sinomo_runtime::sandbox::SandboxCommandId::generate();
    let result = match sinomo_runtime::sandbox::sandbox_http_fetch_to_file(
        policy,
        transport,
        &mut sandbox_fs,
        manifest.sandbox_id.clone(),
        command_id.clone(),
        &request,
        &options.output_path,
        &manifest.limits,
    ) {
        Ok(result) => result,
        Err(error) => {
            if let sinomo_runtime::sandbox::SandboxRecordedHttpFetchError::Fs(
                sinomo_runtime::sandbox::SandboxFsError::QuotaExceeded {
                    kind,
                    actual,
                    limit,
                },
            ) = &error
            {
                let event = sinomo_runtime::sandbox::sandbox_resource_exhausted_event(
                    execution_node_id.clone(),
                    &manifest.sandbox_id,
                    "sandbox-fetch",
                    kind.clone(),
                    *actual,
                    *limit,
                );
                event_store.append(event)?;
            }
            if let Some(record) =
                sandbox_http_exchange_record_for_command(&sandbox_fs, &command_id)?
            {
                let event = sinomo_runtime::sandbox::sandbox_http_exchange_event(
                    execution_node_id,
                    &record,
                );
                event_store.append(event)?;
            }
            return Err(error.into());
        }
    };
    let file_event = sinomo_runtime::sandbox::sandbox_file_written_event_with_context(
        execution_node_id.clone(),
        &manifest.sandbox_id,
        &options.output_path,
        result.response_bytes,
        "sandbox-fetch",
        Some(&result.record.command_id),
        Some(&result.request_id),
    );
    event_store.append(file_event)?;
    let event =
        sinomo_runtime::sandbox::sandbox_http_exchange_event(execution_node_id, &result.record);
    event_store.append(event)?;
    print_sandbox_http_fetch_file_result(&result);
    Ok(result)
}

fn print_sandbox_http_fetch_file_result(
    result: &sinomo_runtime::sandbox::SandboxHttpFetchFileResult,
) {
    println!("request: {}", result.request_id);
    println!("status: {}", result.status);
    println!("bytes: {}", result.response_bytes);
    println!("output: {}", result.output_path);
    println!(
        "audit: {}",
        sinomo_runtime::sandbox::sandbox_http_exchange_record_path(
            &result.record.sandbox_id,
            &result.record.command_id,
            &result.request_id,
        )
    );
}

fn sandbox_http_exchange_record_for_command<F>(
    sandbox_fs: &F,
    command_id: &sinomo_runtime::sandbox::SandboxCommandId,
) -> Result<Option<sinomo_runtime::sandbox::SandboxHttpExchangeRecord>, CliError>
where
    F: sinomo_runtime::sandbox::SandboxFileSystem,
{
    let logs_path = sinomo_runtime::sandbox::SandboxPath::new("/logs")?;
    let mut entries = sandbox_fs.list(&logs_path)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let prefix = format!("/logs/{command_id}.");
    for entry in entries {
        let path = entry.path;
        let path_text = path.as_str();
        if path_text.starts_with(&prefix) && path_text.ends_with(".http.sobj") {
            let bytes = sandbox_fs.read(&path, None)?;
            return Ok(Some(sinomo_runtime::sandbox::decode_http_exchange_record(
                &bytes,
            )?));
        }
    }
    Ok(None)
}

struct DaemonMediatedSandboxHttpTransport {
    address: String,
}

impl sinomo_runtime::sandbox::SandboxHttpTransport for DaemonMediatedSandboxHttpTransport {
    fn fetch(
        &self,
        request: &sinomo_runtime::sandbox::SandboxHttpRequest,
    ) -> Result<
        sinomo_runtime::sandbox::SandboxHttpResponse,
        sinomo_runtime::sandbox::SandboxNetworkError,
    > {
        let rpc = network::ObjectRpcRequest::encode_body(
            network::SANDBOX_HTTP_FETCH_ROUTE,
            network::SANDBOX_RPC_CATEGORY,
            request,
        )
        .map_err(
            |source| sinomo_runtime::sandbox::SandboxNetworkError::Transport {
                detail: source.to_string(),
            },
        )?;
        let timeout = std::time::Duration::from_millis(request.timeout_ms.saturating_add(1_000));
        let response = sinomo_runtime::daemon::send_daemon_rpc(&self.address, &rpc, timeout)
            .map_err(
                |source| sinomo_runtime::sandbox::SandboxNetworkError::Transport {
                    detail: source.to_string(),
                },
            )?;
        response.decode_body().map_err(|source| {
            sinomo_runtime::sandbox::SandboxNetworkError::Transport {
                detail: source.to_string(),
            }
        })
    }
}

fn print_sandbox_command_record_summary(record: &sinomo_runtime::sandbox::SandboxCommandRecord) {
    println!("command: {}", record.command_id);
    println!(
        "status: {}",
        sandbox_command_record_status_label(&record.status)
    );
    println!("workspace: {}", record.workspace_root);
    println!("toolchain: {}", record.toolchain_id);
    println!("cwd: {}", record.cwd);
    println!("argv: {}", record.argv.join(" "));
    if !record.env.is_empty() {
        let env_keys = record.env.keys().cloned().collect::<Vec<_>>().join(", ");
        println!("env keys: {env_keys}");
    }
    if let Some(response) = &record.response {
        println!("exit status: {}", response.exit_status);
        println!("stdout: {}", response.stdout_ref);
        println!("stderr: {}", response.stderr_ref);
        println!("written bytes: {}", response.resource_usage.written_bytes);
        if !response.changed_paths.is_empty() {
            println!("changed paths:");
            for path in &response.changed_paths {
                println!("  {path}");
            }
        }
    }
    if let Some(error) = &record.error {
        println!("error: {error}");
    }
    println!("--- stdout ---");
}

fn sandbox_command_record_status_label(
    status: &sinomo_runtime::sandbox::SandboxCommandRecordStatus,
) -> &'static str {
    match status {
        sinomo_runtime::sandbox::SandboxCommandRecordStatus::Completed => "completed",
        sinomo_runtime::sandbox::SandboxCommandRecordStatus::Failed => "failed",
        sinomo_runtime::sandbox::SandboxCommandRecordStatus::Cancelled => "cancelled",
        sinomo_runtime::sandbox::SandboxCommandRecordStatus::ResourceExhausted => {
            "resource-exhausted"
        }
    }
}

pub(crate) fn sandbox_checkpoint_with_store(
    options: SandboxCheckpointOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let checkpoint = store.create_checkpoint(&options.sandbox_id, OffsetDateTime::now_utc())?;
    println!("checkpoint: {}", checkpoint.checkpoint_id);
    println!("sandbox: {}", checkpoint.sandbox_id);
    println!("manifest: {}", checkpoint.manifest_ref);
    Ok(())
}

pub(crate) fn sandbox_checkpoint_with_daemon(
    options: SandboxCheckpointOptions,
) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon checkpoint requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxCheckpointRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_CHECKPOINT_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        sandbox_daemon_large_metadata_rpc_timeout(),
    )?;
    let response: sinomo_runtime::sandbox::SandboxCheckpointResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("checkpoint: {}", response.checkpoint.checkpoint_id);
    println!("sandbox: {}", response.checkpoint.sandbox_id);
    println!("manifest: {}", response.checkpoint.manifest_ref);
    Ok(())
}

pub(crate) fn sandbox_resume_with_store(
    options: SandboxResumeOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (manifest, _layout) = store.resume_checkpoint(
        &options.sandbox_id,
        &options.checkpoint_id,
        OffsetDateTime::now_utc(),
    )?;
    println!("sandbox resumed: {}", manifest.sandbox_id);
    println!("checkpoint: {}", options.checkpoint_id);
    println!("workspace: {}", manifest.workspace_root);
    Ok(())
}

pub(crate) fn sandbox_resume_with_daemon(options: SandboxResumeOptions) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon resume requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxResumeRequest {
        sandbox_id: options.sandbox_id,
        checkpoint_id: options.checkpoint_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_RESUME_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        sandbox_daemon_large_metadata_rpc_timeout(),
    )?;
    let response: sinomo_runtime::sandbox::SandboxResumeResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("sandbox resumed: {}", response.manifest.sandbox_id);
    println!("checkpoint: {}", request.checkpoint_id);
    println!("workspace: {}", response.manifest.workspace_root);
    Ok(())
}

pub(crate) fn sandbox_reboot_with_store(
    options: SandboxRebootOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    let (manifest, _layout, cleared_tmp) =
        store.reboot_session(&options.sandbox_id, OffsetDateTime::now_utc())?;
    println!("sandbox rebooted: {}", manifest.sandbox_id);
    println!("workspace: {}", manifest.workspace_root);
    println!("tmp cleared: {cleared_tmp}");
    Ok(())
}

pub(crate) fn sandbox_reboot_with_daemon(options: SandboxRebootOptions) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon reboot requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxRebootRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_REBOOT_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(30),
    )?;
    let response: sinomo_runtime::sandbox::SandboxRebootResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("sandbox rebooted: {}", response.manifest.sandbox_id);
    println!("workspace: {}", response.manifest.workspace_root);
    println!("tmp cleared: {}", response.cleared_tmp);
    Ok(())
}

pub(crate) fn sandbox_destroy_with_store(
    options: SandboxDestroyOptions,
    store: &sinomo_runtime::sandbox::LocalSandboxInstanceStore,
) -> Result<(), CliError> {
    store.destroy(&options.sandbox_id)?;
    println!("sandbox destroyed: {}", options.sandbox_id);
    Ok(())
}

pub(crate) fn sandbox_destroy_with_daemon(options: SandboxDestroyOptions) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Err(CliError::DaemonNotRunning {
            detail: "sandbox daemon destroy requires the local daemon".to_string(),
        });
    };
    let request = sinomo_runtime::sandbox::SandboxDestroyRequest {
        sandbox_id: options.sandbox_id,
    };
    let rpc = network::ObjectRpcRequest::encode_body(
        network::SANDBOX_DESTROY_ROUTE,
        network::SANDBOX_RPC_CATEGORY,
        &request,
    )
    .map_err(CliError::from)?;
    let response = sinomo_runtime::daemon::send_daemon_rpc(
        &status.address,
        &rpc,
        std::time::Duration::from_secs(30),
    )?;
    let response: sinomo_runtime::sandbox::SandboxDestroyResponse =
        response.decode_body().map_err(CliError::from)?;
    println!("sandbox destroyed: {}", response.sandbox_id);
    Ok(())
}

pub(crate) fn sandbox_store_from_data_dir(
    data_dir: &SinomoDataDir,
) -> sinomo_runtime::sandbox::LocalSandboxInstanceStore {
    sinomo_runtime::sandbox::LocalSandboxInstanceStore::new(
        data_dir.root().join("sandbox").join("runtime"),
    )
}

pub(crate) fn sandbox_toolchain_store_from_data_dir(
    data_dir: &SinomoDataDir,
) -> sinomo_runtime::sandbox::LocalSandboxToolchainStore {
    sinomo_runtime::sandbox::LocalSandboxToolchainStore::new(
        data_dir.root().join("sandbox").join("cache"),
    )
}

fn parse_sandbox_id(value: &str) -> Result<sinomo_runtime::sandbox::SandboxId, CliError> {
    sinomo_runtime::sandbox::SandboxId::new(value.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid sandbox id `{value}`: {source}"),
    })
}

fn parse_sandbox_command_id(
    value: &str,
) -> Result<sinomo_runtime::sandbox::SandboxCommandId, CliError> {
    sinomo_runtime::sandbox::SandboxCommandId::new(value.to_string()).map_err(|source| {
        CliError::Parse {
            detail: format!("invalid sandbox command id `{value}`: {source}"),
        }
    })
}

fn parse_sandbox_checkpoint_id(
    value: &str,
) -> Result<sinomo_runtime::sandbox::SandboxCheckpointId, CliError> {
    sinomo_runtime::sandbox::SandboxCheckpointId::new(value.to_string()).map_err(|source| {
        CliError::Parse {
            detail: format!("invalid sandbox checkpoint id `{value}`: {source}"),
        }
    })
}

fn parse_sandbox_path(value: &str) -> Result<sinomo_runtime::sandbox::SandboxPath, CliError> {
    sinomo_runtime::sandbox::SandboxPath::new(value.to_string()).map_err(CliError::from)
}

fn parse_sandbox_env_assignment(value: &str) -> Result<(String, String), CliError> {
    let Some((key, env_value)) = value.split_once('=') else {
        return Err(CliError::Parse {
            detail: format!("sandbox env assignment `{value}` must use NAME=VALUE"),
        });
    };
    validate_sandbox_env_name(key)?;
    validate_sandbox_env_value(key, env_value)?;
    Ok((key.to_string(), env_value.to_string()))
}

fn validate_sandbox_env_name(value: &str) -> Result<(), CliError> {
    if !sinomo_runtime::sandbox::sandbox_env_name_is_valid(value) {
        return Err(CliError::Parse {
            detail: format!("invalid sandbox env name `{value}`"),
        });
    }
    Ok(())
}

fn validate_sandbox_env_value(name: &str, value: &str) -> Result<(), CliError> {
    if value.contains('\0') {
        return Err(CliError::Parse {
            detail: format!("sandbox env `{name}` contains an embedded NUL byte"),
        });
    }
    Ok(())
}

fn validate_sandbox_command_capability(value: &str, context: &str) -> Result<(), CliError> {
    if value.trim().is_empty() {
        return Err(CliError::Parse {
            detail: format!("{context} must be non-empty"),
        });
    }
    if value.contains('\0') {
        return Err(CliError::Parse {
            detail: format!("{context} `{value}` contains an embedded NUL byte"),
        });
    }
    Ok(())
}

fn parse_sandbox_mount(value: &str) -> Result<sinomo_runtime::sandbox::SandboxMount, CliError> {
    let parts: Vec<&str> = value.split(':').collect();
    match parts.as_slice() {
        [source_sifs_path, mount_path] => Ok(sinomo_runtime::sandbox::SandboxMount {
            source_sifs_path: parse_sifs_mount_prefix(source_sifs_path)?,
            mount_path: parse_sandbox_path(mount_path)?,
            mode: sinomo_runtime::sandbox::SandboxMountMode::ReadWrite,
        }),
        [source_sifs_path, mount_path, mode] => Ok(sinomo_runtime::sandbox::SandboxMount {
            source_sifs_path: parse_sifs_mount_prefix(source_sifs_path)?,
            mount_path: parse_sandbox_path(mount_path)?,
            mode: parse_sandbox_mount_mode(mode)?,
        }),
        _ => Err(CliError::Parse {
            detail:
                "sandbox mount must use <sifs-path>:<mount-path> or <sifs-path>:<mount-path>:ro|rw"
                    .to_string(),
        }),
    }
}

fn parse_sifs_mount_prefix(value: &str) -> Result<String, CliError> {
    let trimmed = value.trim();
    if !trimmed.starts_with('/')
        || trimmed.contains('\0')
        || trimmed.contains("//")
        || trimmed.contains("/../")
        || trimmed.ends_with("/..")
    {
        return Err(CliError::Parse {
            detail: format!("invalid SIFS path `{value}`"),
        });
    }
    if trimmed == "/" {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", trimmed.trim_matches('/')))
    }
}

fn parse_http_host(value: &str) -> Result<String, CliError> {
    let host = value.trim().to_ascii_lowercase();
    if host.is_empty()
        || host.contains('\0')
        || host.contains('/')
        || host.contains(':')
        || host.contains('@')
        || host.chars().any(char::is_whitespace)
    {
        return Err(CliError::Parse {
            detail: format!("invalid HTTP host `{value}`"),
        });
    }
    Ok(host)
}

fn parse_http_header(value: &str) -> Result<(String, String), CliError> {
    let Some((name, header_value)) = value.split_once(':') else {
        return Err(CliError::Parse {
            detail: format!("invalid HTTP header `{value}`; expected Name: value"),
        });
    };
    let name = name.trim();
    if name.is_empty()
        || name.contains('/')
        || name.contains('@')
        || name.chars().any(char::is_whitespace)
        || name.chars().any(|ch| ch.is_control())
    {
        return Err(CliError::Parse {
            detail: format!("invalid HTTP header name `{name}`"),
        });
    }
    Ok((name.to_string(), header_value.trim().to_string()))
}

fn parse_sandbox_mount_mode(
    value: &str,
) -> Result<sinomo_runtime::sandbox::SandboxMountMode, CliError> {
    match value {
        "ro" | "read-only" => Ok(sinomo_runtime::sandbox::SandboxMountMode::ReadOnly),
        "rw" | "read-write" => Ok(sinomo_runtime::sandbox::SandboxMountMode::ReadWrite),
        _ => Err(CliError::Parse {
            detail: format!("invalid sandbox mount mode `{value}`; expected ro or rw"),
        }),
    }
}

fn parse_toolchain_id(
    value: &str,
) -> Result<sinomo_runtime::sandbox::SandboxToolchainId, CliError> {
    sinomo_runtime::sandbox::SandboxToolchainId::new(value.to_string()).or_else(|new_error| {
        sinomo_runtime::sandbox::SandboxToolchainId::from_legacy_named(value).map_err(
            |legacy_error| CliError::Parse {
                detail: format!(
                    "invalid sandbox toolchain id `{value}`: {new_error}; legacy name parse failed: {legacy_error}"
                ),
            },
        )
    })
}

fn parse_sandbox_temp_policy(
    value: &str,
) -> Result<sinomo_runtime::sandbox::SandboxTempPolicy, CliError> {
    match value {
        "clear" | "clear-on-session-start" => {
            Ok(sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart)
        }
        "retain" | "retain-across-sessions" => {
            Ok(sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions)
        }
        _ => Err(CliError::Parse {
            detail: format!("invalid sandbox tmp policy `{value}`; expected clear or retain"),
        }),
    }
}

fn sandbox_temp_policy_label(policy: &sinomo_runtime::sandbox::SandboxTempPolicy) -> &'static str {
    match policy {
        sinomo_runtime::sandbox::SandboxTempPolicy::ClearOnSessionStart => "clear-on-session-start",
        sinomo_runtime::sandbox::SandboxTempPolicy::RetainAcrossSessions => {
            "retain-across-sessions"
        }
    }
}

fn sandbox_mount_mode_label(mode: &sinomo_runtime::sandbox::SandboxMountMode) -> &'static str {
    match mode {
        sinomo_runtime::sandbox::SandboxMountMode::ReadOnly => "ro",
        sinomo_runtime::sandbox::SandboxMountMode::ReadWrite => "rw",
    }
}
