use crate::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_cli_registry::{
    CliAutocompleteChoice, CliAutocompleteOption, CliAutocompleteSpec, CliAutocompleteValue,
    CliCommand, CliCommandContext, CliCommandError, CliCommandHandler, CliCommandHelp,
    CliCommandMetadata, CliCommandPolicy, CliParseError,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::node::{LocalNodePermissionStore, LocalNodePermissions};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const REMOTE_CLI_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();
const NODE_PERMISSIONS_CLI_POLICY: CliCommandPolicy = CliCommandPolicy::local_capable();
const TRANSFER_WRITE_ATTEMPTS: usize = 3;
static TRANSFER_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Target-aware object-RPC function injected by the CLI composition root.
///
/// `None` selects the receiving/local node. A concrete node ID asks the
/// composition root to route the request to that node.
pub type RemoteManagementCliRpc =
    Arc<dyn Fn(Option<&str>, ObjectRpcRequest) -> Result<ObjectRpcResponse, String> + Send + Sync>;

#[derive(Clone)]
pub struct RemoteManagementCliCommandHandler {
    rpc: RemoteManagementCliRpc,
}

impl fmt::Debug for RemoteManagementCliCommandHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteManagementCliCommandHandler")
            .finish_non_exhaustive()
    }
}

/// Constructs the registered `remote` command around a target-aware RPC
/// function. The handler contains no direct host-management fallback.
pub fn remote_cli_command_handler(
    rpc: impl Fn(Option<&str>, ObjectRpcRequest) -> Result<ObjectRpcResponse, String>
    + Send
    + Sync
    + 'static,
) -> RemoteManagementCliCommandHandler {
    RemoteManagementCliCommandHandler { rpc: Arc::new(rpc) }
}

/// Compatibility spelling for [`remote_cli_command_handler`].
pub fn remote_cli_command_handler_with_rpc(
    rpc: impl Fn(Option<&str>, ObjectRpcRequest) -> Result<ObjectRpcResponse, String>
    + Send
    + Sync
    + 'static,
) -> RemoteManagementCliCommandHandler {
    remote_cli_command_handler(rpc)
}

impl CliCommandHandler for RemoteManagementCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        remote_cli_metadata()
    }

    fn help(&self) -> CliCommandHelp {
        remote_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        remote_cli_autocomplete()
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(RemoteManagementCliCommandRunner {
            command: parse_remote_command(args)?,
            rpc: Arc::clone(&self.rpc),
        }))
    }
}

struct RemoteManagementCliCommandRunner {
    command: RemoteCliCommand,
    rpc: RemoteManagementCliRpc,
}

impl CliCommand for RemoteManagementCliCommandRunner {
    fn name(&self) -> &'static str {
        "remote"
    }

    fn policy(&self) -> CliCommandPolicy {
        REMOTE_CLI_POLICY
    }

    fn is_help(&self) -> bool {
        matches!(self.command, RemoteCliCommand::Help)
    }

    fn help(&self) -> CliCommandHelp {
        remote_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        remote_cli_autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        let Self { command, rpc } = *self;
        run_remote_command(context, &rpc, command)
    }
}

#[derive(Clone, Debug)]
pub struct NodePermissionsCliCommandHandler {
    data_dir: SinomoDataDir,
}

/// Constructs the strictly local `node permissions` command. The captured
/// data directory is the only authority source; this handler has no RPC field.
pub fn node_permissions_cli_command_handler(
    data_dir: SinomoDataDir,
) -> NodePermissionsCliCommandHandler {
    NodePermissionsCliCommandHandler { data_dir }
}

impl CliCommandHandler for NodePermissionsCliCommandHandler {
    fn metadata(&self) -> CliCommandMetadata {
        node_permissions_cli_metadata()
    }

    fn help(&self) -> CliCommandHelp {
        node_permissions_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        node_permissions_cli_autocomplete()
    }

    fn parse(&self, args: &[String]) -> Result<Box<dyn CliCommand>, CliParseError> {
        Ok(Box::new(NodePermissionsCliCommandRunner {
            command: parse_node_permissions_command(args)?,
            data_dir: self.data_dir.clone(),
        }))
    }
}

struct NodePermissionsCliCommandRunner {
    command: NodePermissionsCliCommand,
    data_dir: SinomoDataDir,
}

impl CliCommand for NodePermissionsCliCommandRunner {
    fn name(&self) -> &'static str {
        "node"
    }

    fn policy(&self) -> CliCommandPolicy {
        NODE_PERMISSIONS_CLI_POLICY
    }

    fn is_help(&self) -> bool {
        matches!(self.command, NodePermissionsCliCommand::Help)
    }

    fn help(&self) -> CliCommandHelp {
        node_permissions_cli_help()
    }

    fn autocomplete(&self) -> CliAutocompleteSpec {
        node_permissions_cli_autocomplete()
    }

    fn run(self: Box<Self>, context: &mut CliCommandContext<'_>) -> Result<(), CliCommandError> {
        let Self { command, data_dir } = *self;
        run_node_permissions_command(context, &data_dir, command)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteCallOptions {
    target: Option<String>,
    json: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoteCliCommand {
    Help,
    Info(RemoteCallOptions),
    Observe(RemoteObserveOptions),
    Shell(RemoteShellOptions),
    List(RemoteListOptions),
    Stat(RemoteStatOptions),
    Get(RemoteGetOptions),
    Put(RemotePutOptions),
    Config(RemoteConfigCommand),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteObserveOptions {
    common: RemoteCallOptions,
    include_processes: bool,
    max_processes: Option<usize>,
    include_disks: bool,
    include_network: bool,
    include_devices: bool,
    include_resource_exposures: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteShellOptions {
    common: RemoteCallOptions,
    command: String,
    cwd: Option<String>,
    shell: Option<String>,
    timeout_ms: Option<u64>,
    max_output_bytes: Option<usize>,
    env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteListOptions {
    common: RemoteCallOptions,
    path: String,
    recursive: bool,
    max_entries: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteStatOptions {
    common: RemoteCallOptions,
    path: String,
    include_blake3: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemoteGetOptions {
    common: RemoteCallOptions,
    remote_path: String,
    local_path: PathBuf,
    create_parent: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RemotePutOptions {
    common: RemoteCallOptions,
    local_path: PathBuf,
    remote_path: String,
    create_parent: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoteConfigCommand {
    Help,
    Catalog {
        common: RemoteCallOptions,
        owner: Option<String>,
    },
    Search {
        common: RemoteCallOptions,
        query: String,
    },
    Get {
        common: RemoteCallOptions,
        key: String,
    },
    Set {
        common: RemoteCallOptions,
        key: String,
        value: serde_json::Value,
        write_target: ConfigWriteTarget,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum NodePermissionsCliCommand {
    Help,
    Show {
        json: bool,
    },
    AllowRemoteManagement {
        json: bool,
    },
    DenyRemoteManagement {
        json: bool,
    },
    Expose {
        resource: String,
        operation: String,
        json: bool,
    },
    Hide {
        resource: String,
        operation: Option<String>,
        json: bool,
    },
}

fn remote_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "remote",
        &[],
        "Manage a node through the permission-gated remote-management service.",
        REMOTE_CLI_POLICY,
    )
}

fn remote_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Remote management:",
        [
            "  sinomo remote info [--target <node>] [--json]",
            "  sinomo remote observe [--processes] [--max-processes <n>] [--disks] [--network] [--devices] [--resources] [--target <node>] [--json]",
            "  sinomo remote shell [--cwd <path>] [--shell <path>] [--timeout-ms <ms>] [--max-output-bytes <n>] [--env <key=value>] [--target <node>] [--json] -- <quoted-command-string>",
            "  sinomo remote ls <remote-path> [--recursive] [--max-entries <n>] [--target <node>] [--json]",
            "  sinomo remote stat <remote-path> [--blake3] [--target <node>] [--json]",
            "  sinomo remote get <remote-path> <local-path> [--create-parent] [--target <node>] [--json]",
            "  sinomo remote put <local-path> <remote-path> [--create-parent] [--target <node>] [--json]",
            "  sinomo remote config catalog [--owner <owner>] [--target <node>] [--json]",
            "  sinomo remote config search <query> [--target <node>] [--json]",
            "  sinomo remote config get <key> [--target <node>] [--json]",
            "  sinomo remote config set <key> <json-value> [--write-target <auto|local|cluster>] [--target <node>] [--json]",
            "",
            "Remote management must first be enabled manually with `sinomo node permissions allow remote-management` on the receiving node.",
            "Quote the shell command using your local shell's syntax so it is one argument after `--`; Sinomo preserves that command string exactly.",
        ],
    )
}

fn common_remote_autocomplete_options() -> [CliAutocompleteOption; 2] {
    [
        CliAutocompleteOption::with_value(
            "--target",
            "target node id",
            CliAutocompleteValue::new("node", "target node id", []),
        ),
        CliAutocompleteOption::flag("--json", "emit JSON"),
    ]
}

fn remote_cli_autocomplete() -> CliAutocompleteSpec {
    let common = common_remote_autocomplete_options();
    CliAutocompleteSpec::new("remote", "permission-gated remote node management").with_children([
        CliAutocompleteSpec::new("info", "read system information").with_options(common.clone()),
        CliAutocompleteSpec::new("observe", "read bounded system observations").with_options([
            common[0].clone(),
            common[1].clone(),
            CliAutocompleteOption::flag("--processes", "include bounded process information"),
            CliAutocompleteOption::with_value(
                "--max-processes",
                "maximum process records",
                CliAutocompleteValue::new("n", "process limit", []),
            ),
            CliAutocompleteOption::flag("--disks", "include disk information"),
            CliAutocompleteOption::flag("--network", "include network information"),
            CliAutocompleteOption::flag("--devices", "include verified device inventory"),
            CliAutocompleteOption::flag(
                "--resources",
                "include node-local resource exposure policies (not device inventory)",
            ),
        ]),
        CliAutocompleteSpec::new("shell", "run one bounded shell command").with_options([
            common[0].clone(),
            common[1].clone(),
            CliAutocompleteOption::with_value(
                "--cwd",
                "remote working directory",
                CliAutocompleteValue::new("path", "remote directory", []),
            ),
            CliAutocompleteOption::with_value(
                "--shell",
                "remote shell executable",
                CliAutocompleteValue::new("path", "shell executable", []),
            ),
            CliAutocompleteOption::with_value(
                "--timeout-ms",
                "bounded command timeout",
                CliAutocompleteValue::new("ms", "timeout milliseconds", []),
            ),
            CliAutocompleteOption::with_value(
                "--max-output-bytes",
                "bounded output bytes",
                CliAutocompleteValue::new("n", "output byte limit", []),
            ),
            CliAutocompleteOption::with_value(
                "--env",
                "environment assignment",
                CliAutocompleteValue::new("key=value", "environment assignment", []),
            ),
        ]),
        CliAutocompleteSpec::new("ls", "list a remote directory").with_options([
            common[0].clone(),
            common[1].clone(),
            CliAutocompleteOption::flag("--recursive", "list recursively"),
            CliAutocompleteOption::with_value(
                "--max-entries",
                "maximum returned entries",
                CliAutocompleteValue::new("n", "entry limit", []),
            ),
        ]),
        CliAutocompleteSpec::new("stat", "inspect a remote path").with_options([
            common[0].clone(),
            common[1].clone(),
            CliAutocompleteOption::flag("--blake3", "include a BLAKE3 digest"),
        ]),
        CliAutocompleteSpec::new("get", "download and verify a remote file").with_options([
            common[0].clone(),
            common[1].clone(),
            CliAutocompleteOption::flag("--create-parent", "create local parent directories"),
        ]),
        CliAutocompleteSpec::new("put", "upload and verify a local file").with_options([
            common[0].clone(),
            common[1].clone(),
            CliAutocompleteOption::flag("--create-parent", "create remote parent directories"),
        ]),
        CliAutocompleteSpec::new("config", "read or write node configuration").with_children([
            CliAutocompleteSpec::new("catalog", "list configuration definitions").with_options([
                common[0].clone(),
                common[1].clone(),
                CliAutocompleteOption::with_value(
                    "--owner",
                    "filter by owner",
                    CliAutocompleteValue::new("owner", "configuration owner", []),
                ),
            ]),
            CliAutocompleteSpec::new("search", "search configuration definitions")
                .with_options(common.clone()),
            CliAutocompleteSpec::new("get", "read a configuration value")
                .with_options(common.clone()),
            CliAutocompleteSpec::new("set", "write a configuration value").with_options([
                common[0].clone(),
                common[1].clone(),
                CliAutocompleteOption::with_value(
                    "--write-target",
                    "configuration write target",
                    CliAutocompleteValue::new(
                        "target",
                        "configuration write target",
                        [
                            CliAutocompleteChoice::new("auto", "resolve from config policy"),
                            CliAutocompleteChoice::new("local", "write node-local config"),
                            CliAutocompleteChoice::new("cluster", "write cluster config"),
                        ],
                    ),
                ),
            ]),
        ]),
    ])
}

fn node_permissions_cli_metadata() -> CliCommandMetadata {
    CliCommandMetadata::new(
        "node",
        &[],
        "Edit receiving-node permissions locally.",
        NODE_PERMISSIONS_CLI_POLICY,
    )
}

fn node_permissions_cli_help() -> CliCommandHelp {
    CliCommandHelp::new(
        "Node permissions (local only):",
        [
            "  sinomo node permissions show [--json]",
            "  sinomo node permissions allow remote-management [--json]",
            "  sinomo node permissions deny remote-management [--json]",
            "  sinomo node permissions expose <resource> <operation> [--json]",
            "  sinomo node permissions hide <resource> [operation] [--json]",
            "",
            "No supported permission-management RPC or --target exists; run this command on the receiving node.",
            "An authorized full shell is host-equivalent and must never be used to bypass this boundary.",
        ],
    )
}

fn node_permissions_cli_autocomplete() -> CliAutocompleteSpec {
    let json = CliAutocompleteOption::flag("--json", "emit JSON");
    CliAutocompleteSpec::new("node", "manage receiving-node local state").with_children([
        CliAutocompleteSpec::new("permissions", "edit local permission grants").with_children([
            CliAutocompleteSpec::new("show", "show local permissions").with_options([json.clone()]),
            CliAutocompleteSpec::new("allow", "allow a local permission")
                .with_values([CliAutocompleteValue::new(
                    "permission",
                    "local permission",
                    [CliAutocompleteChoice::new(
                        "remote-management",
                        "allow remote management and unbounded host-control tools",
                    )],
                )])
                .with_options([json.clone()]),
            CliAutocompleteSpec::new("deny", "deny a local permission")
                .with_values([CliAutocompleteValue::new(
                    "permission",
                    "local permission",
                    [CliAutocompleteChoice::new(
                        "remote-management",
                        "deny remote management and unbounded host-control tools",
                    )],
                )])
                .with_options([json.clone()]),
            CliAutocompleteSpec::new("expose", "expose one local resource operation")
                .with_options([json.clone()]),
            CliAutocompleteSpec::new("hide", "hide a local resource or operation")
                .with_options([json]),
        ]),
    ])
}

fn parse_remote_command(args: &[String]) -> Result<RemoteCliCommand, CliParseError> {
    match args.split_first() {
        None => Ok(RemoteCliCommand::Help),
        Some((command, [])) if is_help(command) => Ok(RemoteCliCommand::Help),
        Some((command, rest)) if command == "info" => {
            Ok(RemoteCliCommand::Info(parse_common_only(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "observe" | "obs") => {
            Ok(RemoteCliCommand::Observe(parse_remote_observe(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "shell" | "sh") => {
            Ok(RemoteCliCommand::Shell(parse_remote_shell(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "ls" | "list") => {
            Ok(RemoteCliCommand::List(parse_remote_list(rest)?))
        }
        Some((command, rest)) if command == "stat" => {
            Ok(RemoteCliCommand::Stat(parse_remote_stat(rest)?))
        }
        Some((command, rest)) if command == "get" => {
            Ok(RemoteCliCommand::Get(parse_remote_get(rest)?))
        }
        Some((command, rest)) if command == "put" => {
            Ok(RemoteCliCommand::Put(parse_remote_put(rest)?))
        }
        Some((command, rest)) if command == "config" => {
            Ok(RemoteCliCommand::Config(parse_remote_config(rest)?))
        }
        _ => Err(CliParseError::Usage),
    }
}

fn parse_common_only(args: &[String]) -> Result<RemoteCallOptions, CliParseError> {
    let mut common = RemoteCallOptions::default();
    let mut index = 0;
    while index < args.len() {
        if !parse_common_option(args, &mut index, &mut common)? {
            return Err(CliParseError::Usage);
        }
        index += 1;
    }
    Ok(common)
}

fn parse_remote_observe(args: &[String]) -> Result<RemoteObserveOptions, CliParseError> {
    let mut options = RemoteObserveOptions::default();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut options.common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--processes" => options.include_processes = true,
            "--max-processes" => {
                options.max_processes =
                    Some(parse_usize_option(args, &mut index, "--max-processes")?);
                options.include_processes = true;
            }
            "--disks" => options.include_disks = true,
            "--network" => options.include_network = true,
            "--devices" => options.include_devices = true,
            "--resources" => options.include_resource_exposures = true,
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_remote_shell(args: &[String]) -> Result<RemoteShellOptions, CliParseError> {
    let mut options = RemoteShellOptions::default();
    let mut command = None;
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--" {
            command = match &args[index + 1..] {
                [command] if !command.is_empty() => Some(command.clone()),
                [] => {
                    return parse_error(
                        "remote shell requires one quoted command string after `--`",
                    );
                }
                [_] => {
                    return parse_error("remote shell command string must not be empty");
                }
                _ => {
                    return parse_error(
                        "remote shell accepts exactly one command string after `--`; quote it using your local shell's syntax so it is passed as one argument",
                    );
                }
            };
            break;
        }
        if parse_common_option(args, &mut index, &mut options.common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--cwd" => options.cwd = Some(parse_string_option(args, &mut index, "--cwd")?),
            "--shell" => options.shell = Some(parse_string_option(args, &mut index, "--shell")?),
            "--timeout-ms" => {
                options.timeout_ms = Some(parse_u64_option(args, &mut index, "--timeout-ms")?)
            }
            "--max-output-bytes" => {
                options.max_output_bytes =
                    Some(parse_usize_option(args, &mut index, "--max-output-bytes")?)
            }
            "--env" => {
                let assignment = parse_string_option(args, &mut index, "--env")?;
                let Some((key, value)) = assignment.split_once('=') else {
                    return parse_error("--env must be KEY=VALUE");
                };
                if key.trim().is_empty() {
                    return parse_error("--env key must be non-empty");
                }
                options.env.insert(key.to_string(), value.to_string());
            }
            value if !value.starts_with('-') => {
                return parse_error(
                    "remote shell command must follow `--` as one quoted command string",
                );
            }
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    options.command = command.ok_or_else(|| CliParseError::Parse {
        detail: "remote shell requires one quoted command string after `--`".to_string(),
    })?;
    Ok(options)
}

fn parse_remote_list(args: &[String]) -> Result<RemoteListOptions, CliParseError> {
    let mut options = RemoteListOptions::default();
    let mut paths = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut options.common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--recursive" | "-r" => options.recursive = true,
            "--max-entries" => {
                options.max_entries = Some(parse_usize_option(args, &mut index, "--max-entries")?)
            }
            value if !value.starts_with('-') => paths.push(value.to_string()),
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if paths.len() != 1 {
        return Err(CliParseError::Usage);
    }
    options.path = paths.remove(0);
    Ok(options)
}

fn parse_remote_stat(args: &[String]) -> Result<RemoteStatOptions, CliParseError> {
    let mut options = RemoteStatOptions::default();
    let mut paths = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut options.common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--blake3" | "--hash" => options.include_blake3 = true,
            value if !value.starts_with('-') => paths.push(value.to_string()),
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if paths.len() != 1 {
        return Err(CliParseError::Usage);
    }
    options.path = paths.remove(0);
    Ok(options)
}

fn parse_remote_get(args: &[String]) -> Result<RemoteGetOptions, CliParseError> {
    let mut options = RemoteGetOptions::default();
    let mut paths = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut options.common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--create-parent" => options.create_parent = true,
            value if !value.starts_with('-') => paths.push(value.to_string()),
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if paths.len() != 2 {
        return Err(CliParseError::Usage);
    }
    options.remote_path = paths.remove(0);
    options.local_path = PathBuf::from(paths.remove(0));
    Ok(options)
}

fn parse_remote_put(args: &[String]) -> Result<RemotePutOptions, CliParseError> {
    let mut options = RemotePutOptions::default();
    let mut paths = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut options.common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--create-parent" => options.create_parent = true,
            value if !value.starts_with('-') => paths.push(value.to_string()),
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if paths.len() != 2 {
        return Err(CliParseError::Usage);
    }
    options.local_path = PathBuf::from(paths.remove(0));
    options.remote_path = paths.remove(0);
    Ok(options)
}

fn parse_remote_config(args: &[String]) -> Result<RemoteConfigCommand, CliParseError> {
    let Some((command, rest)) = args.split_first() else {
        return Ok(RemoteConfigCommand::Help);
    };
    if is_help(command) && rest.is_empty() {
        return Ok(RemoteConfigCommand::Help);
    }
    match command.as_str() {
        "catalog" | "list" => {
            let mut common = RemoteCallOptions::default();
            let mut owner = None;
            let mut index = 0;
            while index < rest.len() {
                if parse_common_option(rest, &mut index, &mut common)? {
                    index += 1;
                    continue;
                }
                match rest[index].as_str() {
                    "--owner" => owner = Some(parse_string_option(rest, &mut index, "--owner")?),
                    _ => return Err(CliParseError::Usage),
                }
                index += 1;
            }
            Ok(RemoteConfigCommand::Catalog { common, owner })
        }
        "search" => {
            let (common, values) = parse_common_and_positionals(rest)?;
            if values.len() != 1 {
                return Err(CliParseError::Usage);
            }
            Ok(RemoteConfigCommand::Search {
                common,
                query: values[0].clone(),
            })
        }
        "get" => {
            let (common, values) = parse_common_and_positionals(rest)?;
            if values.len() != 1 {
                return Err(CliParseError::Usage);
            }
            Ok(RemoteConfigCommand::Get {
                common,
                key: values[0].clone(),
            })
        }
        "set" => parse_remote_config_set(rest),
        _ => Err(CliParseError::Usage),
    }
}

fn parse_remote_config_set(args: &[String]) -> Result<RemoteConfigCommand, CliParseError> {
    let mut common = RemoteCallOptions::default();
    let mut write_target = ConfigWriteTarget::Auto;
    let mut positionals = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut common)? {
            index += 1;
            continue;
        }
        match args[index].as_str() {
            "--write-target" => {
                let value = parse_string_option(args, &mut index, "--write-target")?;
                write_target = match value.as_str() {
                    "auto" => ConfigWriteTarget::Auto,
                    "local" => ConfigWriteTarget::Local,
                    "cluster" => ConfigWriteTarget::Cluster,
                    _ => {
                        return parse_error(format!(
                            "invalid config write target `{value}`; expected auto, local, or cluster"
                        ));
                    }
                };
            }
            value if !value.starts_with('-') => positionals.push(value.to_string()),
            _ => return Err(CliParseError::Usage),
        }
        index += 1;
    }
    if positionals.len() != 2 {
        return Err(CliParseError::Usage);
    }
    let value = serde_json::from_str(&positionals[1]).map_err(|source| CliParseError::Parse {
        detail: format!("invalid JSON config value: {source}"),
    })?;
    Ok(RemoteConfigCommand::Set {
        common,
        key: positionals.remove(0),
        value,
        write_target,
    })
}

fn parse_common_and_positionals(
    args: &[String],
) -> Result<(RemoteCallOptions, Vec<String>), CliParseError> {
    let mut common = RemoteCallOptions::default();
    let mut values = Vec::new();
    let mut index = 0;
    while index < args.len() {
        if parse_common_option(args, &mut index, &mut common)? {
            index += 1;
            continue;
        }
        if args[index].starts_with('-') {
            return Err(CliParseError::Usage);
        }
        values.push(args[index].clone());
        index += 1;
    }
    Ok((common, values))
}

fn parse_node_permissions_command(
    args: &[String],
) -> Result<NodePermissionsCliCommand, CliParseError> {
    if args.iter().any(|argument| {
        matches!(argument.as_str(), "--target" | "--node")
            || argument.starts_with("--target=")
            || argument.starts_with("--node=")
    }) {
        return parse_error("node permissions are local-only; --target is not allowed");
    }
    let Some((namespace, rest)) = args.split_first() else {
        return Ok(NodePermissionsCliCommand::Help);
    };
    if is_help(namespace) && rest.is_empty() {
        return Ok(NodePermissionsCliCommand::Help);
    }
    if namespace != "permissions" && namespace != "permission" {
        return Err(CliParseError::Usage);
    }
    let Some((command, rest)) = rest.split_first() else {
        return Ok(NodePermissionsCliCommand::Help);
    };
    if is_help(command) && rest.is_empty() {
        return Ok(NodePermissionsCliCommand::Help);
    }
    let (json, values) = parse_local_json_and_values(rest)?;
    match command.as_str() {
        "show" if values.is_empty() => Ok(NodePermissionsCliCommand::Show { json }),
        "allow" if values == ["remote-management"] => {
            Ok(NodePermissionsCliCommand::AllowRemoteManagement { json })
        }
        "deny" if values == ["remote-management"] => {
            Ok(NodePermissionsCliCommand::DenyRemoteManagement { json })
        }
        "expose" if values.len() == 2 => Ok(NodePermissionsCliCommand::Expose {
            resource: values[0].clone(),
            operation: values[1].clone(),
            json,
        }),
        "hide" if matches!(values.len(), 1 | 2) => Ok(NodePermissionsCliCommand::Hide {
            resource: values[0].clone(),
            operation: values.get(1).cloned(),
            json,
        }),
        _ => Err(CliParseError::Usage),
    }
}

fn parse_local_json_and_values(args: &[String]) -> Result<(bool, Vec<String>), CliParseError> {
    let mut json = false;
    let mut values = Vec::new();
    for argument in args {
        match argument.as_str() {
            "--json" => json = true,
            value if !value.starts_with('-') => values.push(value.to_string()),
            _ => return Err(CliParseError::Usage),
        }
    }
    Ok((json, values))
}

fn parse_common_option(
    args: &[String],
    index: &mut usize,
    common: &mut RemoteCallOptions,
) -> Result<bool, CliParseError> {
    match args[*index].as_str() {
        "--target" | "--node" => {
            let option = args[*index].clone();
            let value = parse_string_option(args, index, &option)?;
            if common.target.replace(value).is_some() {
                return parse_error("--target may be specified only once");
            }
            Ok(true)
        }
        "--json" => {
            common.json = true;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn parse_string_option(
    args: &[String],
    index: &mut usize,
    option: &str,
) -> Result<String, CliParseError> {
    *index += 1;
    let Some(value) = args.get(*index) else {
        return parse_error(format!("{option} requires a value"));
    };
    if value.trim().is_empty() {
        return parse_error(format!("{option} requires a non-empty value"));
    }
    Ok(value.clone())
}

fn parse_usize_option(
    args: &[String],
    index: &mut usize,
    option: &str,
) -> Result<usize, CliParseError> {
    let value = parse_string_option(args, index, option)?;
    value.parse().map_err(|source| CliParseError::Parse {
        detail: format!("invalid {option} value `{value}`: {source}"),
    })
}

fn parse_u64_option(
    args: &[String],
    index: &mut usize,
    option: &str,
) -> Result<u64, CliParseError> {
    let value = parse_string_option(args, index, option)?;
    value.parse().map_err(|source| CliParseError::Parse {
        detail: format!("invalid {option} value `{value}`: {source}"),
    })
}

fn parse_error<T>(detail: impl Into<String>) -> Result<T, CliParseError> {
    Err(CliParseError::Parse {
        detail: detail.into(),
    })
}

fn is_help(value: &str) -> bool {
    matches!(value, "help" | "--help" | "-h")
}

fn run_remote_command(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    command: RemoteCliCommand,
) -> Result<(), CliCommandError> {
    match command {
        RemoteCliCommand::Help => remote_cli_help().write(context),
        RemoteCliCommand::Info(options) => run_remote_info(context, rpc, options),
        RemoteCliCommand::Observe(options) => run_remote_observe(context, rpc, options),
        RemoteCliCommand::Shell(options) => run_remote_shell(context, rpc, options),
        RemoteCliCommand::List(options) => run_remote_list(context, rpc, options),
        RemoteCliCommand::Stat(options) => run_remote_stat(context, rpc, options),
        RemoteCliCommand::Get(options) => run_remote_get(context, rpc, options),
        RemoteCliCommand::Put(options) => run_remote_put(context, rpc, options),
        RemoteCliCommand::Config(command) => run_remote_config(context, rpc, command),
    }
}

fn run_remote_info(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemoteCallOptions,
) -> Result<(), CliCommandError> {
    let response: RemoteManagementSystemInfoResponse = remote_rpc(
        rpc,
        options.target.as_deref(),
        REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
        REMOTE_MANAGEMENT_RPC_CATEGORY,
        &RemoteManagementSystemInfoRequest::new(),
    )?;
    let info = response.into_result().map_err(command_error)?;
    if options.json {
        return write_json(context, &info);
    }
    context.stdout_line(format_args!("os: {}", info.os))?;
    context.stdout_line(format_args!("arch: {}", info.arch))?;
    context.stdout_line(format_args!("family: {}", info.family))?;
    context.stdout_line(format_args!("shell: {}", info.default_shell))?;
    if let Some(current_dir) = info.current_dir {
        context.stdout_line(format_args!("current-dir: {current_dir}"))?;
    }
    if let Some(executable) = info.executable {
        context.stdout_line(format_args!("executable: {executable}"))?;
    }
    Ok(())
}

fn run_remote_observe(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemoteObserveOptions,
) -> Result<(), CliCommandError> {
    let mut request = RemoteManagementSystemObserveRequest::new()
        .including_processes(options.include_processes)
        .including_disks(options.include_disks)
        .including_network(options.include_network)
        .including_devices(options.include_devices)
        .including_resource_exposures(options.include_resource_exposures);
    if let Some(max_processes) = options.max_processes {
        request = request.with_max_processes(max_processes.min(MAX_OBSERVED_PROCESSES));
    }
    let response: RemoteManagementSystemObserveResponse = remote_rpc(
        rpc,
        options.common.target.as_deref(),
        REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
        REMOTE_MANAGEMENT_RPC_CATEGORY,
        &request,
    )?;
    let observation = response.into_result().map_err(command_error)?;
    if options.common.json {
        return write_json(context, &observation);
    }
    context.stdout_line(format_args!("observed-at: {}", observation.observed_at))?;
    if let Some(hostname) = observation.hostname {
        context.stdout_line(format_args!("hostname: {hostname}"))?;
    }
    if let Some(uptime) = observation.uptime_seconds {
        context.stdout_line(format_args!("uptime-seconds: {uptime}"))?;
    }
    if let Some(cpus) = observation.logical_cpu_count {
        context.stdout_line(format_args!("logical-cpus: {cpus}"))?;
    }
    if let Some(cpu) = observation.cpu_usage_percent {
        context.stdout_line(format_args!("cpu-usage-percent: {cpu:.2}"))?;
    }
    if let Some(load) = observation.load_average {
        context.stdout_line(format_args!(
            "load-average: {:.2} {:.2} {:.2}",
            load.one_minute, load.five_minutes, load.fifteen_minutes
        ))?;
    }
    if let Some(memory) = observation.memory {
        context.stdout_line(format_args!(
            "memory: available={} total={}",
            memory.available_bytes, memory.total_bytes
        ))?;
    }
    for disk in observation.disks {
        context.stdout_line(format_args!(
            "disk: {} mount={} available={} total={}",
            disk.name, disk.mount_point, disk.available_bytes, disk.total_bytes
        ))?;
    }
    for interface in observation.network_interfaces {
        context.stdout_line(format_args!(
            "network: {} addresses={}",
            interface.name,
            interface.addresses.join(",")
        ))?;
    }
    for process in observation.processes {
        context.stdout_line(format_args!(
            "process: pid={} name={} command={}",
            process.pid,
            process.name,
            process.command.join(" ")
        ))?;
    }
    if observation.processes_truncated {
        context.stdout_line(format_args!("processes-truncated: true"))?;
    }
    for device in observation.devices {
        context.stdout_line(format_args!(
            "device: {} kind={} name={} capabilities={}",
            device.id,
            device.kind,
            device.name,
            device.capabilities.join(",")
        ))?;
    }
    for policy in observation.resource_exposure_policies {
        context.stdout_line(format_args!(
            "resource-exposure-policy: {} allowed-operations={}",
            policy.resource_id,
            policy.allowed_operations.join(",")
        ))?;
    }
    Ok(())
}

fn run_remote_shell(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemoteShellOptions,
) -> Result<(), CliCommandError> {
    let mut request = RemoteManagementShellRunRequest::new(options.command);
    if let Some(cwd) = options.cwd {
        request = request.with_cwd(cwd);
    }
    if let Some(shell) = options.shell {
        request = request.with_shell(shell);
    }
    if let Some(timeout_ms) = options.timeout_ms {
        request = request.with_timeout_ms(timeout_ms.clamp(1, MAX_COMMAND_TIMEOUT_MS));
    }
    if let Some(max_output_bytes) = options.max_output_bytes {
        request = request.with_max_output_bytes(max_output_bytes.min(MAX_OUTPUT_BYTES));
    }
    for (key, value) in options.env {
        request = request.with_env(key, value);
    }
    request.validate().map_err(command_error)?;
    let response: RemoteManagementShellRunResponse = remote_rpc(
        rpc,
        options.common.target.as_deref(),
        REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
        REMOTE_MANAGEMENT_RPC_CATEGORY,
        &request,
    )?;
    let outcome = response.into_result().map_err(command_error)?;
    if options.common.json {
        write_json(context, &outcome)?;
    } else {
        write_stream(context.stdout(), outcome.stdout.as_bytes(), "stdout")?;
        write_stream(context.stderr(), outcome.stderr.as_bytes(), "stderr")?;
    }
    if outcome.succeeded() {
        Ok(())
    } else if outcome.timed_out {
        Err(CliCommandError::new(format!(
            "remote command timed out after {} ms",
            outcome.duration_ms
        )))
    } else {
        Err(CliCommandError::new(format!(
            "remote command exited with {}",
            outcome
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "no exit code".to_string())
        )))
    }
}

fn run_remote_list(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemoteListOptions,
) -> Result<(), CliCommandError> {
    let mut request =
        RemoteManagementFileListRequest::new(options.path).recursively(options.recursive);
    if let Some(max_entries) = options.max_entries {
        request = request.with_max_entries(max_entries.min(MAX_LIST_ENTRIES));
    }
    request.validate().map_err(command_error)?;
    let response: RemoteManagementFileListResponse = remote_rpc(
        rpc,
        options.common.target.as_deref(),
        REMOTE_MANAGEMENT_FS_LIST_ROUTE,
        REMOTE_MANAGEMENT_RPC_CATEGORY,
        &request,
    )?;
    if options.common.json {
        return write_json(context, &response);
    }
    let (entries, truncated) = response.into_result().map_err(command_error)?;
    for entry in entries {
        context.stdout_line(format_args!(
            "{:?}\t{}\t{}{}",
            entry.file_type,
            entry
                .len
                .map(|len| len.to_string())
                .unwrap_or_else(|| "-".to_string()),
            entry.path,
            if entry.readonly { "\treadonly" } else { "" }
        ))?;
    }
    if truncated {
        context.stderr_line(format_args!("remote listing was truncated"))?;
    }
    Ok(())
}

fn run_remote_stat(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemoteStatOptions,
) -> Result<(), CliCommandError> {
    let stat = fetch_remote_stat(
        rpc,
        options.common.target.as_deref(),
        &options.path,
        options.include_blake3,
    )?;
    if options.common.json {
        return write_json(context, &stat);
    }
    context.stdout_line(format_args!("path: {}", stat.path))?;
    context.stdout_line(format_args!("type: {:?}", stat.file_type))?;
    if let Some(len) = stat.len {
        context.stdout_line(format_args!("length: {len}"))?;
    }
    context.stdout_line(format_args!("readonly: {}", stat.readonly))?;
    if let Some(modified) = stat.modified_unix_ms {
        context.stdout_line(format_args!("modified-unix-ms: {modified}"))?;
    }
    if let Some(hash) = stat.blake3 {
        context.stdout_line(format_args!("blake3: {hash}"))?;
    }
    Ok(())
}

fn run_remote_config(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    command: RemoteConfigCommand,
) -> Result<(), CliCommandError> {
    match command {
        RemoteConfigCommand::Help => remote_cli_help().write(context),
        RemoteConfigCommand::Catalog { common, owner } => {
            let response: ConfigCatalogResponse = remote_rpc(
                rpc,
                common.target.as_deref(),
                CONFIG_CATALOG_ROUTE,
                CONFIG_RPC_CATEGORY,
                &ConfigCatalogRequest { owner },
            )?;
            print_config_definitions(context, response.definitions, common.json)
        }
        RemoteConfigCommand::Search { common, query } => {
            let response: ConfigSearchResponse = remote_rpc(
                rpc,
                common.target.as_deref(),
                CONFIG_SEARCH_ROUTE,
                CONFIG_RPC_CATEGORY,
                &ConfigSearchRequest { query },
            )?;
            print_config_definitions(context, response.definitions, common.json)
        }
        RemoteConfigCommand::Get { common, key } => {
            let response: ConfigGetResponse = remote_rpc(
                rpc,
                common.target.as_deref(),
                CONFIG_GET_ROUTE,
                CONFIG_RPC_CATEGORY,
                &ConfigGetRequest { key },
            )?;
            if common.json {
                return write_json(context, &response.resolution);
            }
            context.stdout_line(format_args!(
                "{} = {}",
                response.resolution.key, response.resolution.value
            ))?;
            context.stdout_line(format_args!("source: {:?}", response.resolution.source))
        }
        RemoteConfigCommand::Set {
            common,
            key,
            value,
            write_target,
        } => {
            let response: ConfigSetResponse = remote_rpc(
                rpc,
                common.target.as_deref(),
                CONFIG_SET_ROUTE,
                CONFIG_RPC_CATEGORY,
                &ConfigSetRequest {
                    key,
                    value,
                    target: write_target,
                },
            )?;
            if common.json {
                return write_json(context, &response.result);
            }
            context.stdout_line(format_args!(
                "{} = {}",
                response.result.key, response.result.value
            ))?;
            context.stdout_line(format_args!("source: {:?}", response.result.source))
        }
    }
}

fn print_config_definitions(
    context: &mut CliCommandContext<'_>,
    definitions: Vec<ConfigDefinition>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        return write_json(context, &definitions);
    }
    for definition in definitions {
        context.stdout_line(format_args!(
            "{}\t{}\t{}{}",
            definition.key,
            definition.owner,
            definition.description,
            if definition.read_only {
                "\tread-only"
            } else {
                ""
            }
        ))?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Eq, PartialEq)]
struct RemoteTransferReport {
    direction: &'static str,
    source: String,
    destination: String,
    bytes: u64,
    blake3: String,
    chunks: usize,
}

fn run_remote_get(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemoteGetOptions,
) -> Result<(), CliCommandError> {
    let target = options.common.target.as_deref();
    let initial_stat = fetch_remote_stat(rpc, target, &options.remote_path, true)?;
    let (expected_len, expected_hash) = verified_file_identity(&initial_stat)?;

    if options.create_parent
        && let Some(parent) = options.local_path.parent()
    {
        fs::create_dir_all(parent).map_err(|source| {
            CliCommandError::new(format!(
                "failed to create local directory {}: {source}",
                parent.display()
            ))
        })?;
    }
    let temp_path = TransferTempPath::new(&options.local_path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path.path())
        .map_err(|source| {
            CliCommandError::new(format!(
                "failed to create transfer file {}: {source}",
                temp_path.path().display()
            ))
        })?;
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    let mut chunks = 0_usize;
    while offset < expected_len {
        let request = RemoteManagementFileReadRequest::new(options.remote_path.clone())
            .with_offset(offset)
            .with_max_bytes(MAX_FILE_READ_BYTES);
        let response: RemoteManagementFileReadResponse = remote_rpc(
            rpc,
            target,
            REMOTE_MANAGEMENT_FS_READ_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            &request,
        )?;
        let content = response.into_result().map_err(command_error)?;
        if content.path != options.remote_path {
            return Err(CliCommandError::new(format!(
                "remote read returned path `{}` while reading `{}`",
                content.path, options.remote_path
            )));
        }
        if content.len != expected_len {
            return Err(CliCommandError::new(format!(
                "remote file length changed during download: expected {expected_len}, got {}",
                content.len
            )));
        }
        if content.bytes.is_empty() {
            return Err(CliCommandError::new(format!(
                "remote read made no progress at offset {offset}"
            )));
        }
        if content.bytes.len() > MAX_FILE_READ_BYTES {
            return Err(CliCommandError::new(format!(
                "remote read returned {} bytes; maximum is {MAX_FILE_READ_BYTES}",
                content.bytes.len()
            )));
        }
        let next_offset = offset
            .checked_add(content.bytes.len() as u64)
            .ok_or_else(|| CliCommandError::new("remote read offset overflow"))?;
        if next_offset > expected_len {
            return Err(CliCommandError::new(format!(
                "remote read exceeded expected file length {expected_len}"
            )));
        }
        if next_offset < expected_len && !content.truncated {
            return Err(CliCommandError::new(
                "remote read reported end-of-file before the advertised length",
            ));
        }
        file.write_all(&content.bytes).map_err(|source| {
            CliCommandError::new(format!(
                "failed to write local transfer file {}: {source}",
                temp_path.path().display()
            ))
        })?;
        hasher.update(&content.bytes);
        offset = next_offset;
        chunks += 1;
    }
    file.sync_all().map_err(|source| {
        CliCommandError::new(format!(
            "failed to sync local transfer file {}: {source}",
            temp_path.path().display()
        ))
    })?;
    drop(file);

    let actual_hash = hasher.finalize().to_hex().to_string();
    ensure_hash_matches("downloaded file", &expected_hash, &actual_hash)?;
    let final_stat = fetch_remote_stat(rpc, target, &options.remote_path, true)?;
    let (final_len, final_hash) = verified_file_identity(&final_stat)?;
    if final_len != expected_len
        || normalize_blake3(&final_hash) != normalize_blake3(&expected_hash)
    {
        return Err(CliCommandError::new(
            "remote file changed while it was being downloaded",
        ));
    }
    temp_path.commit(&options.local_path)?;

    let report = RemoteTransferReport {
        direction: "get",
        source: options.remote_path,
        destination: options.local_path.display().to_string(),
        bytes: expected_len,
        blake3: actual_hash,
        chunks,
    };
    print_transfer_report(context, report, options.common.json)
}

fn run_remote_put(
    context: &mut CliCommandContext<'_>,
    rpc: &RemoteManagementCliRpc,
    options: RemotePutOptions,
) -> Result<(), CliCommandError> {
    // Open first so a path replacement cannot redirect later reads to a
    // different inode/handle after validation.
    let mut file = File::open(&options.local_path).map_err(|source| {
        CliCommandError::new(format!(
            "failed to open local file {}: {source}",
            options.local_path.display()
        ))
    })?;
    let metadata = file.metadata().map_err(|source| {
        CliCommandError::new(format!(
            "failed to inspect opened local file {}: {source}",
            options.local_path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(CliCommandError::new(format!(
            "local path {} is not a regular file",
            options.local_path.display()
        )));
    }
    let expected_len = metadata.len();
    let (snapshot_len, snapshot_hash) = hash_open_local_file(&mut file, &options.local_path)?;
    if snapshot_len != expected_len {
        return Err(CliCommandError::new(format!(
            "local file changed before upload: metadata reported {expected_len} bytes, read {snapshot_len}"
        )));
    }
    let mut buffer = vec![0_u8; MAX_FILE_WRITE_BYTES];
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    let mut chunks = 0_usize;
    loop {
        let read = file.read(&mut buffer).map_err(|source| {
            CliCommandError::new(format!(
                "failed to read local file {}: {source}",
                options.local_path.display()
            ))
        })?;
        if read == 0 && (offset != 0 || chunks != 0) {
            break;
        }
        let bytes = buffer[..read].to_vec();
        hasher.update(&bytes);
        let request =
            RemoteManagementFileWriteRequest::chunk(options.remote_path.clone(), offset, bytes)
                .creating_parent(options.create_parent)
                .truncating(offset == 0);
        request.validate().map_err(command_error)?;
        let response: RemoteManagementFileWriteResponse = remote_rpc_with_attempts(
            rpc,
            options.common.target.as_deref(),
            REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
            REMOTE_MANAGEMENT_RPC_CATEGORY,
            &request,
            TRANSFER_WRITE_ATTEMPTS,
        )?;
        let written = response.into_result().map_err(command_error)?;
        let next_offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| CliCommandError::new("remote write offset overflow"))?;
        if written.path != options.remote_path
            || written.bytes_written != read as u64
            || written.offset != Some(offset)
            || written.next_offset != Some(next_offset)
            || written.len != Some(next_offset)
        {
            return Err(CliCommandError::new(format!(
                "remote write did not acknowledge retry-safe chunk offset {offset} length {read}"
            )));
        }
        offset = next_offset;
        chunks += 1;
        if read == 0 {
            break;
        }
    }
    if offset != expected_len {
        return Err(CliCommandError::new(format!(
            "local file changed while uploading: expected {expected_len} bytes, read {offset}"
        )));
    }
    let local_hash = hasher.finalize().to_hex().to_string();
    let final_metadata = file.metadata().map_err(|source| {
        CliCommandError::new(format!(
            "failed to reinspect opened local file {}: {source}",
            options.local_path.display()
        ))
    })?;
    let (final_len, final_hash) = hash_open_local_file(&mut file, &options.local_path)?;
    if final_metadata.len() != expected_len
        || final_len != expected_len
        || normalize_blake3(&snapshot_hash) != normalize_blake3(&local_hash)
        || normalize_blake3(&final_hash) != normalize_blake3(&local_hash)
    {
        return Err(CliCommandError::new(
            "local file changed while it was being uploaded",
        ));
    }
    let stat = fetch_remote_stat(
        rpc,
        options.common.target.as_deref(),
        &options.remote_path,
        true,
    )?;
    let (remote_len, remote_hash) = verified_file_identity(&stat)?;
    if remote_len != expected_len {
        return Err(CliCommandError::new(format!(
            "uploaded file length mismatch: local {expected_len}, remote {remote_len}"
        )));
    }
    ensure_hash_matches("uploaded file", &remote_hash, &local_hash)?;

    let report = RemoteTransferReport {
        direction: "put",
        source: options.local_path.display().to_string(),
        destination: options.remote_path,
        bytes: expected_len,
        blake3: local_hash,
        chunks,
    };
    print_transfer_report(context, report, options.common.json)
}

fn hash_open_local_file(file: &mut File, path: &Path) -> Result<(u64, String), CliCommandError> {
    file.seek(std::io::SeekFrom::Start(0)).map_err(|source| {
        CliCommandError::new(format!(
            "failed to rewind local file {}: {source}",
            path.display()
        ))
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; MAX_FILE_WRITE_BYTES];
    let mut len = 0_u64;
    loop {
        let read = file.read(&mut buffer).map_err(|source| {
            CliCommandError::new(format!(
                "failed to hash local file {}: {source}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        len = len
            .checked_add(read as u64)
            .ok_or_else(|| CliCommandError::new("local file length overflow"))?;
        hasher.update(&buffer[..read]);
    }
    file.seek(std::io::SeekFrom::Start(0)).map_err(|source| {
        CliCommandError::new(format!(
            "failed to rewind local file {} after hashing: {source}",
            path.display()
        ))
    })?;
    Ok((len, hasher.finalize().to_hex().to_string()))
}

fn fetch_remote_stat(
    rpc: &RemoteManagementCliRpc,
    target: Option<&str>,
    path: &str,
    include_blake3: bool,
) -> Result<RemoteManagementFileStat, CliCommandError> {
    let request = RemoteManagementFileStatRequest::new(path).including_blake3(include_blake3);
    request.validate().map_err(command_error)?;
    let response: RemoteManagementFileStatResponse = remote_rpc(
        rpc,
        target,
        REMOTE_MANAGEMENT_FS_STAT_ROUTE,
        REMOTE_MANAGEMENT_RPC_CATEGORY,
        &request,
    )?;
    response.into_result().map_err(command_error)
}

fn verified_file_identity(
    stat: &RemoteManagementFileStat,
) -> Result<(u64, String), CliCommandError> {
    if stat.file_type != RemoteManagementFileType::File {
        return Err(CliCommandError::new(format!(
            "remote path {} is not a regular file",
            stat.path
        )));
    }
    let len = stat.len.ok_or_else(|| {
        CliCommandError::new(format!("remote path {} did not report a length", stat.path))
    })?;
    let hash = stat.blake3.clone().ok_or_else(|| {
        CliCommandError::new(format!(
            "remote path {} did not report the requested BLAKE3 digest",
            stat.path
        ))
    })?;
    validate_blake3(&hash)?;
    Ok((len, hash))
}

fn validate_blake3(value: &str) -> Result<(), CliCommandError> {
    let normalized = normalize_blake3(value);
    if normalized.len() != 64 || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CliCommandError::new(format!(
            "invalid remote BLAKE3 digest `{value}`"
        )));
    }
    Ok(())
}

fn normalize_blake3(value: &str) -> String {
    value
        .strip_prefix("blake3:")
        .unwrap_or(value)
        .to_ascii_lowercase()
}

fn ensure_hash_matches(label: &str, expected: &str, actual: &str) -> Result<(), CliCommandError> {
    if normalize_blake3(expected) == normalize_blake3(actual) {
        Ok(())
    } else {
        Err(CliCommandError::new(format!(
            "{label} BLAKE3 mismatch: expected {expected}, got {actual}"
        )))
    }
}

fn print_transfer_report(
    context: &mut CliCommandContext<'_>,
    report: RemoteTransferReport,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        return write_json(context, &report);
    }
    context.stdout_line(format_args!(
        "{}: {} -> {}",
        report.direction, report.source, report.destination
    ))?;
    context.stdout_line(format_args!("bytes: {}", report.bytes))?;
    context.stdout_line(format_args!("blake3: {}", report.blake3))?;
    context.stdout_line(format_args!("chunks: {}", report.chunks))
}

struct TransferTempPath {
    path: PathBuf,
    committed: bool,
}

impl TransferTempPath {
    fn new(destination: &Path) -> Result<Self, CliCommandError> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("download");
        let sequence = TRANSFER_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{name}.sinomo-get-{}-{sequence}.part",
            std::process::id()
        ));
        if path.exists() {
            return Err(CliCommandError::new(format!(
                "transfer temporary path already exists: {}",
                path.display()
            )));
        }
        Ok(Self {
            path,
            committed: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(mut self, destination: &Path) -> Result<(), CliCommandError> {
        sinomo_core::storage::durable_file::replace_file(&self.path, destination).map_err(
            |source| {
                CliCommandError::new(format!(
                    "failed to install downloaded file {}: {source}",
                    destination.display()
                ))
            },
        )?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for TransferTempPath {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn remote_rpc<Req, Resp>(
    rpc: &RemoteManagementCliRpc,
    target: Option<&str>,
    route: &str,
    category: &str,
    request: &Req,
) -> Result<Resp, CliCommandError>
where
    Req: Serialize + ?Sized,
    Resp: DeserializeOwned,
{
    remote_rpc_attempt(rpc, target, route, category, request).map_err(CliCommandError::new)
}

fn remote_rpc_with_attempts<Req, Resp>(
    rpc: &RemoteManagementCliRpc,
    target: Option<&str>,
    route: &str,
    category: &str,
    request: &Req,
    attempts: usize,
) -> Result<Resp, CliCommandError>
where
    Req: Serialize + ?Sized,
    Resp: DeserializeOwned,
{
    let attempts = attempts.max(1);
    let mut last_error = None;
    for _ in 0..attempts {
        match remote_rpc_attempt(rpc, target, route, category, request) {
            Ok(response) => return Ok(response),
            Err(error) => last_error = Some(error),
        }
    }
    Err(CliCommandError::new(last_error.unwrap_or_else(|| {
        "remote RPC failed without an error".to_string()
    })))
}

fn remote_rpc_attempt<Req, Resp>(
    rpc: &RemoteManagementCliRpc,
    target: Option<&str>,
    route: &str,
    category: &str,
    request: &Req,
) -> Result<Resp, String>
where
    Req: Serialize + ?Sized,
    Resp: DeserializeOwned,
{
    let request = ObjectRpcRequest::encode_body(route, category, request)
        .map_err(|error| format!("failed to encode {route} request: {error}"))?;
    let response =
        rpc(target, request).map_err(|error| format!("remote RPC {route} failed: {error}"))?;
    if response.route != route {
        return Err(format!(
            "remote RPC returned route `{}` for request `{route}`",
            response.route
        ));
    }
    response
        .decode_body()
        .map_err(|error| format!("failed to decode {route} response: {error}"))
}

fn run_node_permissions_command(
    context: &mut CliCommandContext<'_>,
    data_dir: &SinomoDataDir,
    command: NodePermissionsCliCommand,
) -> Result<(), CliCommandError> {
    if matches!(command, NodePermissionsCliCommand::Help) {
        return node_permissions_cli_help().write(context);
    }
    let store = LocalNodePermissionStore::new(data_dir.root());
    match command {
        NodePermissionsCliCommand::Help => unreachable!("handled above"),
        NodePermissionsCliCommand::Show { json } => {
            let permissions = store.load().map_err(command_error)?;
            print_local_permissions(context, &permissions, None, json)
        }
        NodePermissionsCliCommand::AllowRemoteManagement { json } => {
            let (permissions, changed) = store
                .mutate(|permissions| Ok(permissions.set_remote_management_enabled(true)))
                .map_err(command_error)?;
            print_local_permissions(context, &permissions, Some(changed), json)
        }
        NodePermissionsCliCommand::DenyRemoteManagement { json } => {
            let (permissions, changed) = store
                .mutate(|permissions| Ok(permissions.set_remote_management_enabled(false)))
                .map_err(command_error)?;
            print_local_permissions(context, &permissions, Some(changed), json)
        }
        NodePermissionsCliCommand::Expose {
            resource,
            operation,
            json,
        } => {
            let (permissions, changed) = store
                .mutate(|permissions| permissions.expose_resource_operation(resource, operation))
                .map_err(command_error)?;
            print_local_permissions(context, &permissions, Some(changed), json)
        }
        NodePermissionsCliCommand::Hide {
            resource,
            operation,
            json,
        } => {
            let (permissions, changed) = store
                .mutate(|permissions| match operation {
                    Some(operation) => permissions.hide_resource_operation(&resource, &operation),
                    None => permissions.hide_resource(&resource),
                })
                .map_err(command_error)?;
            print_local_permissions(context, &permissions, Some(changed), json)
        }
    }
}

#[derive(Serialize)]
struct LocalPermissionsOutput<'a> {
    local_only: bool,
    changed: Option<bool>,
    permissions: &'a LocalNodePermissions,
}

fn print_local_permissions(
    context: &mut CliCommandContext<'_>,
    permissions: &LocalNodePermissions,
    changed: Option<bool>,
    json: bool,
) -> Result<(), CliCommandError> {
    if json {
        return write_json(
            context,
            &LocalPermissionsOutput {
                local_only: true,
                changed,
                permissions,
            },
        );
    }
    context.stdout_line(format_args!(
        "remote-management: {}",
        if permissions.remote_management_enabled() {
            "allowed"
        } else {
            "denied"
        }
    ))?;
    if let Some(changed) = changed {
        context.stdout_line(format_args!("changed: {changed}"))?;
    }
    if permissions.resource_exposures().is_empty() {
        context.stdout_line(format_args!("resources: none"))?;
    } else {
        context.stdout_line(format_args!("resources:"))?;
        for (resource, exposure) in permissions.resource_exposures() {
            for operation in exposure.operations() {
                context.stdout_line(format_args!("  {resource}: {operation}"))?;
            }
        }
    }
    Ok(())
}

fn write_json(
    context: &mut CliCommandContext<'_>,
    value: &impl Serialize,
) -> Result<(), CliCommandError> {
    serde_json::to_writer_pretty(&mut *context.stdout(), value).map_err(|source| {
        CliCommandError::new(format!("failed to encode JSON output: {source}"))
    })?;
    write_stream(context.stdout(), b"\n", "stdout")
}

fn write_stream(
    stream: &mut dyn Write,
    bytes: &[u8],
    label: &'static str,
) -> Result<(), CliCommandError> {
    stream
        .write_all(bytes)
        .map_err(|source| CliCommandError::new(format!("failed to write {label}: {source}")))
}

fn command_error(error: impl fmt::Display) -> CliCommandError {
    CliCommandError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_config::{ConfigKey, ConfigValueSource};
    use std::sync::Mutex;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn run_handler(
        handler: &dyn CliCommandHandler,
        args: &[&str],
    ) -> Result<(String, String), String> {
        let command = handler
            .parse(&strings(args))
            .map_err(|error| error.to_string())?;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut context = CliCommandContext::new(&mut stdout, &mut stderr);
        command
            .run(&mut context)
            .map_err(|error| error.to_string())?;
        Ok((
            String::from_utf8(stdout).expect("UTF-8 stdout"),
            String::from_utf8(stderr).expect("UTF-8 stderr"),
        ))
    }

    fn system_info() -> RemoteManagementSystemInfo {
        RemoteManagementSystemInfo {
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            family: "unix".to_string(),
            current_dir: Some("/srv".to_string()),
            executable: Some("/usr/bin/sinomo".to_string()),
            default_shell: "/bin/sh".to_string(),
            env: BTreeMap::new(),
            env_redacted: Default::default(),
        }
    }

    #[test]
    fn remote_handler_forwards_target_and_bounds_one_shot_shell() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_rpc = Arc::clone(&seen);
        let handler = remote_cli_command_handler(move |target, request| {
            seen_for_rpc.lock().expect("seen").push((
                target.map(str::to_string),
                request.route.clone(),
                request.category.clone(),
            ));
            match request.route.as_str() {
                REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE => ObjectRpcResponse::encode_body(
                    REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
                    &RemoteManagementSystemInfoResponse::success(system_info()),
                )
                .map_err(|error| error.to_string()),
                REMOTE_MANAGEMENT_SHELL_RUN_ROUTE => {
                    let request: RemoteManagementShellRunRequest =
                        request.decode_body().map_err(|error| error.to_string())?;
                    assert_eq!(request.command, "echo ok");
                    assert_eq!(request.timeout_ms, Some(MAX_COMMAND_TIMEOUT_MS));
                    assert_eq!(request.max_output_bytes, Some(MAX_OUTPUT_BYTES));
                    ObjectRpcResponse::encode_body(
                        REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
                        &RemoteManagementShellRunResponse::success(RemoteManagementShellOutcome {
                            exit_code: Some(0),
                            stdout: "ok\n".to_string(),
                            stderr: String::new(),
                            duration_ms: 1,
                            timed_out: false,
                            stdout_truncated: false,
                            stderr_truncated: false,
                        }),
                    )
                    .map_err(|error| error.to_string())
                }
                route => Err(format!("unexpected route {route}")),
            }
        });

        let (stdout, _) =
            run_handler(&handler, &["info", "--target", "node-a", "--json"]).expect("remote info");
        assert!(stdout.contains("\"os\": \"linux\""));
        let (stdout, _) = run_handler(
            &handler,
            &[
                "shell",
                "--target",
                "node-b",
                "--timeout-ms",
                "999999999",
                "--max-output-bytes",
                "999999999",
                "--",
                "echo ok",
            ],
        )
        .expect("remote shell");
        assert_eq!(stdout, "ok\n");

        let seen = seen.lock().expect("seen");
        assert_eq!(seen[0].0.as_deref(), Some("node-a"));
        assert_eq!(seen[1].0.as_deref(), Some("node-b"));
        assert!(
            seen.iter()
                .all(|(_, _, category)| category == REMOTE_MANAGEMENT_RPC_CATEGORY)
        );
    }

    #[test]
    fn remote_shell_requires_and_preserves_one_explicit_command_string() {
        let command = "printf '%s\\n' 'a b' && printf '%s\\n' '$HOME'";
        let parsed = parse_remote_shell(&strings(&["--", command])).expect("quoted command");
        assert_eq!(parsed.command, command);

        let error = parse_remote_shell(&strings(&["--", "printf", "a b"]))
            .expect_err("tokenized command must be rejected");
        assert!(error.to_string().contains("exactly one command string"));

        let error = parse_remote_shell(&strings(&["echo ok"]))
            .expect_err("command without delimiter must be rejected");
        assert!(error.to_string().contains("must follow `--`"));
    }

    #[test]
    fn remote_observe_separates_verified_devices_from_resource_exposure_policies() {
        let handler = remote_cli_command_handler(|target, request| {
            assert_eq!(target, Some("node-observe"));
            assert_eq!(request.route, REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE);
            let request: RemoteManagementSystemObserveRequest =
                request.decode_body().map_err(|error| error.to_string())?;
            assert!(request.include_devices);
            assert!(request.include_resource_exposures);
            ObjectRpcResponse::encode_body(
                REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE,
                &RemoteManagementSystemObserveResponse::success(
                    RemoteManagementSystemObservation {
                        observed_at: "2026-07-15T00:00:00Z".to_string(),
                        devices: vec![RemoteManagementDeviceObservation {
                            id: "camera-0".to_string(),
                            kind: "camera".to_string(),
                            name: "Built-in camera".to_string(),
                            capabilities: vec!["capture".to_string()],
                            state: Some("available".to_string()),
                        }],
                        resource_exposure_policies: vec![RemoteManagementResourceExposurePolicy {
                            resource_id: "device:camera".to_string(),
                            allowed_operations: vec!["capture".to_string(), "inspect".to_string()],
                        }],
                        ..Default::default()
                    },
                ),
            )
            .map_err(|error| error.to_string())
        });

        let (stdout, _) = run_handler(
            &handler,
            &[
                "observe",
                "--devices",
                "--resources",
                "--target",
                "node-observe",
            ],
        )
        .expect("remote observation");
        assert!(
            stdout
                .contains("device: camera-0 kind=camera name=Built-in camera capabilities=capture")
        );
        assert!(stdout.contains(
            "resource-exposure-policy: device:camera allowed-operations=capture,inspect"
        ));
    }

    #[test]
    fn config_set_uses_authoritative_route_contract_and_write_target() {
        let seen = Arc::new(Mutex::new(None));
        let seen_for_rpc = Arc::clone(&seen);
        let handler = remote_cli_command_handler(move |target, request| {
            assert_eq!(request.route, CONFIG_SET_ROUTE);
            assert_eq!(request.category, CONFIG_RPC_CATEGORY);
            let body: ConfigSetRequest =
                request.decode_body().map_err(|error| error.to_string())?;
            *seen_for_rpc.lock().expect("seen") = Some((target.map(str::to_string), body.clone()));
            ObjectRpcResponse::encode_body(
                CONFIG_SET_ROUTE,
                &ConfigSetResponse {
                    result: ConfigWriteResult {
                        key: ConfigKey::new(body.key).map_err(|error| error.to_string())?,
                        value: body.value,
                        source: ConfigValueSource::Local,
                    },
                },
            )
            .map_err(|error| error.to_string())
        });

        run_handler(
            &handler,
            &[
                "config",
                "set",
                "sandbox.policy",
                "{\"enabled\":true}",
                "--write-target",
                "local",
                "--target",
                "node-config",
                "--json",
            ],
        )
        .expect("config set");

        let seen = seen.lock().expect("seen");
        let (target, request) = seen.as_ref().expect("request");
        assert_eq!(target.as_deref(), Some("node-config"));
        assert_eq!(request.target, ConfigWriteTarget::Local);
        assert_eq!(request.value, serde_json::json!({ "enabled": true }));
    }

    #[derive(Default)]
    struct FakeTransferState {
        remote: Vec<u8>,
        write_offsets: Vec<u64>,
        read_offsets: Vec<u64>,
        fail_first_zero_write: bool,
    }

    fn transfer_rpc(
        state: Arc<Mutex<FakeTransferState>>,
    ) -> impl Fn(Option<&str>, ObjectRpcRequest) -> Result<ObjectRpcResponse, String> + Send + Sync
    {
        move |_target, request| match request.route.as_str() {
            REMOTE_MANAGEMENT_FS_WRITE_ROUTE => {
                let write: RemoteManagementFileWriteRequest =
                    request.decode_body().map_err(|error| error.to_string())?;
                let offset = write
                    .effective_offset()
                    .ok_or_else(|| "append is not supported by transfer fake".to_string())?;
                let mut state = state.lock().expect("transfer state");
                state.write_offsets.push(offset);
                if offset == 0 && state.fail_first_zero_write {
                    state.fail_first_zero_write = false;
                    return Err("transient write failure".to_string());
                }
                if write.should_truncate() {
                    state.remote.clear();
                }
                let start = usize::try_from(offset).map_err(|error| error.to_string())?;
                let end = start
                    .checked_add(write.bytes.len())
                    .ok_or_else(|| "write range overflow".to_string())?;
                if state.remote.len() < end {
                    state.remote.resize(end, 0);
                }
                state.remote[start..end].copy_from_slice(&write.bytes);
                ObjectRpcResponse::encode_body(
                    REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
                    &RemoteManagementFileWriteResponse::success(
                        RemoteManagementFileWriteOutcome::for_chunk(
                            write.path,
                            offset,
                            write.bytes.len() as u64,
                            state.remote.len() as u64,
                        ),
                    ),
                )
                .map_err(|error| error.to_string())
            }
            REMOTE_MANAGEMENT_FS_READ_ROUTE => {
                let read: RemoteManagementFileReadRequest =
                    request.decode_body().map_err(|error| error.to_string())?;
                let offset = read.offset.unwrap_or(0);
                let mut state = state.lock().expect("transfer state");
                state.read_offsets.push(offset);
                let start = usize::try_from(offset).map_err(|error| error.to_string())?;
                let end = start
                    .saturating_add(read.effective_max_bytes())
                    .min(state.remote.len());
                let bytes = state.remote[start..end].to_vec();
                let len = state.remote.len() as u64;
                ObjectRpcResponse::encode_body(
                    REMOTE_MANAGEMENT_FS_READ_ROUTE,
                    &RemoteManagementFileReadResponse::success(RemoteManagementFileContent {
                        path: read.path,
                        bytes,
                        text: None,
                        len,
                        truncated: end < state.remote.len(),
                    }),
                )
                .map_err(|error| error.to_string())
            }
            REMOTE_MANAGEMENT_FS_STAT_ROUTE => {
                let stat: RemoteManagementFileStatRequest =
                    request.decode_body().map_err(|error| error.to_string())?;
                let state = state.lock().expect("transfer state");
                ObjectRpcResponse::encode_body(
                    REMOTE_MANAGEMENT_FS_STAT_ROUTE,
                    &RemoteManagementFileStatResponse::success(RemoteManagementFileStat {
                        path: stat.path,
                        file_type: RemoteManagementFileType::File,
                        len: Some(state.remote.len() as u64),
                        readonly: false,
                        modified_unix_ms: None,
                        blake3: stat
                            .include_blake3
                            .then(|| blake3::hash(&state.remote).to_hex().to_string()),
                    }),
                )
                .map_err(|error| error.to_string())
            }
            route => Err(format!("unexpected transfer route {route}")),
        }
    }

    #[test]
    fn get_and_put_use_bounded_chunks_retry_offsets_and_verify_blake3() {
        let root = sinomo_testdir::TestDir::new("remote-management-cli-transfer");
        let local_source = root.join("source.bin");
        let local_destination = root.join("downloaded.bin");
        let bytes: Vec<u8> = (0..(MAX_FILE_WRITE_BYTES * 2 + 37))
            .map(|index| (index % 251) as u8)
            .collect();
        fs::write(&local_source, &bytes).expect("source");

        let state = Arc::new(Mutex::new(FakeTransferState {
            fail_first_zero_write: true,
            ..Default::default()
        }));
        let handler = remote_cli_command_handler(transfer_rpc(Arc::clone(&state)));
        let source = local_source.display().to_string();
        run_handler(
            &handler,
            &[
                "put",
                &source,
                "/remote/data.bin",
                "--target",
                "node-files",
                "--json",
            ],
        )
        .expect("put");
        {
            let state = state.lock().expect("state");
            assert_eq!(state.remote, bytes);
            assert_eq!(
                state.write_offsets,
                vec![
                    0,
                    0,
                    MAX_FILE_WRITE_BYTES as u64,
                    (MAX_FILE_WRITE_BYTES * 2) as u64
                ]
            );
        }

        let destination = local_destination.display().to_string();
        run_handler(
            &handler,
            &[
                "get",
                "/remote/data.bin",
                &destination,
                "--target",
                "node-files",
                "--json",
            ],
        )
        .expect("get");
        assert_eq!(fs::read(local_destination).expect("download"), bytes);
        let state = state.lock().expect("state");
        assert_eq!(
            state.read_offsets,
            vec![
                0,
                MAX_FILE_READ_BYTES as u64,
                (MAX_FILE_READ_BYTES * 2) as u64
            ]
        );
    }

    #[test]
    fn node_permissions_are_default_deny_manual_and_strictly_local() {
        let root = sinomo_testdir::TestDir::new("remote-management-cli-permissions");
        let data_dir = SinomoDataDir::new(root.to_path_buf());
        let handler = node_permissions_cli_command_handler(data_dir.clone());
        let store = LocalNodePermissionStore::new(data_dir.root());

        let (stdout, _) =
            run_handler(&handler, &["permissions", "show"]).expect("show default permissions");
        assert!(stdout.contains("remote-management: denied"));
        assert!(!store.remote_management_enabled());

        run_handler(&handler, &["permissions", "allow", "remote-management"])
            .expect("allow locally");
        assert!(store.remote_management_enabled());

        run_handler(
            &handler,
            &["permissions", "expose", "device:camera", "capture"],
        )
        .expect("expose resource");
        assert!(
            store
                .load()
                .expect("permissions")
                .resource_operation_exposed("device:camera", "capture")
        );

        run_handler(
            &handler,
            &["permissions", "hide", "device:camera", "capture"],
        )
        .expect("hide resource operation");
        assert!(
            !store
                .load()
                .expect("permissions")
                .resource_operation_exposed("device:camera", "capture")
        );

        let error = match handler.parse(&strings(&[
            "permissions",
            "deny",
            "remote-management",
            "--target",
            "node-other",
        ])) {
            Ok(_) => panic!("target must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("local-only"));
        assert!(store.remote_management_enabled());

        run_handler(
            &handler,
            &["permissions", "deny", "remote-management", "--json"],
        )
        .expect("deny locally");
        assert!(!store.remote_management_enabled());
    }

    #[test]
    fn handlers_expose_help_autocomplete_and_authority_policies() {
        let remote = remote_cli_command_handler(|_, request| {
            Err(format!("unexpected RPC {}", request.route))
        });
        assert_eq!(remote.metadata().name, "remote");
        assert_eq!(remote.metadata().policy, CliCommandPolicy::local_capable());
        assert!(
            remote
                .help()
                .lines
                .iter()
                .any(|line| line.contains("remote config set"))
        );
        assert!(
            remote
                .help()
                .lines
                .iter()
                .any(|line| line.contains("--resources"))
        );
        let remote_completion = remote.autocomplete();
        assert!(
            remote_completion
                .children
                .iter()
                .any(|child| child.word == "put")
        );
        let observe_completion = remote_completion
            .children
            .iter()
            .find(|child| child.word == "observe")
            .expect("observe completion");
        assert!(
            observe_completion
                .options
                .iter()
                .any(|option| option.name == "--resources")
        );

        let root = sinomo_testdir::TestDir::new("remote-management-cli-help");
        let node = node_permissions_cli_command_handler(SinomoDataDir::new(root.to_path_buf()));
        assert_eq!(node.metadata().name, "node");
        assert_eq!(node.metadata().policy, CliCommandPolicy::local_capable());
        assert!(node.help().title.contains("local only"));
        assert_eq!(node.autocomplete().children[0].word, "permissions");
    }
}
