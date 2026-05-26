use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsCliCommand {
    Help,
    Status(DfsCommonOptions),
    FileSystem {
        file_system_id: DfsFileSystemId,
        options: DfsCommonOptions,
    },
    Path {
        path: String,
        file_system_id: DfsFileSystemId,
        options: DfsCommonOptions,
    },
    Object {
        object_id: DfsObjectId,
        options: DfsCommonOptions,
    },
    Conflicts(DfsFilterOptions),
    Repair(DfsFilterOptions),
    Smoke {
        json: bool,
    },
    MetadataPublish {
        json: bool,
    },
    Services {
        json: bool,
    },
    Pin(DfsPinCliCommand),
    Expose(DfsExposeCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct DfsCommonOptions {
    pub(crate) metadata_root: Option<PathBuf>,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct DfsFilterOptions {
    pub(crate) common: DfsCommonOptions,
    pub(crate) file_system_id: Option<DfsFileSystemId>,
    pub(crate) object_id: Option<DfsObjectId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsPinCliCommand {
    List(DfsPinListOptions),
    Add(DfsPinAddOptions),
    Remove(DfsPinRemoveOptions),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct DfsPinListOptions {
    pub(crate) pin_root: Option<PathBuf>,
    pub(crate) file_system_id: Option<DfsFileSystemId>,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsPinAddOptions {
    pub(crate) pin_root: Option<PathBuf>,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: String,
    pub(crate) object_id: Option<DfsObjectId>,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) mode: DfsPinMode,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsPinRemoveOptions {
    pub(crate) pin_root: Option<PathBuf>,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: String,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsExposeCliCommand {
    Init(DfsExposeCommonOptions),
    List(DfsExposeCommonOptions),
    Add(DfsExposeAddOptions),
    Remove(DfsExposeRemoveOptions),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct DfsExposeCommonOptions {
    pub(crate) config: Option<PathBuf>,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsExposeAddOptions {
    pub(crate) common: DfsExposeCommonOptions,
    pub(crate) id: Option<String>,
    pub(crate) source: PathBuf,
    pub(crate) dfs_path: String,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) writable: bool,
    pub(crate) follow_symlinks: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsExposeRemoveOptions {
    pub(crate) common: DfsExposeCommonOptions,
    pub(crate) id: String,
}

pub(crate) fn parse_dfs_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    match args.split_first() {
        None => Ok(DfsCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(DfsCliCommand::Help)
        }
        Some((command, rest)) if command == "status" => {
            Ok(DfsCliCommand::Status(parse_dfs_common_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "filesystem" | "fs") => {
            let (id, rest) = rest.split_first().ok_or(CliError::Usage)?;
            Ok(DfsCliCommand::FileSystem {
                file_system_id: parse_dfs_file_system_id(id)?,
                options: parse_dfs_common_options(rest)?,
            })
        }
        Some((command, rest)) if command == "path" => parse_dfs_path_command(rest),
        Some((command, rest)) if command == "object" => {
            let (id, rest) = rest.split_first().ok_or(CliError::Usage)?;
            Ok(DfsCliCommand::Object {
                object_id: parse_dfs_object_id(id)?,
                options: parse_dfs_common_options(rest)?,
            })
        }
        Some((command, rest)) if command == "conflicts" => {
            Ok(DfsCliCommand::Conflicts(parse_dfs_filter_options(rest)?))
        }
        Some((command, rest)) if command == "repair" => {
            Ok(DfsCliCommand::Repair(parse_dfs_filter_options(rest)?))
        }
        Some((command, rest)) if command == "smoke" => parse_dfs_smoke_command(rest),
        Some((command, rest)) if command == "services" => parse_dfs_services_command(rest),
        Some((command, rest)) if command == "metadata" => parse_dfs_metadata_command(rest),
        Some((command, rest)) if command == "pin" => {
            Ok(DfsCliCommand::Pin(parse_dfs_pin_command(rest)?))
        }
        Some((command, rest)) if command == "expose" => {
            Ok(DfsCliCommand::Expose(parse_dfs_expose_command(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_dfs_common_options(args: &[String]) -> Result<DfsCommonOptions, CliError> {
    let mut options = DfsCommonOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--metadata-root" => {
                index += 1;
                let Some(path) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.metadata_root = Some(PathBuf::from(path));
            }
            "--json" => options.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_dfs_path_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    let (path, rest) = args.split_first().ok_or(CliError::Usage)?;
    let mut file_system_id = default_dfs_file_system_id();
    let mut common_args = Vec::new();
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--filesystem" | "--fs" => {
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                file_system_id = parse_dfs_file_system_id(value)?;
            }
            "--metadata-root" => {
                common_args.push(rest[index].clone());
                index += 1;
                let Some(value) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                common_args.push(value.clone());
            }
            "--json" => common_args.push(rest[index].clone()),
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsCliCommand::Path {
        path: path.clone(),
        file_system_id,
        options: parse_dfs_common_options(&common_args)?,
    })
}

pub(crate) fn parse_dfs_filter_options(args: &[String]) -> Result<DfsFilterOptions, CliError> {
    let mut options = DfsFilterOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--metadata-root" => {
                index += 1;
                let Some(path) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.common.metadata_root = Some(PathBuf::from(path));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                let Some(file_system_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.file_system_id = Some(parse_dfs_file_system_id(file_system_id)?);
            }
            "--object" => {
                index += 1;
                let Some(object_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.object_id = Some(parse_dfs_object_id(object_id)?);
            }
            "--json" => options.common.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_dfs_services_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(DfsCliCommand::Services { json })
}

pub(crate) fn parse_dfs_smoke_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    let mut json = false;
    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            _ => return Err(CliError::Usage),
        }
    }
    Ok(DfsCliCommand::Smoke { json })
}

pub(crate) fn parse_dfs_metadata_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "publish" => {
            let mut json = false;
            for arg in rest {
                match arg.as_str() {
                    "--json" => json = true,
                    _ => return Err(CliError::Usage),
                }
            }
            Ok(DfsCliCommand::MetadataPublish { json })
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_dfs_pin_command(args: &[String]) -> Result<DfsPinCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "list" => {
            Ok(DfsPinCliCommand::List(parse_dfs_pin_list_options(rest)?))
        }
        Some((command, rest)) if command == "add" => {
            Ok(DfsPinCliCommand::Add(parse_dfs_pin_add_options(rest)?))
        }
        Some((command, rest)) if command == "remove" => Ok(DfsPinCliCommand::Remove(
            parse_dfs_pin_remove_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_dfs_pin_list_options(args: &[String]) -> Result<DfsPinListOptions, CliError> {
    let mut options = DfsPinListOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--pin-root" => {
                index += 1;
                let Some(path) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.pin_root = Some(PathBuf::from(path));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                let Some(file_system_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.file_system_id = Some(parse_dfs_file_system_id(file_system_id)?);
            }
            "--json" => options.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_dfs_pin_add_options(args: &[String]) -> Result<DfsPinAddOptions, CliError> {
    let mut pin_root = None;
    let mut file_system_id = None;
    let mut path = None;
    let mut object_id = None;
    let mut node_id = None;
    let mut mode = DfsPinMode::Content;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--pin-root" => {
                index += 1;
                pin_root = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                file_system_id = Some(parse_dfs_file_system_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--path" => {
                index += 1;
                path = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "path",
                )?);
            }
            "--object" => {
                index += 1;
                object_id = Some(parse_dfs_object_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--node" => {
                index += 1;
                node_id = Some(parse_node_id(args.get(index).ok_or(CliError::Usage)?)?);
            }
            "--mode" => {
                index += 1;
                mode = parse_dfs_pin_mode(args.get(index).ok_or(CliError::Usage)?)?;
            }
            "--json" => json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsPinAddOptions {
        pin_root,
        file_system_id: file_system_id.unwrap_or_else(default_dfs_file_system_id),
        path: path.ok_or(CliError::Usage)?,
        object_id,
        node_id,
        mode,
        json,
    })
}

pub(crate) fn parse_dfs_pin_remove_options(
    args: &[String],
) -> Result<DfsPinRemoveOptions, CliError> {
    let mut pin_root = None;
    let mut file_system_id = None;
    let mut path = None;
    let mut node_id = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--pin-root" => {
                index += 1;
                pin_root = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                file_system_id = Some(parse_dfs_file_system_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--path" => {
                index += 1;
                path = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "path",
                )?);
            }
            "--node" => {
                index += 1;
                node_id = Some(parse_node_id(args.get(index).ok_or(CliError::Usage)?)?);
            }
            "--json" => json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsPinRemoveOptions {
        pin_root,
        file_system_id: file_system_id.unwrap_or_else(default_dfs_file_system_id),
        path: path.ok_or(CliError::Usage)?,
        node_id,
        json,
    })
}

pub(crate) fn parse_dfs_expose_command(args: &[String]) -> Result<DfsExposeCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "init" => {
            Ok(DfsExposeCliCommand::Init(parse_dfs_expose_common(rest)?))
        }
        Some((command, rest)) if command == "list" => {
            Ok(DfsExposeCliCommand::List(parse_dfs_expose_common(rest)?))
        }
        Some((command, rest)) if command == "add" => {
            Ok(DfsExposeCliCommand::Add(parse_dfs_expose_add(rest)?))
        }
        Some((command, rest)) if command == "remove" => {
            Ok(DfsExposeCliCommand::Remove(parse_dfs_expose_remove(rest)?))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_dfs_expose_common(args: &[String]) -> Result<DfsExposeCommonOptions, CliError> {
    let mut options = DfsExposeCommonOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                options.config = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--json" => options.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_dfs_expose_add(args: &[String]) -> Result<DfsExposeAddOptions, CliError> {
    let mut common = DfsExposeCommonOptions::default();
    let mut id = None;
    let mut source = None;
    let mut dfs_path = None;
    let mut file_system_id = None;
    let mut node_id = None;
    let mut writable = true;
    let mut follow_symlinks = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                common.config = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--id" => {
                index += 1;
                id = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "id",
                )?);
            }
            "--source" => {
                index += 1;
                source = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--dfs-path" | "--path" => {
                index += 1;
                dfs_path = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "dfs path",
                )?);
            }
            "--filesystem" | "--fs" => {
                index += 1;
                file_system_id = Some(parse_dfs_file_system_id(
                    args.get(index).ok_or(CliError::Usage)?,
                )?);
            }
            "--node" => {
                index += 1;
                node_id = Some(parse_node_id(args.get(index).ok_or(CliError::Usage)?)?);
            }
            "--read-only" => writable = false,
            "--follow-symlinks" => follow_symlinks = true,
            "--json" => common.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsExposeAddOptions {
        common,
        id,
        source: source.ok_or(CliError::Usage)?,
        dfs_path: dfs_path.ok_or(CliError::Usage)?,
        file_system_id: file_system_id.unwrap_or_else(default_dfs_file_system_id),
        node_id,
        writable,
        follow_symlinks,
    })
}

pub(crate) fn parse_dfs_expose_remove(args: &[String]) -> Result<DfsExposeRemoveOptions, CliError> {
    let mut common = DfsExposeCommonOptions::default();
    let mut id = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                common.config = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--id" => {
                index += 1;
                id = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "id",
                )?);
            }
            "--json" => common.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(DfsExposeRemoveOptions {
        common,
        id: id.ok_or(CliError::Usage)?,
    })
}

pub(crate) fn run_dfs_cli_command(command: DfsCliCommand) -> Result<(), CliError> {
    match command {
        DfsCliCommand::Help => {
            print_dfs_help();
            Ok(())
        }
        DfsCliCommand::Status(options) => {
            let status = inspect_dfs_status(dfs_metadata_root(options.metadata_root.clone())?)?;
            if options.json {
                print_json("<dfs-status>", &status)?;
            } else {
                println!("DFS status:");
                println!("  filesystems: {}", status.file_system_count);
                println!("  objects: {}", status.object_count);
                println!("  versions: {}", status.version_count);
                println!("  receipts: {}", status.receipt_count);
                println!("  intents: {}", status.intent_count);
                println!("  pending writes: {}", status.pending_write_count);
                println!("  pending repairs: {}", status.pending_repair_count);
                println!("  open conflicts: {}", status.open_conflict_count);
            }
            Ok(())
        }
        DfsCliCommand::FileSystem {
            file_system_id,
            options,
        } => {
            let Some(file_system) = inspect_dfs_file_system(
                dfs_metadata_root(options.metadata_root.clone())?,
                &file_system_id,
            )?
            else {
                return Err(CliError::MissingArtifact {
                    artifact: format!("DFS filesystem {file_system_id}"),
                });
            };
            if options.json {
                print_json("<dfs-filesystem>", &file_system)?;
            } else {
                println!("DFS filesystem: {}", file_system.file_system.file_system_id);
                println!("  name: {}", file_system.file_system.display_name);
                println!("  owner: {}", file_system.file_system.owner_node_id);
                println!(
                    "  subtree policies: {}",
                    file_system.file_system.subtree_policies.len()
                );
                println!("  objects: {}", file_system.object_count);
                println!("  pending repairs: {}", file_system.pending_repair_count);
                println!("  open conflicts: {}", file_system.open_conflict_count);
            }
            Ok(())
        }
        DfsCliCommand::Path {
            path,
            file_system_id,
            options,
        } => {
            let Some(object) = inspect_dfs_path(
                dfs_metadata_root(options.metadata_root.clone())?,
                &file_system_id,
                &path,
            )?
            else {
                return Err(CliError::MissingArtifact {
                    artifact: format!("DFS path {path} in filesystem {file_system_id}"),
                });
            };
            if options.json {
                print_json("<dfs-path>", &object)?;
            } else {
                println!("DFS path: {path}");
                print_dfs_object_view(&object);
            }
            Ok(())
        }
        DfsCliCommand::Object { object_id, options } => {
            let Some(object) = inspect_dfs_object(
                dfs_metadata_root(options.metadata_root.clone())?,
                &object_id,
            )?
            else {
                return Err(CliError::MissingArtifact {
                    artifact: format!("DFS object {object_id}"),
                });
            };
            if options.json {
                print_json("<dfs-object>", &object)?;
            } else {
                print_dfs_object_view(&object);
            }
            Ok(())
        }
        DfsCliCommand::Conflicts(options) => {
            let conflicts = inspect_dfs_conflicts(
                dfs_metadata_root(options.common.metadata_root.clone())?,
                options.file_system_id.as_ref(),
                options.object_id.as_ref(),
            )?;
            if options.common.json {
                print_json("<dfs-conflicts>", &conflicts)?;
            } else if conflicts.is_empty() {
                println!("No DFS conflicts.");
            } else {
                for conflict in conflicts {
                    println!(
                        "{} object={} local={} remote={} status={:?} reason={}",
                        conflict.conflict_id,
                        conflict.object_id,
                        conflict.local_version,
                        conflict.remote_version,
                        conflict.status,
                        conflict.reason
                    );
                }
            }
            Ok(())
        }
        DfsCliCommand::Repair(options) => {
            let repairs = inspect_dfs_repairs(
                dfs_metadata_root(options.common.metadata_root.clone())?,
                options.file_system_id.as_ref(),
                options.object_id.as_ref(),
            )?;
            if options.common.json {
                print_json("<dfs-repair>", &repairs)?;
            } else if repairs.is_empty() {
                println!("No DFS repairs are pending.");
            } else {
                for repair in repairs {
                    println!(
                        "{} object={} version={} missing={} hash={}",
                        repair.intent_id,
                        repair.object_id,
                        repair.version,
                        repair.missing_replica_node_id,
                        option_label(&repair.content_hash)
                    );
                }
            }
            Ok(())
        }
        DfsCliCommand::Smoke { json } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let report = daemon_run_daemon_dfs_smoke(&DaemonPaths::new(&data_dir))?;
            if json {
                print_json("<dfs-smoke>", &report)?;
            } else {
                println!("DFS smoke write:");
                println!("  path: {}", report.path);
                println!("  object: {}", report.object_id);
                println!("  version: {}", report.version);
                println!(
                    "  size: {}",
                    report
                        .size_bytes
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                );
                println!(
                    "  requested replicas: {}",
                    report.requested_replica_node_ids.len()
                );
                for node_id in &report.requested_replica_node_ids {
                    println!("    {node_id}");
                }
                println!(
                    "  committed replicas: {}",
                    report.committed_replica_node_ids.len()
                );
                for node_id in &report.committed_replica_node_ids {
                    println!("    {node_id}");
                }
                println!("  pending repairs: {}", report.pending_repair_count);
                println!(
                    "  metadata replication outcomes: {}",
                    report.metadata_replication.outcomes.len()
                );
            }
            Ok(())
        }
        DfsCliCommand::Services { json } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            let daemon_paths = DaemonPaths::new(&data_dir);
            let services = DfsServiceAdvertisements {
                metadata: decode_dfs_advertisements(daemon_handle_daemon_cluster_kv_range_prefix(
                    &daemon_paths,
                    sinomo_runtime::dfs::metadata_services_prefix(),
                )?)?,
                content: decode_dfs_advertisements(daemon_handle_daemon_cluster_kv_range_prefix(
                    &daemon_paths,
                    sinomo_runtime::dfs::content_services_prefix(),
                )?)?,
            };
            if json {
                print_json("<dfs-services>", &services)?;
            } else {
                println!("DFS services:");
                println!("  metadata: {}", services.metadata.len());
                for service in &services.metadata {
                    println!(
                        "    {} endpoint={} metadata={} content={}",
                        service.node_id,
                        service.endpoint,
                        service.accepts_metadata,
                        service.accepts_content
                    );
                }
                println!("  content: {}", services.content.len());
                for service in &services.content {
                    println!(
                        "    {} endpoint={} metadata={} content={}",
                        service.node_id,
                        service.endpoint,
                        service.accepts_metadata,
                        service.accepts_content
                    );
                }
            }
            Ok(())
        }
        DfsCliCommand::MetadataPublish { json } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let report = daemon_publish_default_dfs_metadata_journal(&DaemonPaths::new(&data_dir))?;
            if json {
                print_json("<dfs-metadata-publish>", &report)?;
            } else if report.outcomes.is_empty() {
                println!("No DFS metadata journal replication work.");
            } else {
                for outcome in report.outcomes {
                    println!("{outcome:?}");
                }
            }
            Ok(())
        }
        DfsCliCommand::Pin(command) => run_dfs_pin_command(command),
        DfsCliCommand::Expose(command) => run_dfs_expose_command(command),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsServiceAdvertisements {
    metadata: Vec<sinomo_runtime::dfs::DfsNodeAdvertisement>,
    content: Vec<sinomo_runtime::dfs::DfsNodeAdvertisement>,
}

fn decode_dfs_advertisements(
    entries: Vec<sinomo_runtime::kv::KvEntry>,
) -> Result<Vec<sinomo_runtime::dfs::DfsNodeAdvertisement>, CliError> {
    let mut advertisements: Vec<sinomo_runtime::dfs::DfsNodeAdvertisement> = Vec::new();
    for entry in entries {
        advertisements.push(entry.decode()?);
    }
    advertisements.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.endpoint.cmp(&right.endpoint))
    });
    Ok(advertisements)
}

fn print_dfs_object_view(object: &sinomo_runtime::dfs::DfsOperatorObjectView) {
    println!("DFS object: {}", object.object.object_id);
    println!("  filesystem: {}", object.object.file_system_id);
    println!("  path: {}", object.object.path);
    println!("  owner: {}", object.object.owner_node_id);
    println!("  policy: {:?}", object.object.policy);
    println!("  visible version: {}", object.object.version);
    println!(
        "  content hash: {}",
        option_label(&object.object.content_hash)
    );
    println!(
        "  locations: {}",
        join_labels(&object.object.current_locations)
    );
    println!("  agent state: {:?}", object.agent_state);
    println!("  versions: {}", object.versions.len());
    println!("  receipts: {}", object.receipts.len());
    println!("  intents: {}", object.intents.len());
    println!("  pending repairs: {}", object.repairs.len());
    println!(
        "  open conflicts: {}",
        object
            .conflicts
            .iter()
            .filter(|conflict| conflict.status == sinomo_runtime::dfs::DfsConflictStatus::Open)
            .count()
    );
}

pub(crate) fn run_dfs_expose_command(command: DfsExposeCliCommand) -> Result<(), CliError> {
    match command {
        DfsExposeCliCommand::Init(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let node_id = default_dfs_node_id();
            let config_path = dfs_expose_config_path(options.config, &data_dir);
            let config = ensure_default_dfs_expose_config(&config_path, data_dir.root(), node_id)?;
            if options.json {
                print_json("<dfs-expose-config>", &config)?;
            } else {
                println!("DFS expose config: {}", config_path.display());
                for entry in config.entries {
                    println!(
                        "{} {} -> {} owner={} writable={}",
                        entry.file_system_id,
                        entry.source_path.display(),
                        entry.dfs_path,
                        entry.owner_node_id,
                        entry.writable
                    );
                }
            }
            Ok(())
        }
        DfsExposeCliCommand::List(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let config_path = dfs_expose_config_path(options.config, &data_dir);
            let config = load_dfs_expose_config(&config_path)?;
            if options.json {
                print_json("<dfs-expose-config>", &config)?;
            } else if config.entries.is_empty() {
                println!("No DFS exposed directories.");
            } else {
                for entry in config.entries {
                    println!(
                        "{} {} -> {} owner={} writable={} symlinks={}",
                        entry.id,
                        entry.source_path.display(),
                        entry.dfs_path,
                        entry.owner_node_id,
                        entry.writable,
                        entry.follow_symlinks
                    );
                }
            }
            Ok(())
        }
        DfsExposeCliCommand::Add(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let config_path = dfs_expose_config_path(options.common.config.clone(), &data_dir);
            let node_id = options.node_id.unwrap_or_else(default_dfs_node_id);
            let config = add_dfs_expose_entry(
                &config_path,
                DfsExposeAddRequest {
                    id: options.id,
                    file_system_id: options.file_system_id,
                    dfs_path: options.dfs_path,
                    source_path: options.source,
                    owner_node_id: node_id,
                    writable: options.writable,
                    follow_symlinks: options.follow_symlinks,
                    created_at: OffsetDateTime::now_utc(),
                },
            )?;
            if options.common.json {
                print_json("<dfs-expose-config>", &config)?;
            } else {
                println!("DFS expose config updated: {}", config_path.display());
            }
            Ok(())
        }
        DfsExposeCliCommand::Remove(options) => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let config_path = dfs_expose_config_path(options.common.config.clone(), &data_dir);
            let (_config, removed) = remove_dfs_expose_entry(&config_path, &options.id)?;
            if options.common.json {
                print_json(
                    "<dfs-expose-remove>",
                    &serde_json::json!({ "id": options.id, "removed": removed }),
                )?;
            } else if removed {
                println!("removed DFS exposure");
            } else {
                println!("no matching DFS exposure");
            }
            Ok(())
        }
    }
}

pub(crate) fn run_dfs_pin_command(command: DfsPinCliCommand) -> Result<(), CliError> {
    match command {
        DfsPinCliCommand::List(options) => {
            let pins = list_local_dfs_pins(
                dfs_pin_root(options.pin_root)?,
                options.file_system_id.as_ref(),
            )?;
            if options.json {
                print_json("<dfs-pins>", &pins)?;
            } else if pins.is_empty() {
                println!("No DFS pins.");
            } else {
                for pin in pins {
                    println!(
                        "{} path={} node={} mode={:?} object={}",
                        pin.file_system_id,
                        pin.path,
                        pin.pinned_by_node_id,
                        pin.mode,
                        pin.object_id
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "-".to_string())
                    );
                }
            }
            Ok(())
        }
        DfsPinCliCommand::Add(options) => {
            let node_id = options.node_id.unwrap_or_else(default_dfs_node_id);
            let record = add_local_dfs_pin(
                dfs_pin_root(options.pin_root)?,
                DfsPinAddRequest {
                    file_system_id: options.file_system_id,
                    path: options.path,
                    object_id: options.object_id,
                    pinned_by_node_id: node_id,
                    mode: options.mode,
                    created_at: OffsetDateTime::now_utc(),
                },
            )?;
            if options.json {
                print_json("<dfs-pin>", &record)?;
            } else {
                println!(
                    "pinned {} path={} node={} mode={:?}",
                    record.file_system_id, record.path, record.pinned_by_node_id, record.mode
                );
            }
            Ok(())
        }
        DfsPinCliCommand::Remove(options) => {
            let removed = remove_local_dfs_pin(
                dfs_pin_root(options.pin_root)?,
                &options.file_system_id,
                &options.path,
                options.node_id.as_ref(),
            )?;
            if options.json {
                print_json(
                    "<dfs-pin-remove>",
                    &serde_json::json!({
                        "file_system_id": options.file_system_id,
                        "path": options.path,
                        "removed": removed,
                    }),
                )?;
            } else if removed {
                println!(
                    "removed pin for {} path={}",
                    options.file_system_id, options.path
                );
            } else {
                println!(
                    "no matching pin for {} path={}",
                    options.file_system_id, options.path
                );
            }
            Ok(())
        }
    }
}

pub(crate) fn dfs_metadata_root(explicit: Option<PathBuf>) -> Result<PathBuf, CliError> {
    resolve_dfs_metadata_root(explicit).map_err(Into::into)
}

pub(crate) fn dfs_pin_root(explicit: Option<PathBuf>) -> Result<PathBuf, CliError> {
    resolve_dfs_pin_root(explicit).map_err(Into::into)
}

pub(crate) fn parse_dfs_file_system_id(value: &str) -> Result<DfsFileSystemId, CliError> {
    DfsFileSystemId::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid DFS filesystem id `{value}`: {source}"),
    })
}

pub(crate) fn default_dfs_file_system_id() -> DfsFileSystemId {
    sifs_file_system_id()
}

pub(crate) fn parse_dfs_object_id(value: &str) -> Result<DfsObjectId, CliError> {
    DfsObjectId::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid DFS object id `{value}`: {source}"),
    })
}

pub(crate) fn parse_node_id(value: &str) -> Result<NodeId, CliError> {
    NodeId::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid node id `{value}`: {source}"),
    })
}

pub(crate) fn parse_dfs_pin_mode(value: &str) -> Result<DfsPinMode, CliError> {
    match value {
        "metadata" => Ok(DfsPinMode::Metadata),
        "content" => Ok(DfsPinMode::Content),
        _ => Err(CliError::Parse {
            detail: format!("invalid DFS pin mode `{value}`"),
        }),
    }
}

pub(crate) fn default_dfs_node_id() -> NodeId {
    cluster_paths_from_defaults()
        .and_then(|paths| read_cluster_state(&paths))
        .map(|state| state.local_node_id)
        .unwrap_or_else(|_| fallback_dfs_node_id())
}

pub(crate) fn non_empty_string(value: &str, field: &str) -> Result<String, CliError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CliError::Parse {
            detail: format!("{field} cannot be empty"),
        });
    }
    Ok(trimmed.to_string())
}

pub(crate) fn print_json<T: Serialize>(label: &str, value: &T) -> Result<(), CliError> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|source| CliError::Json {
            path: label.to_string(),
            detail: source.to_string(),
        })?
    );
    Ok(())
}

pub(crate) fn option_label(value: &Option<String>) -> String {
    value.clone().unwrap_or_else(|| "-".to_string())
}

pub(crate) fn join_labels<T: ToString>(values: &[T]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}
