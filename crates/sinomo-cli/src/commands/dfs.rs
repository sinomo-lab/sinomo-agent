use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;

const DFS_CLUSTER_STATUS_REMOTE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);
const DFS_SMOKE_DAEMON_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(9_500);

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
    ResolveConflicts(DfsConflictResolveOptions),
    ConflictVersions(DfsConflictVersionsOptions),
    UseConflictVersion(DfsConflictUseVersionOptions),
    SaveConflictVersion(DfsConflictSaveVersionOptions),
    Repair(DfsFilterOptions),
    Smoke {
        json: bool,
    },
    MetadataPublish {
        json: bool,
    },
    MetadataShardTransferPrimary(DfsMetadataShardTransferPrimaryOptions),
    Mount(DfsMountOptions),
    Services {
        json: bool,
    },
    Pin(DfsPinCliCommand),
    Expose(DfsExposeCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct DfsCommonOptions {
    pub(crate) metadata_root: Option<PathBuf>,
    pub(crate) cluster: bool,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct DfsFilterOptions {
    pub(crate) common: DfsCommonOptions,
    pub(crate) file_system_id: Option<DfsFileSystemId>,
    pub(crate) object_id: Option<DfsObjectId>,
    pub(crate) limit: Option<usize>,
    pub(crate) cursor: Option<DfsOperatorInspectionCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsConflictResolveOptions {
    pub(crate) common: DfsCommonOptions,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: Option<String>,
    pub(crate) object_id: Option<DfsObjectId>,
    pub(crate) conflict_ids: Vec<String>,
    pub(crate) delete_file: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsConflictVersionsOptions {
    pub(crate) common: DfsCommonOptions,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: Option<String>,
    pub(crate) object_id: Option<DfsObjectId>,
    pub(crate) conflict_ids: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsConflictUseVersionOptions {
    pub(crate) common: DfsCommonOptions,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: Option<String>,
    pub(crate) object_id: Option<DfsObjectId>,
    pub(crate) version: DfsVersion,
    pub(crate) conflict_ids: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsConflictSaveVersionOptions {
    pub(crate) common: DfsCommonOptions,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) path: Option<String>,
    pub(crate) object_id: Option<DfsObjectId>,
    pub(crate) version: DfsVersion,
    pub(crate) conflict_ids: Vec<String>,
    pub(crate) destination_path: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsMountOptions {
    pub(crate) mount_point: PathBuf,
    pub(crate) root_path: String,
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) read_only: bool,
    pub(crate) allow_other: bool,
    pub(crate) foreground: bool,
    pub(crate) sync_local_changes: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsMetadataShardTransferPrimaryOptions {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shard_id: String,
    pub(crate) new_primary_node_id: NodeId,
    pub(crate) expected_epoch: Option<u64>,
    pub(crate) json: bool,
}

#[derive(Debug, Serialize)]
struct DfsConflictCliPage {
    conflicts: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct DfsConflictResolveCliReport {
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    deleted_file: bool,
    removed: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    not_found_conflict_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DfsConflictUseVersionCliReport {
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    version: DfsVersion,
    changed: bool,
    removed: Vec<DfsConflictRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    not_found_conflict_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DfsConflictSaveVersionCliReport {
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_path: Option<String>,
    version: DfsVersion,
    destination_path: String,
    bytes_written: usize,
}

#[derive(Debug, Serialize)]
struct DfsRepairCliPage {
    repairs: Vec<DfsOperatorRepairView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct DfsMetadataShardTransferPrimaryReport {
    file_system_id: DfsFileSystemId,
    shard_id: String,
    previous_primary_node_id: NodeId,
    new_primary_node_id: NodeId,
    previous_epoch: u64,
    new_epoch: u64,
    changed: bool,
    shard: DfsMetadataShardConfig,
}

pub(crate) enum DfsInspectionMetadata {
    Root(LocalSobjMetadataBackend),
    Sharded(LocalShardedMetadataBackend),
}

impl DfsInspectionMetadata {
    pub(crate) fn inspector(&self) -> DfsOperatorInspector<'_> {
        match self {
            Self::Root(backend) => DfsOperatorInspector::new(backend),
            Self::Sharded(backend) => DfsOperatorInspector::new(backend),
        }
    }

    pub(crate) fn backend_mut(&mut self) -> &mut dyn DfsMetadataBackend {
        match self {
            Self::Root(backend) => backend,
            Self::Sharded(backend) => backend,
        }
    }
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
    pub(crate) mode: Option<DfsPinMode>,
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
        Some((command, rest)) if command == "conflicts" => parse_dfs_conflicts_command(rest),
        Some((command, rest)) if command == "repair" => {
            Ok(DfsCliCommand::Repair(parse_dfs_filter_options(rest)?))
        }
        Some((command, rest)) if command == "smoke" => parse_dfs_smoke_command(rest),
        Some((command, rest)) if command == "services" => parse_dfs_services_command(rest),
        Some((command, rest)) if command == "metadata" => parse_dfs_metadata_command(rest),
        Some((command, rest)) if command == "mount" => {
            Ok(DfsCliCommand::Mount(parse_dfs_mount_options(rest)?))
        }
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
            "--cluster" => options.cluster = true,
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

pub(crate) fn parse_dfs_conflicts_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "resolve" => Ok(DfsCliCommand::ResolveConflicts(
            parse_dfs_conflict_resolve_options(rest)?,
        )),
        Some((command, rest)) if command == "versions" => Ok(DfsCliCommand::ConflictVersions(
            parse_dfs_conflict_versions_options(rest)?,
        )),
        Some((command, rest)) if command == "use-version" => Ok(DfsCliCommand::UseConflictVersion(
            parse_dfs_conflict_use_version_options(rest)?,
        )),
        Some((command, rest)) if command == "save-version" => Ok(
            DfsCliCommand::SaveConflictVersion(parse_dfs_conflict_save_version_options(rest)?),
        ),
        _ => Ok(DfsCliCommand::Conflicts(parse_dfs_filter_options(args)?)),
    }
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
            "--limit" => {
                index += 1;
                let Some(limit) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.limit = Some(limit.parse::<usize>().map_err(|source| CliError::Parse {
                    detail: format!("invalid DFS inspection limit {limit}: {source}"),
                })?);
            }
            "--cursor" => {
                index += 1;
                let Some(cursor) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cursor = Some(decode_dfs_inspection_cursor(cursor)?);
            }
            "--json" => options.common.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_dfs_conflict_resolve_options(
    args: &[String],
) -> Result<DfsConflictResolveOptions, CliError> {
    let mut common = DfsCommonOptions::default();
    let mut file_system_id = default_dfs_file_system_id();
    let mut path = None;
    let mut object_id = None;
    let mut conflict_ids = Vec::new();
    let mut delete_file = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--metadata-root" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                common.metadata_root = Some(PathBuf::from(value));
            }
            "--filesystem" | "--fs" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                file_system_id = parse_dfs_file_system_id(value)?;
            }
            "--path" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                path = Some(non_empty_string(value, "path")?);
            }
            "--object" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                object_id = Some(parse_dfs_object_id(value)?);
            }
            "--id" | "--conflict" | "--conflict-id" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                conflict_ids.push(non_empty_string(value, "conflict id")?);
            }
            "--delete-file" => delete_file = true,
            "--json" => common.json = true,
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    if path.is_none() && object_id.is_none() {
        return Err(CliError::Usage);
    }
    Ok(DfsConflictResolveOptions {
        common,
        file_system_id,
        path,
        object_id,
        conflict_ids,
        delete_file,
    })
}

pub(crate) fn parse_dfs_conflict_versions_options(
    args: &[String],
) -> Result<DfsConflictVersionsOptions, CliError> {
    let mut common = DfsCommonOptions::default();
    let mut file_system_id = default_dfs_file_system_id();
    let mut path = None;
    let mut object_id = None;
    let mut conflict_ids = Vec::new();
    parse_dfs_conflict_target_options(
        args,
        &mut common,
        &mut file_system_id,
        &mut path,
        &mut object_id,
        &mut conflict_ids,
        None,
    )?;
    if path.is_none() && object_id.is_none() {
        return Err(CliError::Usage);
    }
    Ok(DfsConflictVersionsOptions {
        common,
        file_system_id,
        path,
        object_id,
        conflict_ids,
    })
}

pub(crate) fn parse_dfs_conflict_use_version_options(
    args: &[String],
) -> Result<DfsConflictUseVersionOptions, CliError> {
    let mut common = DfsCommonOptions::default();
    let mut file_system_id = default_dfs_file_system_id();
    let mut path = None;
    let mut object_id = None;
    let mut conflict_ids = Vec::new();
    let mut version = None;
    parse_dfs_conflict_target_options(
        args,
        &mut common,
        &mut file_system_id,
        &mut path,
        &mut object_id,
        &mut conflict_ids,
        Some(&mut version),
    )?;
    if path.is_none() && object_id.is_none() {
        return Err(CliError::Usage);
    }
    Ok(DfsConflictUseVersionOptions {
        common,
        file_system_id,
        path,
        object_id,
        version: version.ok_or(CliError::Usage)?,
        conflict_ids,
    })
}

pub(crate) fn parse_dfs_conflict_save_version_options(
    args: &[String],
) -> Result<DfsConflictSaveVersionOptions, CliError> {
    let mut common = DfsCommonOptions::default();
    let mut file_system_id = default_dfs_file_system_id();
    let mut path = None;
    let mut object_id = None;
    let mut conflict_ids = Vec::new();
    let mut version = None;
    let mut destination_path = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--to" | "--destination" | "--dest" => {
                index += 1;
                destination_path = Some(non_empty_string(
                    args.get(index).ok_or(CliError::Usage)?,
                    "destination path",
                )?);
            }
            _ => {
                parse_one_dfs_conflict_target_option(
                    args,
                    &mut index,
                    &mut common,
                    &mut file_system_id,
                    &mut path,
                    &mut object_id,
                    &mut conflict_ids,
                    Some(&mut version),
                )?;
            }
        }
        index += 1;
    }
    if path.is_none() && object_id.is_none() {
        return Err(CliError::Usage);
    }
    Ok(DfsConflictSaveVersionOptions {
        common,
        file_system_id,
        path,
        object_id,
        version: version.ok_or(CliError::Usage)?,
        conflict_ids,
        destination_path: destination_path.ok_or(CliError::Usage)?,
    })
}

fn parse_dfs_conflict_target_options(
    args: &[String],
    common: &mut DfsCommonOptions,
    file_system_id: &mut DfsFileSystemId,
    path: &mut Option<String>,
    object_id: &mut Option<DfsObjectId>,
    conflict_ids: &mut Vec<String>,
    mut version: Option<&mut Option<DfsVersion>>,
) -> Result<(), CliError> {
    let mut index = 0;
    while index < args.len() {
        parse_one_dfs_conflict_target_option(
            args,
            &mut index,
            common,
            file_system_id,
            path,
            object_id,
            conflict_ids,
            version.as_deref_mut(),
        )?;
        index += 1;
    }
    Ok(())
}

fn parse_one_dfs_conflict_target_option(
    args: &[String],
    index: &mut usize,
    common: &mut DfsCommonOptions,
    file_system_id: &mut DfsFileSystemId,
    path: &mut Option<String>,
    object_id: &mut Option<DfsObjectId>,
    conflict_ids: &mut Vec<String>,
    version: Option<&mut Option<DfsVersion>>,
) -> Result<(), CliError> {
    match args[*index].as_str() {
        "--metadata-root" => {
            *index += 1;
            let Some(value) = args.get(*index) else {
                return Err(CliError::Usage);
            };
            common.metadata_root = Some(PathBuf::from(value));
        }
        "--filesystem" | "--fs" => {
            *index += 1;
            let Some(value) = args.get(*index) else {
                return Err(CliError::Usage);
            };
            *file_system_id = parse_dfs_file_system_id(value)?;
        }
        "--path" => {
            *index += 1;
            let Some(value) = args.get(*index) else {
                return Err(CliError::Usage);
            };
            *path = Some(non_empty_string(value, "path")?);
        }
        "--object" => {
            *index += 1;
            let Some(value) = args.get(*index) else {
                return Err(CliError::Usage);
            };
            *object_id = Some(parse_dfs_object_id(value)?);
        }
        "--id" | "--conflict" | "--conflict-id" => {
            *index += 1;
            let Some(value) = args.get(*index) else {
                return Err(CliError::Usage);
            };
            conflict_ids.push(non_empty_string(value, "conflict id")?);
        }
        "--version" => {
            let Some(version) = version else {
                return Err(CliError::Usage);
            };
            *index += 1;
            let Some(value) = args.get(*index) else {
                return Err(CliError::Usage);
            };
            *version = Some(parse_dfs_version(value)?);
        }
        "--json" => common.json = true,
        _ => return Err(CliError::Usage),
    }
    Ok(())
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
        Some((command, rest)) if command == "shard" => parse_dfs_metadata_shard_command(rest),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_dfs_metadata_shard_command(args: &[String]) -> Result<DfsCliCommand, CliError> {
    let (command, rest) = args.split_first().ok_or(CliError::Usage)?;
    match command.as_str() {
        "transfer-primary" => {
            let mut file_system_id = default_dfs_file_system_id();
            let mut shard_id = None;
            let mut new_primary_node_id = None;
            let mut expected_epoch = None;
            let mut json = false;
            let mut index = 0;
            while index < rest.len() {
                match rest[index].as_str() {
                    "--filesystem" | "--fs" => {
                        index += 1;
                        file_system_id =
                            parse_dfs_file_system_id(rest.get(index).ok_or(CliError::Usage)?)?;
                    }
                    "--shard" => {
                        index += 1;
                        shard_id = Some(non_empty_string(
                            rest.get(index).ok_or(CliError::Usage)?,
                            "metadata shard id",
                        )?);
                    }
                    "--to" | "--node" | "--primary" => {
                        index += 1;
                        new_primary_node_id =
                            Some(parse_node_id(rest.get(index).ok_or(CliError::Usage)?)?);
                    }
                    "--expected-epoch" | "--epoch" => {
                        index += 1;
                        let value = rest.get(index).ok_or(CliError::Usage)?;
                        expected_epoch =
                            Some(value.parse::<u64>().map_err(|source| CliError::Parse {
                                detail: format!(
                                    "invalid DFS metadata shard epoch `{value}`: {source}"
                                ),
                            })?);
                    }
                    "--json" => json = true,
                    value if !value.starts_with('-') && shard_id.is_none() => {
                        shard_id = Some(non_empty_string(value, "metadata shard id")?);
                    }
                    _ => return Err(CliError::Usage),
                }
                index += 1;
            }
            Ok(DfsCliCommand::MetadataShardTransferPrimary(
                DfsMetadataShardTransferPrimaryOptions {
                    file_system_id,
                    shard_id: shard_id.ok_or(CliError::Usage)?,
                    new_primary_node_id: new_primary_node_id.ok_or(CliError::Usage)?,
                    expected_epoch,
                    json,
                },
            ))
        }
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_dfs_mount_options(args: &[String]) -> Result<DfsMountOptions, CliError> {
    let mut mount_point = None;
    let mut root_path = "/".to_string();
    let mut data_dir = None;
    let mut read_only = false;
    let mut allow_other = false;
    let mut foreground = true;
    let mut sync_local_changes = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--root" | "--path" => {
                index += 1;
                root_path = non_empty_string(args.get(index).ok_or(CliError::Usage)?, "root")?;
            }
            "--data-dir" => {
                index += 1;
                data_dir = Some(PathBuf::from(args.get(index).ok_or(CliError::Usage)?));
            }
            "--read-only" | "--ro" => read_only = true,
            "--allow-other" => allow_other = true,
            "--foreground" => foreground = true,
            "--background" => foreground = false,
            "--sync-local-changes" => sync_local_changes = true,
            "--no-sync-local-changes" => sync_local_changes = false,
            value if value.starts_with('-') => return Err(CliError::Usage),
            value => {
                if mount_point.is_some() {
                    return Err(CliError::Usage);
                }
                mount_point = Some(PathBuf::from(value));
            }
        }
        index += 1;
    }
    Ok(DfsMountOptions {
        mount_point: mount_point.ok_or(CliError::Usage)?,
        root_path,
        data_dir,
        read_only,
        allow_other,
        foreground,
        sync_local_changes,
    })
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
    let mut mode = None;
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
            "--mode" => {
                index += 1;
                mode = Some(parse_dfs_pin_mode(args.get(index).ok_or(CliError::Usage)?)?);
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
        mode,
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

fn run_dfs_inspection(
    options: &DfsCommonOptions,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, CliError> {
    if let Some(metadata_root) = options.metadata_root.clone() {
        return run_dfs_inspection_against_metadata_root(metadata_root, request);
    }
    if let Some(status) = default_main_daemon_status()? {
        return fetch_dfs_inspection_from_daemon_address(
            &status.address,
            &request,
            std::time::Duration::from_secs(10),
        );
    }

    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    sinomo_runtime::dfs::inspect_cluster_aware_dfs(&data_dir, &cluster_paths, request)
        .map_err(Into::into)
}

fn fetch_dfs_inspection_from_daemon_address(
    address: &str,
    request: &DaemonDfsInspectionRequest,
    timeout: std::time::Duration,
) -> Result<DaemonDfsInspectionResponse, CliError> {
    let rpc_request = sinomo_runtime::dfs::dfs_inspection_rpc_request(request)?;
    let response = daemon_send_daemon_rpc(address, &rpc_request, timeout)?;
    sinomo_runtime::dfs::decode_dfs_inspection_rpc_response(&response).map_err(Into::into)
}

fn run_dfs_inspection_against_metadata_root(
    metadata_root: PathBuf,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, CliError> {
    let mut metadata = open_dfs_metadata_for_inspection(Some(metadata_root))?;
    let request = match request {
        DaemonDfsInspectionRequest::ResolveConflicts {
            file_system_id,
            object_id,
            conflict_ids,
        } => {
            return Ok(DaemonDfsInspectionResponse::ResolveConflicts {
                report: sinomo_runtime::dfs::resolve_dfs_conflicts(
                    metadata.backend_mut(),
                    file_system_id.as_ref(),
                    object_id.as_ref(),
                    &conflict_ids,
                )?,
            });
        }
        DaemonDfsInspectionRequest::UseConflictVersion {
            object_id,
            version,
            conflict_ids,
        } => {
            return Ok(DaemonDfsInspectionResponse::UseConflictVersion {
                report: sinomo_runtime::dfs::use_dfs_conflict_version(
                    metadata.backend_mut(),
                    &object_id,
                    &version,
                    &conflict_ids,
                )?,
            });
        }
        request => request,
    };
    let inspector = metadata.inspector();
    Ok(match request {
        DaemonDfsInspectionRequest::Status => DaemonDfsInspectionResponse::Status {
            status: inspector.status()?,
        },
        DaemonDfsInspectionRequest::FileSystem { file_system_id } => {
            DaemonDfsInspectionResponse::FileSystem {
                file_system: inspector.file_system(&file_system_id)?,
            }
        }
        DaemonDfsInspectionRequest::Path {
            file_system_id,
            path,
        } => DaemonDfsInspectionResponse::Path {
            object: inspector.object_by_path(&file_system_id, &path)?,
        },
        DaemonDfsInspectionRequest::Object { object_id } => DaemonDfsInspectionResponse::Object {
            object: inspector.object(&object_id)?,
        },
        DaemonDfsInspectionRequest::Conflicts {
            file_system_id,
            object_id,
            limit,
            cursor,
        } => {
            let page = inspector.conflicts_page(
                file_system_id.as_ref(),
                object_id.as_ref(),
                limit,
                cursor.as_ref(),
            )?;
            DaemonDfsInspectionResponse::Conflicts {
                conflicts: page.conflicts,
                next_cursor: page.next_cursor,
            }
        }
        DaemonDfsInspectionRequest::ResolveConflicts { .. } => unreachable!(),
        DaemonDfsInspectionRequest::ConflictVersions {
            object_id,
            conflict_ids,
        } => DaemonDfsInspectionResponse::ConflictVersions {
            report: inspector.conflict_versions(&object_id, &conflict_ids)?,
        },
        DaemonDfsInspectionRequest::UseConflictVersion { .. } => unreachable!(),
        DaemonDfsInspectionRequest::Repair {
            file_system_id,
            object_id,
            limit,
            cursor,
        } => {
            let page = inspector.repairs_page(
                file_system_id.as_ref(),
                object_id.as_ref(),
                limit,
                cursor.as_ref(),
            )?;
            DaemonDfsInspectionResponse::Repair {
                repairs: page.repairs,
                next_cursor: page.next_cursor,
            }
        }
    })
}

pub(crate) fn run_dfs_cli_command(command: DfsCliCommand) -> Result<(), CliError> {
    match command {
        DfsCliCommand::Help => {
            print_dfs_help();
            Ok(())
        }
        DfsCliCommand::Status(options) => {
            if options.cluster && options.metadata_root.is_some() {
                return Err(CliError::Usage);
            }
            if options.cluster {
                let report = inspect_cluster_dfs_status()?;
                if options.json {
                    print_json("<dfs-cluster-status>", &report)?;
                } else {
                    print_dfs_cluster_status(&report);
                }
                return Ok(());
            }
            let DaemonDfsInspectionResponse::Status { status } =
                run_dfs_inspection(&options, DaemonDfsInspectionRequest::Status)?
            else {
                return Err(CliError::Usage);
            };
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
                print_dfs_repair_attempt_summary("  ", &status);
                println!(
                    "  pending namespace jobs: {}",
                    status.pending_namespace_job_count
                );
                println!(
                    "  failed namespace jobs: {}",
                    status.failed_namespace_job_count
                );
                println!("  open conflicts: {}", status.open_conflict_count);
            }
            Ok(())
        }
        DfsCliCommand::FileSystem {
            file_system_id,
            options,
        } => {
            let DaemonDfsInspectionResponse::FileSystem { file_system } = run_dfs_inspection(
                &options,
                DaemonDfsInspectionRequest::FileSystem {
                    file_system_id: file_system_id.clone(),
                },
            )?
            else {
                return Err(CliError::Usage);
            };
            let Some(file_system) = file_system else {
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
            let DaemonDfsInspectionResponse::Path { object } = run_dfs_inspection(
                &options,
                DaemonDfsInspectionRequest::Path {
                    file_system_id: file_system_id.clone(),
                    path: path.clone(),
                },
            )?
            else {
                return Err(CliError::Usage);
            };
            let Some(object) = object else {
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
            let DaemonDfsInspectionResponse::Object { object } = run_dfs_inspection(
                &options,
                DaemonDfsInspectionRequest::Object {
                    object_id: object_id.clone(),
                },
            )?
            else {
                return Err(CliError::Usage);
            };
            let Some(object) = object else {
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
            let DaemonDfsInspectionResponse::Conflicts {
                conflicts,
                next_cursor,
            } = run_dfs_inspection(
                &options.common,
                DaemonDfsInspectionRequest::Conflicts {
                    file_system_id: options.file_system_id.clone(),
                    object_id: options.object_id.clone(),
                    limit: options.limit,
                    cursor: options.cursor.clone(),
                },
            )?
            else {
                return Err(CliError::Usage);
            };
            let next_cursor = next_cursor
                .as_ref()
                .map(encode_dfs_inspection_cursor)
                .transpose()?;
            if options.common.json {
                print_json(
                    "<dfs-conflicts>",
                    &DfsConflictCliPage {
                        conflicts,
                        next_cursor,
                    },
                )?;
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
                if let Some(next_cursor) = next_cursor {
                    println!("next cursor: {next_cursor}");
                }
            }
            Ok(())
        }
        DfsCliCommand::ResolveConflicts(options) => run_dfs_conflicts_resolve_command(options),
        DfsCliCommand::ConflictVersions(options) => run_dfs_conflicts_versions_command(options),
        DfsCliCommand::UseConflictVersion(options) => {
            run_dfs_conflicts_use_version_command(options)
        }
        DfsCliCommand::SaveConflictVersion(options) => {
            run_dfs_conflicts_save_version_command(options)
        }
        DfsCliCommand::Repair(options) => {
            let DaemonDfsInspectionResponse::Repair {
                repairs,
                next_cursor,
            } = run_dfs_inspection(
                &options.common,
                DaemonDfsInspectionRequest::Repair {
                    file_system_id: options.file_system_id.clone(),
                    object_id: options.object_id.clone(),
                    limit: options.limit,
                    cursor: options.cursor.clone(),
                },
            )?
            else {
                return Err(CliError::Usage);
            };
            let next_cursor = next_cursor
                .as_ref()
                .map(encode_dfs_inspection_cursor)
                .transpose()?;
            if options.common.json {
                print_json(
                    "<dfs-repair>",
                    &DfsRepairCliPage {
                        repairs,
                        next_cursor,
                    },
                )?;
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
                if let Some(next_cursor) = next_cursor {
                    println!("next cursor: {next_cursor}");
                }
            }
            Ok(())
        }
        DfsCliCommand::Smoke { json } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let report = if let Some(status) = default_main_daemon_status()? {
                run_dfs_smoke_via_daemon_address(&status.address, DFS_SMOKE_DAEMON_RPC_TIMEOUT)?
            } else {
                run_local_dfs_smoke(&data_dir)?
            };
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
            let cluster_paths = sinomo_runtime::network::ClusterPaths::new(&data_dir);
            let services = DfsServiceAdvertisements {
                metadata: decode_dfs_advertisements(
                    sinomo_runtime::kv::read_cluster_kv_prefix_entries(
                        &cluster_paths.kv_paths(),
                        sinomo_runtime::dfs::metadata_services_prefix(),
                    )?,
                )?,
                content: decode_dfs_advertisements(
                    sinomo_runtime::kv::read_cluster_kv_prefix_entries(
                        &cluster_paths.kv_paths(),
                        sinomo_runtime::dfs::content_services_prefix(),
                    )?,
                )?,
            };
            if json {
                print_json("<dfs-services>", &services)?;
            } else {
                println!("DFS services:");
                println!("  metadata: {}", services.metadata.len());
                for service in &services.metadata {
                    println!(
                        "    {} metadata={} content={}",
                        service.node_id, service.accepts_metadata, service.accepts_content
                    );
                }
                println!("  content: {}", services.content.len());
                for service in &services.content {
                    println!(
                        "    {} metadata={} content={}",
                        service.node_id, service.accepts_metadata, service.accepts_content
                    );
                }
            }
            Ok(())
        }
        DfsCliCommand::MetadataPublish { json } => {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            let report = publish_dfs_metadata_journal_from_data_dir(&data_dir)?;
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
        DfsCliCommand::MetadataShardTransferPrimary(options) => {
            run_dfs_metadata_shard_transfer_primary(options)
        }
        DfsCliCommand::Mount(options) => run_dfs_mount_command(options),
        DfsCliCommand::Pin(command) => run_dfs_pin_command(command),
        DfsCliCommand::Expose(command) => run_dfs_expose_command(command),
    }
}

fn run_dfs_metadata_shard_transfer_primary(
    options: DfsMetadataShardTransferPrimaryOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut kv = CliDfsControlPlaneKvClient::new(&cluster_paths);
    let mut store = sinomo_runtime::dfs::DfsControlPlaneStore::new(&mut kv);
    let previous = store
        .get_metadata_shard(&options.file_system_id, &options.shard_id)?
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "DFS metadata shard {} for filesystem {} was not found",
                options.shard_id, options.file_system_id
            ),
        })?;

    if let Some(expected_epoch) = options.expected_epoch {
        if previous.epoch != expected_epoch {
            return Err(CliError::ClusterReadOnly {
                detail: format!(
                    "DFS metadata shard {} epoch is {}, expected {}",
                    options.shard_id, previous.epoch, expected_epoch
                ),
            });
        }
    }

    if previous.primary_node_id == options.new_primary_node_id {
        let report = DfsMetadataShardTransferPrimaryReport {
            file_system_id: options.file_system_id,
            shard_id: options.shard_id,
            previous_primary_node_id: previous.primary_node_id.clone(),
            new_primary_node_id: previous.primary_node_id.clone(),
            previous_epoch: previous.epoch,
            new_epoch: previous.epoch,
            changed: false,
            shard: previous,
        };
        print_dfs_metadata_shard_transfer_primary_report(&report, options.json)?;
        return Ok(());
    }

    if !previous
        .replica_node_ids
        .iter()
        .any(|node_id| node_id == &options.new_primary_node_id)
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "node {} is not a replica of DFS metadata shard {}; add metadata replication before promoting it",
                options.new_primary_node_id, options.shard_id
            ),
        });
    }

    let expected_epoch = previous.epoch;
    let updated = store.transfer_metadata_shard_primary(
        &options.file_system_id,
        &options.shard_id,
        expected_epoch,
        options.new_primary_node_id.clone(),
    )?;
    let Some(updated) = updated else {
        let current = store.get_metadata_shard(&options.file_system_id, &options.shard_id)?;
        let current_epoch = current
            .as_ref()
            .map(|shard| shard.epoch.to_string())
            .unwrap_or_else(|| "missing".to_string());
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "DFS metadata shard {} changed while transferring primary; expected epoch {}, current epoch {}",
                options.shard_id, expected_epoch, current_epoch
            ),
        });
    };

    let report = DfsMetadataShardTransferPrimaryReport {
        file_system_id: options.file_system_id,
        shard_id: options.shard_id,
        previous_primary_node_id: previous.primary_node_id,
        new_primary_node_id: updated.primary_node_id.clone(),
        previous_epoch: expected_epoch,
        new_epoch: updated.epoch,
        changed: true,
        shard: updated,
    };
    print_dfs_metadata_shard_transfer_primary_report(&report, options.json)?;
    Ok(())
}

fn resolve_dfs_conflict_target(
    common: &DfsCommonOptions,
    file_system_id: &DfsFileSystemId,
    path: Option<&String>,
    explicit_object_id: Option<&DfsObjectId>,
) -> Result<(DfsObjectId, Option<String>), CliError> {
    if let Some(requested_path) = path {
        let DaemonDfsInspectionResponse::Path { object } = run_dfs_inspection(
            common,
            DaemonDfsInspectionRequest::Path {
                file_system_id: file_system_id.clone(),
                path: requested_path.clone(),
            },
        )?
        else {
            return Err(CliError::Usage);
        };
        let Some(object) = object else {
            return Err(CliError::MissingArtifact {
                artifact: format!("DFS path {requested_path} in filesystem {file_system_id}"),
            });
        };
        if let Some(explicit_object_id) = explicit_object_id
            && explicit_object_id != &object.object.object_id
        {
            return Err(CliError::Parse {
                detail: format!(
                    "DFS path {} resolved to object {}, not requested object {}",
                    requested_path, object.object.object_id, explicit_object_id
                ),
            });
        }
        return Ok((object.object.object_id.clone(), Some(object.object.path)));
    }

    let object_id = explicit_object_id.cloned().ok_or(CliError::Usage)?;
    Ok((object_id, None))
}

fn run_dfs_conflicts_resolve_command(options: DfsConflictResolveOptions) -> Result<(), CliError> {
    if options.common.metadata_root.is_some() && options.delete_file {
        return Err(CliError::Parse {
            detail: "--delete-file cannot be used with --metadata-root".to_string(),
        });
    }

    let mut object_id = options.object_id.clone();
    let mut path = options.path.clone();
    if let Some(requested_path) = options.path.as_ref() {
        let DaemonDfsInspectionResponse::Path { object } = run_dfs_inspection(
            &options.common,
            DaemonDfsInspectionRequest::Path {
                file_system_id: options.file_system_id.clone(),
                path: requested_path.clone(),
            },
        )?
        else {
            return Err(CliError::Usage);
        };
        let Some(object) = object else {
            return Err(CliError::MissingArtifact {
                artifact: format!(
                    "DFS path {} in filesystem {}",
                    requested_path, options.file_system_id
                ),
            });
        };
        if let Some(explicit_object_id) = object_id.as_ref()
            && explicit_object_id != &object.object.object_id
        {
            return Err(CliError::Parse {
                detail: format!(
                    "DFS path {} resolved to object {}, not requested object {}",
                    requested_path, object.object.object_id, explicit_object_id
                ),
            });
        }
        object_id = Some(object.object.object_id.clone());
        path = Some(object.object.path.clone());
    }

    let object_id = object_id.ok_or(CliError::Usage)?;
    if options.delete_file && path.is_none() {
        let DaemonDfsInspectionResponse::Object { object } = run_dfs_inspection(
            &options.common,
            DaemonDfsInspectionRequest::Object {
                object_id: object_id.clone(),
            },
        )?
        else {
            return Err(CliError::Usage);
        };
        let Some(object) = object else {
            return Err(CliError::MissingArtifact {
                artifact: format!("DFS object {object_id}"),
            });
        };
        path = Some(object.object.path.clone());
    }

    let DaemonDfsInspectionResponse::ResolveConflicts { report } = run_dfs_inspection(
        &options.common,
        DaemonDfsInspectionRequest::ResolveConflicts {
            file_system_id: Some(options.file_system_id.clone()),
            object_id: Some(object_id.clone()),
            conflict_ids: options.conflict_ids.clone(),
        },
    )?
    else {
        return Err(CliError::Usage);
    };

    let mut deleted_file = false;
    if options.delete_file {
        let Some(path_to_delete) = path.as_ref() else {
            return Err(CliError::Parse {
                detail: "cannot delete DFS file without a resolved path".to_string(),
            });
        };
        delete_dfs_file_for_conflict_resolution(path_to_delete)?;
        deleted_file = true;
    }

    if options.common.json {
        print_json(
            "<dfs-conflicts-resolve>",
            &DfsConflictResolveCliReport {
                file_system_id: options.file_system_id,
                object_id,
                path,
                deleted_file,
                removed: report.removed,
                not_found_conflict_ids: report.not_found_conflict_ids,
            },
        )?;
    } else {
        if report.removed.is_empty() {
            println!("No matching DFS conflicts.");
        } else {
            println!("Resolved {} DFS conflict record(s).", report.removed.len());
            for conflict in &report.removed {
                println!(
                    "  {} object={} path={}",
                    conflict.conflict_id, conflict.object_id, conflict.path
                );
            }
        }
        if deleted_file && let Some(path) = path.as_ref() {
            println!("Deleted DFS file: {path}");
        }
        if !report.not_found_conflict_ids.is_empty() {
            println!(
                "Conflict id(s) not found: {}",
                report.not_found_conflict_ids.join(", ")
            );
        }
    }
    Ok(())
}

fn run_dfs_conflicts_versions_command(options: DfsConflictVersionsOptions) -> Result<(), CliError> {
    let (object_id, _path) = resolve_dfs_conflict_target(
        &options.common,
        &options.file_system_id,
        options.path.as_ref(),
        options.object_id.as_ref(),
    )?;
    let DaemonDfsInspectionResponse::ConflictVersions { report } = run_dfs_inspection(
        &options.common,
        DaemonDfsInspectionRequest::ConflictVersions {
            object_id: object_id.clone(),
            conflict_ids: options.conflict_ids.clone(),
        },
    )?
    else {
        return Err(CliError::Usage);
    };
    let Some(report) = report else {
        return Err(CliError::MissingArtifact {
            artifact: format!("DFS object {object_id}"),
        });
    };
    if options.common.json {
        print_json("<dfs-conflict-versions>", &report)
    } else {
        print_dfs_conflict_versions_report(&report);
        Ok(())
    }
}

fn run_dfs_conflicts_use_version_command(
    options: DfsConflictUseVersionOptions,
) -> Result<(), CliError> {
    let (object_id, path) = resolve_dfs_conflict_target(
        &options.common,
        &options.file_system_id,
        options.path.as_ref(),
        options.object_id.as_ref(),
    )?;
    let DaemonDfsInspectionResponse::UseConflictVersion { report } = run_dfs_inspection(
        &options.common,
        DaemonDfsInspectionRequest::UseConflictVersion {
            object_id: object_id.clone(),
            version: options.version.clone(),
            conflict_ids: options.conflict_ids.clone(),
        },
    )?
    else {
        return Err(CliError::Usage);
    };

    if options.common.json {
        print_json(
            "<dfs-conflicts-use-version>",
            &DfsConflictUseVersionCliReport {
                file_system_id: options.file_system_id,
                object_id,
                path,
                version: options.version,
                changed: report.changed,
                removed: report.removed,
                not_found_conflict_ids: report.not_found_conflict_ids,
            },
        )?;
    } else {
        if report.changed {
            println!(
                "DFS object {} now uses version {}.",
                report.committed_object.object_id, report.committed_object.version
            );
        } else {
            println!(
                "DFS object {} already used version {}.",
                report.committed_object.object_id, report.committed_object.version
            );
        }
        if report.removed.is_empty() {
            println!("No matching DFS conflict records were removed.");
        } else {
            println!("Removed {} DFS conflict record(s).", report.removed.len());
            for conflict in &report.removed {
                println!("  {}", conflict.conflict_id);
            }
        }
        if !report.not_found_conflict_ids.is_empty() {
            println!(
                "Conflict id(s) not found: {}",
                report.not_found_conflict_ids.join(", ")
            );
        }
    }
    Ok(())
}

fn run_dfs_conflicts_save_version_command(
    options: DfsConflictSaveVersionOptions,
) -> Result<(), CliError> {
    if options.common.metadata_root.is_some() {
        return Err(CliError::Parse {
            detail: "conflicts save-version needs DFS content and cannot use --metadata-root"
                .to_string(),
        });
    }
    let (object_id, source_path) = resolve_dfs_conflict_target(
        &options.common,
        &options.file_system_id,
        options.path.as_ref(),
        options.object_id.as_ref(),
    )?;
    let version = find_dfs_conflict_version(
        &options.common,
        &object_id,
        &options.version,
        options.conflict_ids.clone(),
    )?;
    let content_hash = version
        .content_hash
        .clone()
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "DFS object {object_id} version {} does not have file content",
                options.version
            ),
        })?;
    let bytes = read_dfs_object_version_content(
        &object_id,
        &options.version,
        &content_hash,
        version.size_bytes,
    )?;
    write_dfs_file_for_conflict_version(&options.destination_path, &bytes)?;
    if options.common.json {
        print_json(
            "<dfs-conflicts-save-version>",
            &DfsConflictSaveVersionCliReport {
                file_system_id: options.file_system_id,
                object_id,
                source_path,
                version: options.version,
                destination_path: options.destination_path,
                bytes_written: bytes.len(),
            },
        )?;
    } else {
        println!(
            "Saved DFS object {} version {} to {} ({} bytes).",
            object_id,
            options.version,
            options.destination_path,
            bytes.len()
        );
    }
    Ok(())
}

fn find_dfs_conflict_version(
    common: &DfsCommonOptions,
    object_id: &DfsObjectId,
    version: &DfsVersion,
    conflict_ids: Vec<String>,
) -> Result<DfsOperatorConflictVersionView, CliError> {
    let DaemonDfsInspectionResponse::ConflictVersions { report } = run_dfs_inspection(
        common,
        DaemonDfsInspectionRequest::ConflictVersions {
            object_id: object_id.clone(),
            conflict_ids,
        },
    )?
    else {
        return Err(CliError::Usage);
    };
    let Some(report) = report else {
        return Err(CliError::MissingArtifact {
            artifact: format!("DFS object {object_id}"),
        });
    };
    report
        .versions
        .into_iter()
        .find(|candidate| &candidate.version == version)
        .ok_or_else(|| CliError::MissingArtifact {
            artifact: format!("DFS object {object_id} version {version}"),
        })
}

fn read_dfs_object_version_content(
    object_id: &DfsObjectId,
    version: &DfsVersion,
    content_hash: &str,
    size_bytes: Option<u64>,
) -> Result<Vec<u8>, CliError> {
    if let Some(status) = default_main_daemon_status()? {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::daemon::DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::daemon::DaemonDfsBrowserReadObjectVersionRequest {
                object_id: object_id.to_string(),
                version: version.to_string(),
                content_hash: content_hash.to_string(),
                size_bytes,
            },
        )?;
        let response = daemon_send_daemon_rpc(
            &status.address,
            &request,
            std::time::Duration::from_secs(30),
        )?;
        let response: sinomo_runtime::daemon::DaemonDfsBrowserReadObjectVersionResponse =
            response.decode_body()?;
        return Ok(response.bytes);
    }

    let opened = sinomo_runtime::dfs::open_mounted_sifs_service(None)?;
    opened
        .service
        .read_object_version_content(object_id, version, content_hash, size_bytes)
        .map_err(|source| CliError::Parse {
            detail: format!("cannot read DFS object {object_id} version {version}: {source}"),
        })
}

fn write_dfs_file_for_conflict_version(path: &str, bytes: &[u8]) -> Result<(), CliError> {
    if let Some(status) = default_main_daemon_status()? {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::daemon::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::daemon::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        let response = daemon_send_daemon_rpc(
            &status.address,
            &request,
            std::time::Duration::from_secs(30),
        )?;
        let _: sinomo_runtime::daemon::DaemonDfsBrowserWriteFileResponse =
            response.decode_body()?;
        return Ok(());
    }

    let mut opened = sinomo_runtime::dfs::open_mounted_sifs_service(None)?;
    if let Some(parent) = dfs_parent_path(path) {
        opened
            .service
            .posix_mkdir_all(&parent)
            .map_err(|source| CliError::Parse {
                detail: format!("cannot create DFS directory {parent}: {source}"),
            })?;
    }
    let mut candidates = opened.replica_candidates.clone();
    if !candidates.contains(opened.service.node_id()) {
        candidates.insert(0, opened.service.node_id().clone());
    }
    let mut write_options = sinomo_runtime::dfs::DfsWriteOptions::replicated_eventual(candidates);
    write_options.foreground_replica_repair = false;
    opened
        .service
        .posix_write_file(path, bytes, write_options)
        .map(drop)
        .map_err(|source| CliError::Parse {
            detail: format!("cannot write DFS file {path}: {source}"),
        })
}

fn dfs_parent_path(path: &str) -> Option<String> {
    let normalized = path.trim();
    let index = normalized.rfind('/')?;
    if index == 0 {
        Some("/".to_string())
    } else {
        Some(normalized[..index].to_string())
    }
}

fn delete_dfs_file_for_conflict_resolution(path: &str) -> Result<(), CliError> {
    if let Some(status) = default_main_daemon_status()? {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::daemon::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::daemon::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory: false,
            },
        )?;
        let response = daemon_send_daemon_rpc(
            &status.address,
            &request,
            std::time::Duration::from_secs(10),
        )?;
        let _: sinomo_runtime::daemon::DaemonDfsBrowserDeleteResponse = response.decode_body()?;
        return Ok(());
    }

    let mut opened = sinomo_runtime::dfs::open_mounted_sifs_service(None)?;
    opened
        .service
        .posix_unlink(path)
        .map(drop)
        .map_err(|source| CliError::Parse {
            detail: format!("cannot delete DFS file {path}: {source}"),
        })
}

fn print_dfs_metadata_shard_transfer_primary_report(
    report: &DfsMetadataShardTransferPrimaryReport,
    json: bool,
) -> Result<(), CliError> {
    if json {
        return print_json("<dfs-metadata-shard-transfer-primary>", report);
    }
    if report.changed {
        println!(
            "DFS metadata shard primary transferred: {} shard={} {} -> {} epoch {} -> {}",
            report.file_system_id,
            report.shard_id,
            report.previous_primary_node_id,
            report.new_primary_node_id,
            report.previous_epoch,
            report.new_epoch
        );
    } else {
        println!(
            "DFS metadata shard primary unchanged: {} shard={} primary={} epoch={}",
            report.file_system_id, report.shard_id, report.new_primary_node_id, report.new_epoch
        );
    }
    Ok(())
}

fn run_dfs_smoke_via_daemon_address(
    address: &str,
    timeout: std::time::Duration,
) -> Result<sinomo_runtime::dfs::DfsSmokeReport, CliError> {
    let rpc_request = sinomo_runtime::dfs::dfs_smoke_rpc_request()?;
    let response = daemon_send_daemon_rpc(address, &rpc_request, timeout)?;
    sinomo_runtime::dfs::decode_dfs_smoke_rpc_response(&response).map_err(Into::into)
}

#[derive(Clone, Debug)]
struct CliDfsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
}

impl CliDfsControlPlaneKvClient {
    fn new(cluster_paths: &ClusterPaths) -> Self {
        Self {
            cluster_paths: cluster_paths.clone(),
        }
    }

    fn kv_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl sinomo_runtime::dfs::DfsControlPlaneKvClient for CliDfsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<sinomo_runtime::kv::KvEntry>, KvError> {
        sinomo_runtime::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<sinomo_runtime::kv::KvEntry>, KvError> {
        sinomo_runtime::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_runtime::kv::KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let mut state = cluster::read_cluster_state(&self.cluster_paths)
            .map_err(|source| self.kv_error(source))?;
        let result =
            cli_commit_cluster_kv_txn_with_quorum_response(&self.cluster_paths, &mut state, txn)
                .map_err(|source| self.kv_error(source))?;
        Ok(result.txn_response.unwrap_or(KvTxnResponse {
            succeeded: true,
            revision: result.snapshot.revision,
            responses: Vec::new(),
        }))
    }
}

fn run_local_dfs_smoke(
    data_dir: &SinomoDataDir,
) -> Result<sinomo_runtime::dfs::DfsSmokeReport, CliError> {
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let control_plane = cluster_paths.kv_paths().snapshot.exists().then(|| {
        Box::new(CliDfsControlPlaneKvClient::new(&cluster_paths))
            as Box<dyn sinomo_runtime::dfs::DfsControlPlaneKvClient>
    });
    let Some(report) = sinomo_runtime::dfs::run_default_cluster_dfs_smoke_with_peer_transport(
        data_dir,
        &cluster_paths,
        cli_dfs_peer_transport(),
        Box::new(CliDfsControlPlaneKvClient::new(&cluster_paths)),
        control_plane,
        sinomo_runtime::dfs::DfsDefaultClusterSmokeOptions {
            bootstrap: sinomo_runtime::dfs::DfsDefaultClusterBootstrapOptions {
                publish_cluster_control: false,
                materialize_local_metadata: false,
            },
            smoke: foreground_dfs_smoke_options(),
        },
    )?
    else {
        return Err(CliError::ClusterOrphan);
    };
    Ok(report)
}

fn foreground_dfs_smoke_options() -> sinomo_runtime::dfs::DfsSmokeOptions {
    sinomo_runtime::dfs::DfsSmokeOptions {
        stable_path: true,
        remote_replica_candidates: false,
        foreground_replica_repair: false,
        inspect_pending_repairs: false,
        publish_metadata: false,
        metadata_replication: sinomo_runtime::dfs::DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 0,
            ..sinomo_runtime::dfs::DfsMetadataJournalReplicationOptions::default()
        },
        ..sinomo_runtime::dfs::DfsSmokeOptions::default()
    }
}

fn cli_dfs_peer_transport() -> std::sync::Arc<dyn sinomo_runtime::dfs::DfsPeerTransport> {
    std::sync::Arc::new(CliDfsPeerTransport)
}

#[derive(Clone, Debug)]
struct CliDfsPeerTransport;

impl sinomo_runtime::dfs::DfsPeerTransport for CliDfsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &sinomo_runtime::node::NodeId,
        request: &sinomo_runtime::network::ObjectRpcRequest,
        options: sinomo_runtime::dfs::DfsObjectRpcSendOptions,
    ) -> Result<
        sinomo_runtime::network::ObjectRpcResponse,
        sinomo_runtime::dfs::DfsPeerTransportError,
    > {
        daemon_dfs_object_rpc_to_target(target_node_id, request, options)
            .map_err(|error| sinomo_runtime::dfs::DfsPeerTransportError::new(error.to_string()))
    }
}

fn daemon_dfs_object_rpc_to_target(
    target_node_id: &sinomo_runtime::node::NodeId,
    request: &sinomo_runtime::network::ObjectRpcRequest,
    options: sinomo_runtime::dfs::DfsObjectRpcSendOptions,
) -> Result<sinomo_runtime::network::ObjectRpcResponse, CliError> {
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::Network {
        address: target_node_id.to_string(),
        detail: "DFS cluster object RPC requires the local daemon to be running".to_string(),
    })?;
    DaemonApiClient::at_address(status.address)
        .dfs_object_rpc_to_target_with_options(target_node_id, request, options)
        .map_err(CliError::from)
}

fn publish_dfs_metadata_journal_from_data_dir(
    data_dir: &SinomoDataDir,
) -> Result<sinomo_runtime::dfs::DfsMetadataJournalReplicationReport, CliError> {
    let cluster_paths = sinomo_runtime::network::ClusterPaths::new(data_dir);
    Ok(
        sinomo_runtime::dfs::publish_cluster_dfs_metadata_journal_with_peer_transport(
            data_dir,
            &cluster_paths,
            cli_dfs_peer_transport(),
            sinomo_runtime::dfs::DfsMetadataJournalReplicationOptions::default(),
        )?,
    )
}

pub(crate) fn run_dfs_mount_command(options: DfsMountOptions) -> Result<(), CliError> {
    #[cfg(windows)]
    {
        run_dfs_cloud_files_mount_command(options)
    }
    #[cfg(all(not(windows), feature = "dfs-fuse"))]
    {
        let mut mount_options =
            sinomo_dfs_fuse::DfsFuseMountOptions::new(options.mount_point.clone())
                .with_root_path(options.root_path);
        if let Some(data_dir) = options.data_dir {
            mount_options = mount_options.with_data_dir(data_dir);
        }
        mount_options.read_only = options.read_only;
        mount_options.allow_other = options.allow_other;
        mount_options.foreground = options.foreground;
        sinomo_dfs_fuse::mount_sifs(mount_options).map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })
    }
    #[cfg(all(not(windows), not(feature = "dfs-fuse")))]
    {
        let _ = options;
        Err(CliError::Parse {
            detail: "SIFS FUSE support is disabled in this build; use a default Unix build or rebuild with `--features dfs-fuse`".to_string(),
        })
    }
}

#[cfg(windows)]
fn run_dfs_cloud_files_mount_command(options: DfsMountOptions) -> Result<(), CliError> {
    use sinomo_runtime::dfs_cloud_files_api::{
        CloudFilesModuleRequest, CloudFilesModuleResponse, CloudFilesMountRequest,
        SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
        SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
    };
    use sinomo_runtime::module::{
        ModuleId, NativeModule, native_module_cache_path, read_native_dylib_module_loader_state,
    };
    use sinomo_runtime::module_api::{
        NativeModuleInvocationRequest, NativeModuleRequest, NativeModuleResponse, decode_sobj,
        encode_sobj,
    };

    if options.allow_other {
        return Err(CliError::Parse {
            detail: "`--allow-other` is only supported by the Unix FUSE backend".to_string(),
        });
    }
    let data_dir = match options.data_dir.clone() {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home()?,
    };
    data_dir.ensure_root_layout()?;
    let data_dir_root = data_dir.root();
    let module_id =
        ModuleId::from_legacy_named(SINOMO_DFS_CLOUD_FILES_MODULE_ID).map_err(|source| {
            CliError::Parse {
                detail: format!("invalid Cloud Files module id: {source}"),
            }
        })?;
    let state = read_native_dylib_module_loader_state(&data_dir_root, &module_id)
        .map_err(|source| CliError::Parse {
            detail: format!("read Cloud Files native module loader state: {source}"),
        })?
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "Cloud Files native module is not active; run `sinomo install` or activate `{SINOMO_DFS_CLOUD_FILES_MODULE_ID}`"
            ),
        })?;
    if state.interface_version != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION {
        return Err(CliError::Parse {
            detail: format!(
                "Cloud Files native module interface mismatch: expected {}, got {}",
                SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, state.interface_version
            ),
        });
    }
    let library_path = native_module_cache_path(
        &data_dir_root,
        &state.cache_key,
        &state.content_hash,
        &state.library_name,
    );
    let module = unsafe {
        NativeModule::load_from_content_addressed_path(&library_path, &state.content_hash)
    }
    .map_err(|source| CliError::Parse {
        detail: format!("load Cloud Files native module: {source}"),
    })?;
    let request = CloudFilesMountRequest {
        sync_root: options.mount_point,
        root_path: options.root_path,
        data_dir_root: Some(data_dir_root),
        read_only: options.read_only,
        sync_local_changes: options.sync_local_changes,
        provider_name: "Sinomo SIFS".to_string(),
        provider_version: env!("CARGO_PKG_VERSION").to_string(),
        prepopulate_depth: 1,
        connect: options.foreground,
        max_run_seconds: None,
    };
    let payload_sobj = encode_sobj(&CloudFilesModuleRequest::Mount(request)).map_err(|source| {
        CliError::Parse {
            detail: format!("encode Cloud Files mount request: {source}"),
        }
    })?;
    let native_request = NativeModuleRequest::Invoke(NativeModuleInvocationRequest {
        operation: SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION.to_string(),
        payload_sobj,
    });
    match module
        .invoke(&native_request)
        .map_err(|source| CliError::Parse {
            detail: format!("invoke Cloud Files native module: {source}"),
        })? {
        NativeModuleResponse::Invocation(response)
            if response.operation == SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION =>
        {
            match decode_sobj::<CloudFilesModuleResponse>(&response.payload_sobj).map_err(
                |source| CliError::Parse {
                    detail: format!("decode Cloud Files mount response: {source}"),
                },
            )? {
                CloudFilesModuleResponse::Mounted(response) => {
                    println!(
                        "SIFS Cloud Files sync root {} -> {} ({})",
                        response.sync_root.display(),
                        response.root_path,
                        response.detail
                    );
                    Ok(())
                }
                CloudFilesModuleResponse::Error(error) => Err(CliError::Parse {
                    detail: format!("Cloud Files mount failed: {}: {}", error.code, error.detail),
                }),
            }
        }
        NativeModuleResponse::Error(error) => Err(CliError::Parse {
            detail: format!("Cloud Files mount failed: {}: {}", error.code, error.detail),
        }),
        other => Err(CliError::Parse {
            detail: format!("unexpected Cloud Files native module response: {other:?}"),
        }),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsServiceAdvertisements {
    metadata: Vec<sinomo_runtime::dfs::DfsNodeAdvertisement>,
    content: Vec<sinomo_runtime::dfs::DfsNodeAdvertisement>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsClusterStatusReport {
    pub(crate) nodes: Vec<DfsClusterNodeStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsClusterNodeStatus {
    pub(crate) node_id: NodeId,
    pub(crate) local: bool,
    pub(crate) endpoint: Option<String>,
    pub(crate) status: Option<sinomo_runtime::dfs::DfsOperatorStatus>,
    pub(crate) error: Option<String>,
}

pub(crate) fn inspect_cluster_dfs_status() -> Result<DfsClusterStatusReport, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    inspect_cluster_dfs_status_with_data_dir(data_dir)
}

pub(crate) fn inspect_cluster_dfs_status_with_data_dir(
    data_dir: SinomoDataDir,
) -> Result<DfsClusterStatusReport, CliError> {
    let trace = dfs_cluster_status_trace_enabled();
    let started_at = std::time::Instant::now();
    trace_dfs_cluster_status(trace, started_at, "start");
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = read_cluster_state(&cluster_paths)?;
    trace_dfs_cluster_status(
        trace,
        started_at,
        format_args!(
            "loaded cluster state local={} members={}",
            state.local_node_id,
            state.members.len()
        ),
    );
    let local_status_started_at = std::time::Instant::now();
    let local_status =
        sinomo_runtime::dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)?;
    trace_dfs_cluster_status(
        trace,
        started_at,
        format_args!(
            "local status complete in {}ms",
            local_status_started_at.elapsed().as_millis()
        ),
    );
    let mut nodes = vec![DfsClusterNodeStatus {
        node_id: state.local_node_id.clone(),
        local: true,
        endpoint: Some("local-daemon".to_string()),
        status: Some(local_status),
        error: None,
    }];
    let remote_node_ids = dfs_cluster_status_remote_main_node_ids(&state);
    trace_dfs_cluster_status(
        trace,
        started_at,
        format_args!(
            "remote candidates={} bounded_probes={}",
            remote_node_ids.len(),
            remote_node_ids.len()
        ),
    );
    let remote_timeout = DFS_CLUSTER_STATUS_REMOTE_TIMEOUT;
    let mut probes = Vec::new();
    for node_id in remote_node_ids {
        if let Some(error) =
            dfs_cluster_status_known_unavailable_node_error(&cluster_paths, &state, &node_id)
        {
            nodes.push(DfsClusterNodeStatus {
                node_id,
                local: false,
                endpoint: None,
                status: None,
                error: Some(error),
            });
            continue;
        }
        probes.push(spawn_dfs_status_probe(
            &cluster_paths,
            &state,
            &node_id,
            remote_timeout,
            trace,
        )?);
    }
    for probe in probes {
        let node_id = probe.node_id.clone();
        nodes.push(match receive_dfs_status_probe(probe, trace, started_at) {
            Ok(status) => DfsClusterNodeStatus {
                node_id,
                local: false,
                endpoint: Some("cluster-object-rpc".to_string()),
                status: Some(status),
                error: None,
            },
            Err(error) => DfsClusterNodeStatus {
                node_id,
                local: false,
                endpoint: None,
                status: None,
                error: Some(error.to_string()),
            },
        });
    }
    trace_dfs_cluster_status(
        trace,
        started_at,
        format_args!("complete nodes={}", nodes.len()),
    );
    Ok(DfsClusterStatusReport { nodes })
}

fn dfs_cluster_status_remote_main_node_ids(
    state: &sinomo_runtime::core::ClusterBootstrapState,
) -> std::collections::BTreeSet<NodeId> {
    state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && member.capabilities.iter().any(|capability| {
                    capability.as_str() == sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY
                })
        })
        .map(|member| member.node_id.clone())
        .collect()
}

fn dfs_cluster_status_known_unavailable_node_error(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    node_id: &NodeId,
) -> Option<String> {
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)?;
    match sinomo_runtime::network::cluster_member_main_daemon_connectivity(
        cluster_paths,
        state,
        member,
    ) {
        sinomo_runtime::network::ClusterMemberMainDaemonConnectivity::Healthy
        | sinomo_runtime::network::ClusterMemberMainDaemonConnectivity::Unknown => None,
        sinomo_runtime::network::ClusterMemberMainDaemonConnectivity::Degraded => Some(
            "node has no currently healthy main-daemon route according to cluster connectivity probes"
                .to_string(),
        ),
        sinomo_runtime::network::ClusterMemberMainDaemonConnectivity::Unreachable => Some(
            "node is currently unavailable according to cluster connectivity probes".to_string(),
        ),
    }
}

struct DfsStatusProbe {
    node_id: NodeId,
    started_at: std::time::Instant,
    timeout: std::time::Duration,
    receiver: std::sync::mpsc::Receiver<Result<sinomo_runtime::dfs::DfsOperatorStatus, CliError>>,
}

fn spawn_dfs_status_probe(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    node_id: &NodeId,
    timeout: std::time::Duration,
    trace: bool,
) -> Result<DfsStatusProbe, CliError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let cluster_paths = cluster_paths.clone();
    let state = state.clone();
    let node_id = node_id.clone();
    let worker_node_id = node_id.clone();
    let node_id_label = node_id.to_string();
    let started_at = std::time::Instant::now();
    let thread_name = format!("sinomo-dfs-status-{node_id}");
    std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            if trace {
                eprintln!("sinomo dfs cluster status node={worker_node_id}: probe start");
            }
            let result = fetch_dfs_status_from_node_blocking(
                &cluster_paths,
                &state,
                &worker_node_id,
                timeout,
            );
            if trace {
                eprintln!(
                    "sinomo dfs cluster status node={worker_node_id}: probe done in {}ms",
                    started_at.elapsed().as_millis()
                );
            }
            let _ = sender.send(result);
        })
        .map_err(|source| CliError::Network {
            address: format!("cluster dfs status {node_id_label}"),
            detail: format!("failed to start probe thread: {source}"),
        })?;
    Ok(DfsStatusProbe {
        node_id,
        started_at,
        timeout,
        receiver,
    })
}

fn receive_dfs_status_probe(
    probe: DfsStatusProbe,
    trace: bool,
    trace_started_at: std::time::Instant,
) -> Result<sinomo_runtime::dfs::DfsOperatorStatus, CliError> {
    let remaining = probe
        .timeout
        .checked_sub(probe.started_at.elapsed())
        .unwrap_or_default();
    let result = if remaining.is_zero() {
        probe.receiver.try_recv().map_err(|source| match source {
            std::sync::mpsc::TryRecvError::Empty => std::sync::mpsc::RecvTimeoutError::Timeout,
            std::sync::mpsc::TryRecvError::Disconnected => {
                std::sync::mpsc::RecvTimeoutError::Disconnected
            }
        })
    } else {
        probe.receiver.recv_timeout(remaining)
    };
    let result = match result {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(CliError::Network {
            address: format!("cluster dfs status {}", probe.node_id),
            detail: format!("timed out after {}ms", probe.timeout.as_millis()),
        }),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(CliError::Network {
            address: format!("cluster dfs status {}", probe.node_id),
            detail: "probe thread exited without a result".to_string(),
        }),
    };
    if trace {
        let outcome = if result.is_ok() { "ok" } else { "error" };
        trace_dfs_cluster_status(
            true,
            trace_started_at,
            format_args!(
                "probe result node={} outcome={} elapsed={}ms",
                probe.node_id,
                outcome,
                probe.started_at.elapsed().as_millis()
            ),
        );
    }
    result
}

fn dfs_cluster_status_trace_enabled() -> bool {
    std::env::var_os("SINOMO_TRACE_DFS_CLUSTER_STATUS").is_some()
}

fn trace_dfs_cluster_status(
    enabled: bool,
    started_at: std::time::Instant,
    message: impl std::fmt::Display,
) {
    if enabled {
        eprintln!(
            "sinomo dfs cluster status +{}ms: {message}",
            started_at.elapsed().as_millis()
        );
    }
}

fn fetch_dfs_status_from_node_blocking(
    _cluster_paths: &ClusterPaths,
    _state: &sinomo_runtime::core::ClusterBootstrapState,
    node_id: &NodeId,
    timeout: std::time::Duration,
) -> Result<sinomo_runtime::dfs::DfsOperatorStatus, CliError> {
    let trace = dfs_cluster_status_trace_enabled();
    let started_at = std::time::Instant::now();
    let rpc_request = sinomo_runtime::dfs::dfs_status_rpc_request()?;
    if trace {
        eprintln!(
            "sinomo dfs cluster status node={node_id} +{}ms: request built",
            started_at.elapsed().as_millis()
        );
    }
    if trace {
        eprintln!(
            "sinomo dfs cluster status node={node_id} +{}ms: daemon send ready",
            started_at.elapsed().as_millis()
        );
    }
    let response = daemon_dfs_object_rpc_to_target(
        node_id,
        &rpc_request,
        sinomo_runtime::dfs::DfsObjectRpcSendOptions::default().with_overall_timeout(timeout),
    );
    if trace {
        let outcome = if response.is_ok() { "ok" } else { "error" };
        eprintln!(
            "sinomo dfs cluster status node={node_id} +{}ms: rpc {outcome}",
            started_at.elapsed().as_millis()
        );
    }
    let response = response.map_err(CliError::from)?;
    let decoded =
        sinomo_runtime::dfs::decode_dfs_status_rpc_response(&response).map_err(Into::into);
    if trace {
        let outcome = if decoded.is_ok() { "ok" } else { "error" };
        eprintln!(
            "sinomo dfs cluster status node={node_id} +{}ms: decode {outcome}",
            started_at.elapsed().as_millis()
        );
    }
    decoded
}

fn decode_dfs_advertisements(
    entries: Vec<sinomo_runtime::kv::KvEntry>,
) -> Result<Vec<sinomo_runtime::dfs::DfsNodeAdvertisement>, CliError> {
    let mut advertisements: Vec<sinomo_runtime::dfs::DfsNodeAdvertisement> = Vec::new();
    for entry in entries {
        advertisements.push(entry.decode()?);
    }
    advertisements.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(advertisements)
}

fn print_dfs_cluster_status(report: &DfsClusterStatusReport) {
    println!("DFS cluster status:");
    for node in &report.nodes {
        println!(
            "  {}{}:",
            node.node_id,
            if node.local { " (local)" } else { "" }
        );
        if let Some(endpoint) = &node.endpoint {
            println!("    endpoint: {endpoint}");
        }
        if let Some(status) = &node.status {
            println!("    filesystems: {}", status.file_system_count);
            println!("    objects: {}", status.object_count);
            println!("    versions: {}", status.version_count);
            println!("    pending writes: {}", status.pending_write_count);
            println!("    pending repairs: {}", status.pending_repair_count);
            print_dfs_repair_attempt_summary("    ", status);
            println!(
                "    pending namespace jobs: {}",
                status.pending_namespace_job_count
            );
            println!(
                "    failed namespace jobs: {}",
                status.failed_namespace_job_count
            );
            println!("    open conflicts: {}", status.open_conflict_count);
            if !status.metadata_shards.is_empty() {
                println!("    metadata shards: {}", status.metadata_shards.len());
                for shard in &status.metadata_shards {
                    println!(
                        "      {}: journal {}",
                        shard.shard_id.0, shard.journal_last_sequence
                    );
                    if let Some(primary_node_id) = &shard.primary_node_id {
                        println!("        primary: {primary_node_id}");
                    }
                    for replica in &shard.replica_lag {
                        println!(
                            "        replica {}: through {}, lag {}",
                            replica.target_node_id,
                            replica.replicated_through_sequence,
                            replica.lag_entries
                        );
                    }
                    for cursor in &shard.replay_cursors {
                        println!(
                            "        replay {}: through {}",
                            cursor.source_shard_id.0, cursor.replayed_through_sequence
                        );
                    }
                }
            }
        }
        if let Some(error) = &node.error {
            println!("    error: {error}");
        }
    }
}

fn print_dfs_repair_attempt_summary(indent: &str, status: &sinomo_runtime::dfs::DfsOperatorStatus) {
    let total = status
        .repair_attempt_count
        .max(status.repair_attempts.len());
    if total == 0 {
        return;
    }
    let in_flight = status
        .repair_attempts
        .iter()
        .filter(|attempt| attempt.in_flight)
        .count();
    let suppressed = status
        .repair_attempts
        .iter()
        .filter(|attempt| attempt.next_retry_unix_nanos == u64::MAX)
        .count();
    let retry_waiting = status
        .repair_attempts
        .len()
        .saturating_sub(in_flight)
        .saturating_sub(suppressed);
    if status.repair_attempts.len() == total {
        println!(
            "{indent}repair attempts: {total} (in_flight={in_flight}, retry_waiting={retry_waiting}, suppressed={suppressed})"
        );
    } else {
        println!(
            "{indent}repair attempts: {total} (sample={}, in_flight={}, retry_waiting={}, suppressed={})",
            status.repair_attempts.len(),
            in_flight,
            retry_waiting,
            suppressed
        );
    }
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

fn print_dfs_conflict_versions_report(report: &DfsOperatorConflictVersionsReport) {
    println!("DFS conflict versions: {}", report.object.object_id);
    println!("  path: {}", report.object.path);
    println!("  current: {}", report.object.version);
    if report.conflicts.is_empty() {
        println!("  conflicts: none");
    } else {
        println!("  conflicts: {}", report.conflicts.len());
    }
    if report.versions.is_empty() {
        println!("No conflict versions.");
        return;
    }
    for version in &report.versions {
        let roles = version
            .roles
            .iter()
            .map(|role| {
                role.conflict_id
                    .as_ref()
                    .map(|id| format!("{}:{id}", role.role))
                    .unwrap_or_else(|| role.role.clone())
            })
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "  {} roles=[{}] hash={} size={} stored={} visible={} current={}",
            version.version,
            roles,
            option_label(&version.content_hash),
            version
                .size_bytes
                .map(|size| size.to_string())
                .unwrap_or_else(|| "-".to_string()),
            version.stored,
            version.visible,
            version.current
        );
        if !version.locations.is_empty() {
            println!("    locations: {}", join_labels(&version.locations));
        }
    }
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
                options.mode.as_ref(),
            )?;
            if options.json {
                print_json(
                    "<dfs-pin-remove>",
                    &serde_json::json!({
                        "file_system_id": options.file_system_id,
                        "path": options.path,
                        "mode": options.mode,
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

pub(crate) fn open_dfs_metadata_for_inspection(
    explicit: Option<PathBuf>,
) -> Result<DfsInspectionMetadata, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    open_dfs_metadata_for_inspection_with_data_dir(explicit, &data_dir)
}

pub(crate) fn open_dfs_metadata_for_inspection_with_data_dir(
    explicit: Option<PathBuf>,
    data_dir: &SinomoDataDir,
) -> Result<DfsInspectionMetadata, CliError> {
    if let Some(metadata_root) = explicit {
        return open_root_local_metadata_backend(dfs_metadata_root(Some(metadata_root))?)
            .map(DfsInspectionMetadata::Root)
            .map_err(Into::into);
    }

    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(data_dir);
    if cluster_paths.state.exists() {
        let file_system_id = sifs_file_system_id();
        let mut shards = sinomo_runtime::kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &format!("{}/", metadata_shards_prefix(&file_system_id)),
        )?
        .into_iter()
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        if !shards.is_empty() {
            return open_local_sharded_metadata_backend(
                default_dfs_metadata_shards_root(data_dir),
                shards,
            )
            .map(DfsInspectionMetadata::Sharded)
            .map_err(Into::into);
        }
    }

    open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
        .map(DfsInspectionMetadata::Root)
        .map_err(Into::into)
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

pub(crate) fn parse_dfs_version(value: &str) -> Result<DfsVersion, CliError> {
    DfsVersion::new(value).map_err(|source| CliError::Parse {
        detail: format!("invalid DFS version `{value}`: {source}"),
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

fn encode_dfs_inspection_cursor(cursor: &DfsOperatorInspectionCursor) -> Result<String, CliError> {
    let bytes = serde_json::to_vec(cursor).map_err(|source| CliError::Json {
        path: "<dfs-inspection-cursor>".to_string(),
        detail: source.to_string(),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_dfs_inspection_cursor(cursor: &str) -> Result<DfsOperatorInspectionCursor, CliError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|source| CliError::Parse {
            detail: format!("invalid DFS inspection cursor: {source}"),
        })?;
    serde_json::from_slice(&bytes).map_err(|source| CliError::Json {
        path: "<dfs-inspection-cursor>".to_string(),
        detail: source.to_string(),
    })
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
