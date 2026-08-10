use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec,
    CliParseError, CliPositionalSpec, CliValueSpec,
};
#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;

use crate::sifs_file_manager::SifsControlPlaneKvClient;

const DFS_CLUSTER_STATUS_REMOTE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);
const DFS_SMOKE_DAEMON_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(9_500);
const DFS_PLACEMENT_AUTHORITY_ADMIN_RPC_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);
const DFS_REPAIR_QUEUE_RECOVERY_RPC_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsCliCommand {
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
    RecoverRepairQueue {
        json: bool,
    },
    Smoke {
        json: bool,
    },
    MetadataPublish {
        json: bool,
    },
    MetadataShardTransferPrimary(DfsMetadataShardTransferPrimaryOptions),
    PlacementAuthority(DfsPlacementAuthorityCliCommand),
    Mount(DfsMountOptions),
    Services {
        json: bool,
    },
    Pin(DfsPinCliCommand),
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsPlacementAuthorityCliCommand {
    Inspect(DfsPlacementAuthorityInspectOptions),
    Clear(DfsPlacementAuthorityClearOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsPlacementAuthorityInspectOptions {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DfsPlacementAuthorityClearOptions {
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) expected_recovery_token: String,
    pub(crate) recovery: DfsPlacementAuthorityRecovery,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsPlacementAuthorityRecovery {
    RemovedNode,
    BreakGlass { acknowledgement: String },
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

pub(crate) fn dfs_cli_command_handler() -> DeclarativeCommandHandler<DfsCliCommand> {
    DeclarativeCommandHandler::new(
        "dfs",
        CliCommandPolicy::local_capable(),
        dfs_cli_spec,
        parse_dfs_matches,
        run_dfs_cli_command,
    )
    .with_parsed_policy(dfs_cli_policy)
}

fn dfs_cli_spec() -> CliCommandSpec {
    let option = |name: &'static str, help: &'static str, value: &'static str| {
        CliOptionSpec::value(name, help, CliValueSpec::new(value, help))
    };
    let json = || CliOptionSpec::flag("--json", "print JSON output");
    let filesystem =
        || option("--filesystem", "DFS filesystem id", "file-system-id").with_aliases(["--fs"]);
    let metadata_root = || option("--metadata-root", "metadata root override", "path");
    let common = || {
        [
            metadata_root(),
            CliOptionSpec::flag("--cluster", "query cluster state"),
            json(),
        ]
    };
    let filter = || {
        [
            metadata_root(),
            filesystem(),
            option("--object", "DFS object id", "object-id"),
            option("--limit", "maximum results", "count"),
            option("--cursor", "inspection cursor", "cursor"),
            json(),
        ]
    };
    let conflict_target = || {
        [
            metadata_root(),
            filesystem(),
            option("--path", "DFS path", "path"),
            option("--object", "DFS object id", "object-id"),
            option("--id", "conflict id", "conflict-id")
                .with_aliases(["--conflict", "--conflict-id"])
                .repeatable(),
            json(),
        ]
    };
    let conflicts = {
        let mut resolve = conflict_target().to_vec();
        resolve.push(CliOptionSpec::flag(
            "--delete-file",
            "unlink the conflicted file",
        ));
        let versions = conflict_target();
        let mut use_version = conflict_target().to_vec();
        use_version.push(option("--version", "stored DFS version", "version").required());
        let mut save_version = use_version.clone();
        save_version.push(
            option("--to", "destination DFS path", "path")
                .with_aliases(["--destination", "--dest"])
                .required(),
        );
        CliCommandSpec::new("conflicts", "inspect and resolve DFS conflicts")
            .with_options(filter())
            .with_children([
                CliCommandSpec::new("resolve", "remove matching conflict records")
                    .with_options(resolve),
                CliCommandSpec::new("versions", "show stored conflicting versions")
                    .with_options(versions),
                CliCommandSpec::new("use-version", "make one conflict version visible")
                    .with_options(use_version),
                CliCommandSpec::new("save-version", "copy a conflict version for manual merge")
                    .with_options(save_version),
            ])
    };
    let pin_mode = || {
        CliValueSpec::new("metadata|content", "pin mode")
            .with_choices([
                CliAutocompleteChoice::new("metadata", "pin metadata only"),
                CliAutocompleteChoice::new("content", "pin content"),
            ])
            .restricted()
    };
    CliCommandSpec::new("dfs", "inspect and manage SIFS and DFS state")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("status", "summarize DFS metadata and repair state")
                .with_options(common()),
            CliCommandSpec::new("filesystem", "inspect one filesystem")
                .with_aliases(["fs"])
                .with_options(common())
                .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                    "file-system-id",
                    "DFS filesystem id",
                ))]),
            CliCommandSpec::new("path", "inspect an object by filesystem path")
                .with_options([filesystem(), metadata_root(), json()])
                .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                    "path", "DFS path",
                ))]),
            CliCommandSpec::new("object", "inspect one DFS object")
                .with_options(common())
                .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                    "object-id",
                    "DFS object id",
                ))]),
            conflicts,
            CliCommandSpec::new("repair", "list inferred replica repairs").with_options(filter()),
            CliCommandSpec::new(
                "recover-repair-queue",
                "explicitly rebuild a lost repair queue from a full metadata scan",
            )
            .with_options([
                CliOptionSpec::flag(
                    "--acknowledge-full-metadata-scan",
                    "acknowledge that explicit disaster recovery scans all DFS metadata",
                ),
                json(),
            ]),
            CliCommandSpec::new("services", "list DFS service advertisements")
                .with_options([json()]),
            CliCommandSpec::new("smoke", "write a small replicated SIFS smoke file")
                .with_options([json()]),
            CliCommandSpec::new("metadata", "maintain DFS metadata").with_children([
                CliCommandSpec::new("publish", "publish the primary metadata shard journal")
                    .with_options([json()]),
                CliCommandSpec::new("shard", "manage metadata shards").with_children([
                    CliCommandSpec::new("transfer-primary", "transfer a metadata shard primary")
                        .with_options([
                            filesystem(),
                            option("--shard", "metadata shard id", "shard-id"),
                            option("--to", "new primary node", "node-id")
                                .with_aliases(["--node", "--primary"])
                                .required(),
                            option("--expected-epoch", "expected shard epoch", "epoch")
                                .with_aliases(["--epoch"]),
                            json(),
                        ])
                        .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                            "shard-id",
                            "metadata shard id",
                        ))]),
                ]),
            ]),
            CliCommandSpec::new(
                "placement-authority",
                "inspect or recover placement authority",
            )
            .with_children([
                CliCommandSpec::new("inspect", "inspect the placement mutation permit")
                    .with_options([filesystem(), json()]),
                CliCommandSpec::new(
                    "clear",
                    format!(
                        "clear a fenced placement mutation permit; break-glass requires `{}`",
                        sinomo_runtime::dfs::DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT
                    ),
                )
                    .with_options([
                        filesystem(),
                        option("--expected-token", "expected recovery token", "token").required(),
                        CliOptionSpec::flag(
                            "--acknowledge-owner-stopped",
                            "confirm the removed owner is stopped",
                        ),
                        CliOptionSpec::value(
                            "--break-glass-acknowledgement",
                            format!(
                                "exact break-glass acknowledgement: {}",
                                sinomo_runtime::dfs::DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT
                            ),
                            CliValueSpec::new("text", "exact break-glass acknowledgement"),
                        ),
                        json(),
                    ]),
            ]),
            CliCommandSpec::new("mount", "expose SIFS locally")
                .with_options([
                    option("--root", "SIFS root path", "path").with_aliases(["--path"]),
                    option("--data-dir", "Sinomo data directory", "path"),
                    CliOptionSpec::flag("--read-only", "mount read-only").with_aliases(["--ro"]),
                    CliOptionSpec::flag("--allow-other", "allow other local users"),
                    CliOptionSpec::flag("--foreground", "stay in the foreground"),
                    CliOptionSpec::flag("--background", "run mount in the background"),
                    CliOptionSpec::flag("--sync-local-changes", "sync local mount changes"),
                    CliOptionSpec::flag(
                        "--no-sync-local-changes",
                        "disable local change synchronization",
                    ),
                ])
                .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                    "mount-point",
                    "local mount directory",
                ))]),
            CliCommandSpec::new("pin", "manage local DFS pins").with_children([
                CliCommandSpec::new("list", "list local DFS pins").with_options([
                    filesystem(),
                    option("--pin-root", "pin store root", "path"),
                    json(),
                ]),
                CliCommandSpec::new("add", "add a local DFS pin").with_options([
                    option("--path", "DFS path", "path").required(),
                    filesystem(),
                    option("--object", "DFS object id", "object-id"),
                    option("--node", "target node id", "node-id"),
                    CliOptionSpec::value("--mode", "pin mode", pin_mode()),
                    option("--pin-root", "pin store root", "path"),
                    json(),
                ]),
                CliCommandSpec::new("remove", "remove a local DFS pin").with_options([
                    option("--path", "DFS path", "path").required(),
                    filesystem(),
                    option("--node", "target node id", "node-id"),
                    CliOptionSpec::value("--mode", "pin mode", pin_mode()),
                    option("--pin-root", "pin store root", "path"),
                    json(),
                ]),
            ]),
        ])
}

fn dfs_common_from_matches(matches: &CliCommandMatches) -> DfsCommonOptions {
    DfsCommonOptions {
        metadata_root: matches.value("--metadata-root").map(PathBuf::from),
        cluster: matches.contains("--cluster"),
        json: matches.contains("--json"),
    }
}

fn dfs_filter_from_matches(matches: &CliCommandMatches) -> Result<DfsFilterOptions, CliParseError> {
    Ok(DfsFilterOptions {
        common: dfs_common_from_matches(matches),
        file_system_id: matches
            .value("--filesystem")
            .map(parse_dfs_file_system_id)
            .transpose()
            .map_err(semantic_parse_error)?,
        object_id: matches
            .value("--object")
            .map(parse_dfs_object_id)
            .transpose()
            .map_err(semantic_parse_error)?,
        limit: matches
            .value("--limit")
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|source| CliParseError::Parse {
                        detail: format!("invalid DFS inspection limit {value}: {source}"),
                    })
            })
            .transpose()?,
        cursor: matches
            .value("--cursor")
            .map(decode_dfs_inspection_cursor)
            .transpose()
            .map_err(semantic_parse_error)?,
    })
}

struct DfsConflictTargetMatches {
    common: DfsCommonOptions,
    file_system_id: DfsFileSystemId,
    path: Option<String>,
    object_id: Option<DfsObjectId>,
    conflict_ids: Vec<String>,
}

fn dfs_conflict_target_from_matches(
    matches: &CliCommandMatches,
) -> Result<DfsConflictTargetMatches, CliParseError> {
    let path = matches
        .value("--path")
        .map(|value| non_empty_string(value, "path"))
        .transpose()
        .map_err(semantic_parse_error)?;
    let object_id = matches
        .value("--object")
        .map(parse_dfs_object_id)
        .transpose()
        .map_err(semantic_parse_error)?;
    if path.is_none() && object_id.is_none() {
        return Err(CliParseError::Parse {
            detail: "DFS conflict command requires --path or --object".to_string(),
        });
    }
    Ok(DfsConflictTargetMatches {
        common: dfs_common_from_matches(matches),
        file_system_id: matches
            .value("--filesystem")
            .map(parse_dfs_file_system_id)
            .transpose()
            .map_err(semantic_parse_error)?
            .unwrap_or_else(default_dfs_file_system_id),
        path,
        object_id,
        conflict_ids: matches
            .values("--id")
            .map(|value| non_empty_string(value, "conflict id").map_err(semantic_parse_error))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn parse_dfs_matches(matches: &CliCommandMatches) -> Result<DfsCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    match path.as_slice() {
        ["dfs", "status"] => Ok(DfsCliCommand::Status(dfs_common_from_matches(matches))),
        ["dfs", "filesystem"] => Ok(DfsCliCommand::FileSystem {
            file_system_id: parse_dfs_file_system_id(required_positional(matches, 0)?)
                .map_err(semantic_parse_error)?,
            options: dfs_common_from_matches(matches),
        }),
        ["dfs", "path"] => Ok(DfsCliCommand::Path {
            path: required_positional(matches, 0)?.to_string(),
            file_system_id: matches
                .value("--filesystem")
                .map(parse_dfs_file_system_id)
                .transpose()
                .map_err(semantic_parse_error)?
                .unwrap_or_else(default_dfs_file_system_id),
            options: dfs_common_from_matches(matches),
        }),
        ["dfs", "object"] => Ok(DfsCliCommand::Object {
            object_id: parse_dfs_object_id(required_positional(matches, 0)?)
                .map_err(semantic_parse_error)?,
            options: dfs_common_from_matches(matches),
        }),
        ["dfs", "conflicts"] => dfs_filter_from_matches(matches).map(DfsCliCommand::Conflicts),
        ["dfs", "conflicts", "resolve"] => {
            let target = dfs_conflict_target_from_matches(matches)?;
            Ok(DfsCliCommand::ResolveConflicts(DfsConflictResolveOptions {
                common: target.common,
                file_system_id: target.file_system_id,
                path: target.path,
                object_id: target.object_id,
                conflict_ids: target.conflict_ids,
                delete_file: matches.contains("--delete-file"),
            }))
        }
        ["dfs", "conflicts", "versions"] => {
            let target = dfs_conflict_target_from_matches(matches)?;
            Ok(DfsCliCommand::ConflictVersions(
                DfsConflictVersionsOptions {
                    common: target.common,
                    file_system_id: target.file_system_id,
                    path: target.path,
                    object_id: target.object_id,
                    conflict_ids: target.conflict_ids,
                },
            ))
        }
        ["dfs", "conflicts", "use-version"] => {
            let target = dfs_conflict_target_from_matches(matches)?;
            Ok(DfsCliCommand::UseConflictVersion(
                DfsConflictUseVersionOptions {
                    common: target.common,
                    file_system_id: target.file_system_id,
                    path: target.path,
                    object_id: target.object_id,
                    version: parse_dfs_version(required_option(matches, "--version")?)
                        .map_err(semantic_parse_error)?,
                    conflict_ids: target.conflict_ids,
                },
            ))
        }
        ["dfs", "conflicts", "save-version"] => {
            let target = dfs_conflict_target_from_matches(matches)?;
            Ok(DfsCliCommand::SaveConflictVersion(
                DfsConflictSaveVersionOptions {
                    common: target.common,
                    file_system_id: target.file_system_id,
                    path: target.path,
                    object_id: target.object_id,
                    version: parse_dfs_version(required_option(matches, "--version")?)
                        .map_err(semantic_parse_error)?,
                    conflict_ids: target.conflict_ids,
                    destination_path: non_empty_string(
                        required_option(matches, "--to")?,
                        "destination path",
                    )
                    .map_err(semantic_parse_error)?,
                },
            ))
        }
        ["dfs", "repair"] => dfs_filter_from_matches(matches).map(DfsCliCommand::Repair),
        ["dfs", "recover-repair-queue"] => {
            if !matches.contains("--acknowledge-full-metadata-scan") {
                return Err(CliParseError::Parse {
                    detail: "recover-repair-queue requires --acknowledge-full-metadata-scan"
                        .to_string(),
                });
            }
            Ok(DfsCliCommand::RecoverRepairQueue {
                json: matches.contains("--json"),
            })
        }
        ["dfs", "services"] => Ok(DfsCliCommand::Services {
            json: matches.contains("--json"),
        }),
        ["dfs", "smoke"] => Ok(DfsCliCommand::Smoke {
            json: matches.contains("--json"),
        }),
        ["dfs", "metadata", "publish"] => Ok(DfsCliCommand::MetadataPublish {
            json: matches.contains("--json"),
        }),
        ["dfs", "metadata", "shard", "transfer-primary"] => {
            let shard_id = matches
                .value("--shard")
                .or_else(|| matches.positionals().first().map(String::as_str))
                .ok_or_else(|| CliParseError::Parse {
                    detail: "metadata shard transfer requires a shard id".to_string(),
                })?;
            Ok(DfsCliCommand::MetadataShardTransferPrimary(
                DfsMetadataShardTransferPrimaryOptions {
                    file_system_id: matches
                        .value("--filesystem")
                        .map(parse_dfs_file_system_id)
                        .transpose()
                        .map_err(semantic_parse_error)?
                        .unwrap_or_else(default_dfs_file_system_id),
                    shard_id: non_empty_string(shard_id, "metadata shard id")
                        .map_err(semantic_parse_error)?,
                    new_primary_node_id: parse_node_id(required_option(matches, "--to")?)
                        .map_err(semantic_parse_error)?,
                    expected_epoch: matches
                        .value("--expected-epoch")
                        .map(|value| {
                            value.parse::<u64>().map_err(|source| CliParseError::Parse {
                                detail: format!(
                                    "invalid DFS metadata shard epoch `{value}`: {source}"
                                ),
                            })
                        })
                        .transpose()?,
                    json: matches.contains("--json"),
                },
            ))
        }
        ["dfs", "placement-authority", "inspect"] => Ok(DfsCliCommand::PlacementAuthority(
            DfsPlacementAuthorityCliCommand::Inspect(DfsPlacementAuthorityInspectOptions {
                file_system_id: matches
                    .value("--filesystem")
                    .map(parse_dfs_file_system_id)
                    .transpose()
                    .map_err(semantic_parse_error)?
                    .unwrap_or_else(default_dfs_file_system_id),
                json: matches.contains("--json"),
            }),
        )),
        ["dfs", "placement-authority", "clear"] => {
            let removed = matches.contains("--acknowledge-owner-stopped");
            let break_glass = matches.value("--break-glass-acknowledgement");
            let recovery = match (removed, break_glass) {
                (true, None) => DfsPlacementAuthorityRecovery::RemovedNode,
                (false, Some(acknowledgement))
                    if acknowledgement
                        == sinomo_runtime::dfs::DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT =>
                {
                    DfsPlacementAuthorityRecovery::BreakGlass {
                        acknowledgement: acknowledgement.to_string(),
                    }
                }
                _ => return Err(CliParseError::Parse {
                    detail: "placement-authority clear requires exactly one valid recovery acknowledgement".to_string(),
                }),
            };
            let token = required_option(matches, "--expected-token")?;
            if token.trim().is_empty() || token.trim() != token {
                return Err(CliParseError::Parse {
                    detail: "--expected-token must be non-empty without surrounding whitespace"
                        .to_string(),
                });
            }
            Ok(DfsCliCommand::PlacementAuthority(
                DfsPlacementAuthorityCliCommand::Clear(DfsPlacementAuthorityClearOptions {
                    file_system_id: matches
                        .value("--filesystem")
                        .map(parse_dfs_file_system_id)
                        .transpose()
                        .map_err(semantic_parse_error)?
                        .unwrap_or_else(default_dfs_file_system_id),
                    expected_recovery_token: token.to_string(),
                    recovery,
                    json: matches.contains("--json"),
                }),
            ))
        }
        ["dfs", "mount"] => parse_dfs_mount_matches(matches).map(DfsCliCommand::Mount),
        ["dfs", "pin", command] => parse_dfs_pin_matches(command, matches).map(DfsCliCommand::Pin),
        _ => Err(CliParseError::Parse {
            detail: "dfs requires a subcommand".to_string(),
        }),
    }
}

#[cfg(test)]
pub(crate) fn parse_dfs_schema_for_test(args: &[String]) -> Result<DfsCliCommand, CliError> {
    parse_schema_command_for_test(dfs_cli_spec(), args, parse_dfs_matches)
}

fn parse_dfs_mount_matches(matches: &CliCommandMatches) -> Result<DfsMountOptions, CliParseError> {
    if matches.contains("--foreground") && matches.contains("--background") {
        return Err(CliParseError::Parse {
            detail: "dfs mount cannot combine --foreground and --background".to_string(),
        });
    }
    if matches.contains("--sync-local-changes") && matches.contains("--no-sync-local-changes") {
        return Err(CliParseError::Parse {
            detail: "dfs mount cannot combine --sync-local-changes and --no-sync-local-changes"
                .to_string(),
        });
    }
    Ok(DfsMountOptions {
        mount_point: PathBuf::from(required_positional(matches, 0)?),
        root_path: matches.value("--root").unwrap_or("/").to_string(),
        data_dir: matches.value("--data-dir").map(PathBuf::from),
        read_only: matches.contains("--read-only"),
        allow_other: matches.contains("--allow-other"),
        foreground: !matches.contains("--background"),
        sync_local_changes: matches.contains("--sync-local-changes"),
    })
}

fn parse_dfs_pin_matches(
    command: &str,
    matches: &CliCommandMatches,
) -> Result<DfsPinCliCommand, CliParseError> {
    let filesystem = || {
        matches
            .value("--filesystem")
            .map(parse_dfs_file_system_id)
            .transpose()
            .map_err(semantic_parse_error)
    };
    let node = || {
        matches
            .value("--node")
            .map(parse_node_id)
            .transpose()
            .map_err(semantic_parse_error)
    };
    let mode = || {
        matches
            .value("--mode")
            .map(parse_dfs_pin_mode)
            .transpose()
            .map_err(semantic_parse_error)
    };
    match command {
        "list" => Ok(DfsPinCliCommand::List(DfsPinListOptions {
            pin_root: matches.value("--pin-root").map(PathBuf::from),
            file_system_id: filesystem()?,
            json: matches.contains("--json"),
        })),
        "add" => Ok(DfsPinCliCommand::Add(DfsPinAddOptions {
            pin_root: matches.value("--pin-root").map(PathBuf::from),
            file_system_id: filesystem()?.unwrap_or_else(default_dfs_file_system_id),
            path: non_empty_string(required_option(matches, "--path")?, "path")
                .map_err(semantic_parse_error)?,
            object_id: matches
                .value("--object")
                .map(parse_dfs_object_id)
                .transpose()
                .map_err(semantic_parse_error)?,
            node_id: node()?,
            mode: mode()?.unwrap_or(DfsPinMode::Content),
            json: matches.contains("--json"),
        })),
        "remove" => Ok(DfsPinCliCommand::Remove(DfsPinRemoveOptions {
            pin_root: matches.value("--pin-root").map(PathBuf::from),
            file_system_id: filesystem()?.unwrap_or_else(default_dfs_file_system_id),
            path: non_empty_string(required_option(matches, "--path")?, "path")
                .map_err(semantic_parse_error)?,
            node_id: node()?,
            mode: mode()?,
            json: matches.contains("--json"),
        })),
        _ => Err(CliParseError::Parse {
            detail: format!("unknown dfs pin command `{command}`"),
        }),
    }
}

fn dfs_cli_policy(command: &DfsCliCommand) -> CliCommandPolicy {
    if dfs_command_requires_cluster_state(command) {
        CliCommandPolicy::cluster_required()
    } else {
        CliCommandPolicy::local_capable()
    }
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
        DfsCliCommand::RecoverRepairQueue { json } => {
            let status =
                default_main_daemon_status()?.ok_or_else(|| CliError::DaemonNotRunning {
                    detail: "DFS repair queue recovery requires the local main daemon".to_string(),
                })?;
            let response = sinomo_runtime::daemon::send_daemon_dfs_repair_queue_recovery(
                &status.address,
                DFS_REPAIR_QUEUE_RECOVERY_RPC_TIMEOUT,
            )?;
            if json {
                print_json("<dfs-repair-queue-recovery>", &response)?;
            } else {
                println!("DFS repair queue recovery:");
                println!("  completed: {}", response.completed);
                println!("  reached limit: {}", response.reached_limit);
                println!(
                    "  remaining queued repairs: {}",
                    response.remaining_queued_repairs
                );
            }
            if !response.completed {
                return Err(CliError::Parse {
                    detail:
                        "DFS repair queue recovery yielded to foreground work; retry the command"
                            .to_string(),
                });
            }
            Ok(())
        }
        DfsCliCommand::Smoke { json } => {
            let status =
                default_main_daemon_status()?.ok_or_else(|| CliError::DaemonNotRunning {
                    detail: "DFS smoke requires the local main daemon; offline cluster mutation is reserved for explicit recovery"
                        .to_string(),
                })?;
            let report =
                run_dfs_smoke_via_daemon_address(&status.address, DFS_SMOKE_DAEMON_RPC_TIMEOUT)?;
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
        DfsCliCommand::PlacementAuthority(command) => run_dfs_placement_authority_command(command),
        DfsCliCommand::Mount(options) => run_dfs_mount_command(options),
        DfsCliCommand::Pin(command) => run_dfs_pin_command(command),
    }
}

fn run_dfs_placement_authority_command(
    command: DfsPlacementAuthorityCliCommand,
) -> Result<(), CliError> {
    match command {
        DfsPlacementAuthorityCliCommand::Inspect(options) => {
            let response = send_dfs_placement_authority_admin_request(
                &sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminRequest::Inspect {
                    file_system_id: options.file_system_id.clone(),
                },
            )?;
            let sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminResponse::Inspection {
                permit,
            } = &response
            else {
                return Err(CliError::Parse {
                    detail: "main daemon returned an unexpected DFS placement-authority response"
                        .to_string(),
                });
            };
            if options.json {
                print_json("<dfs-placement-authority-inspect>", &response)
            } else {
                print_dfs_placement_authority_inspection(&options.file_system_id, permit.as_ref());
                Ok(())
            }
        }
        DfsPlacementAuthorityCliCommand::Clear(options) => {
            let request = match &options.recovery {
                DfsPlacementAuthorityRecovery::RemovedNode => {
                    sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminRequest::RecoverRemovedNode {
                        file_system_id: options.file_system_id.clone(),
                        expected_recovery_token: options.expected_recovery_token.clone(),
                        acknowledge_owner_stopped: true,
                    }
                }
                DfsPlacementAuthorityRecovery::BreakGlass { acknowledgement } => {
                    sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminRequest::BreakGlassRecover {
                        file_system_id: options.file_system_id.clone(),
                        expected_recovery_token: options.expected_recovery_token.clone(),
                        acknowledgement: acknowledgement.clone(),
                    }
                }
            };
            let response = send_dfs_placement_authority_admin_request(&request)?;
            let sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminResponse::Recovery {
                outcome,
            } = &response
            else {
                return Err(CliError::Parse {
                    detail: "main daemon returned an unexpected DFS placement-authority response"
                        .to_string(),
                });
            };
            if options.json {
                print_json("<dfs-placement-authority-clear>", &response)?;
            } else {
                print_dfs_placement_authority_recovery_outcome(&options.file_system_id, outcome);
            }
            validate_dfs_placement_authority_recovery_outcome(outcome)
        }
    }
}

fn send_dfs_placement_authority_admin_request(
    request: &sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminRequest,
) -> Result<sinomo_runtime::daemon::DaemonDfsPlacementAuthorityAdminResponse, CliError> {
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::DaemonNotRunning {
        detail: "DFS placement-authority administration requires the local main daemon".to_string(),
    })?;
    let rpc_request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::daemon::DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
        sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
        request,
    )?;
    let response = daemon_send_daemon_rpc(
        &status.address,
        &rpc_request,
        DFS_PLACEMENT_AUTHORITY_ADMIN_RPC_TIMEOUT,
    )?;
    response.error_detail()?;
    response.decode_body().map_err(Into::into)
}

fn print_dfs_placement_authority_inspection(
    file_system_id: &DfsFileSystemId,
    permit: Option<&sinomo_runtime::dfs::DfsPlacementAuthorityPermitStatus>,
) {
    let Some(permit) = permit else {
        println!("No DFS placement-authority permit is active for filesystem {file_system_id}.");
        return;
    };
    let holder_kind = match permit.holder_kind {
        sinomo_runtime::dfs::DfsPlacementAuthorityPermitHolderKind::Identified => "identified",
        sinomo_runtime::dfs::DfsPlacementAuthorityPermitHolderKind::Partial => "partial",
        sinomo_runtime::dfs::DfsPlacementAuthorityPermitHolderKind::Legacy => "legacy",
    };
    println!("DFS placement-authority permit:");
    println!("  filesystem: {}", permit.file_system_id);
    println!("  reconciliation job: {}", permit.job_id);
    println!("  holder identity: {holder_kind}");
    println!(
        "  holder cluster: {}",
        permit.holder_cluster_id.as_deref().unwrap_or("unknown")
    );
    println!(
        "  holder node: {}",
        permit
            .holder_node_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref()
            .unwrap_or("unknown")
    );
    println!("  recovery token: {}", permit.recovery_token);
    println!(
        "Inspect cluster membership and stop or fence the prior writer before attempting recovery."
    );
}

fn print_dfs_placement_authority_recovery_outcome(
    file_system_id: &DfsFileSystemId,
    outcome: &sinomo_runtime::dfs::DfsPlacementAuthorityRecoveryOutcome,
) {
    use sinomo_runtime::dfs::DfsPlacementAuthorityRecoveryOutcome;
    match outcome {
        DfsPlacementAuthorityRecoveryOutcome::Missing => {
            println!("No DFS placement-authority permit is active for filesystem {file_system_id}.")
        }
        DfsPlacementAuthorityRecoveryOutcome::Cleared => {
            println!("Cleared the DFS placement-authority permit for filesystem {file_system_id}.")
        }
        DfsPlacementAuthorityRecoveryOutcome::Changed
        | DfsPlacementAuthorityRecoveryOutcome::Refused { .. } => {}
    }
}

fn validate_dfs_placement_authority_recovery_outcome(
    outcome: &sinomo_runtime::dfs::DfsPlacementAuthorityRecoveryOutcome,
) -> Result<(), CliError> {
    use sinomo_runtime::dfs::DfsPlacementAuthorityRecoveryOutcome;
    match outcome {
        DfsPlacementAuthorityRecoveryOutcome::Missing
        | DfsPlacementAuthorityRecoveryOutcome::Cleared => Ok(()),
        DfsPlacementAuthorityRecoveryOutcome::Changed => Err(CliError::ClusterReadOnly {
            detail: "the DFS placement-authority permit changed; inspect it again before retrying"
                .to_string(),
        }),
        DfsPlacementAuthorityRecoveryOutcome::Refused { reason } => {
            Err(CliError::ClusterReadOnly {
                detail: format!(
                    "DFS placement-authority recovery was refused: {}",
                    dfs_placement_authority_refusal_label(reason)
                ),
            })
        }
    }
}

fn dfs_placement_authority_refusal_label(
    reason: &sinomo_runtime::dfs::DfsPlacementAuthorityRecoveryRefusal,
) -> String {
    use sinomo_runtime::dfs::DfsPlacementAuthorityRecoveryRefusal;
    match reason {
        DfsPlacementAuthorityRecoveryRefusal::OwnerStoppedAcknowledgementRequired => {
            "the operator did not acknowledge that the prior owner is stopped or fenced".to_string()
        }
        DfsPlacementAuthorityRecoveryRefusal::CurrentLocalNode => {
            "the current local node still owns the permit; restart its main daemon instead"
                .to_string()
        }
        DfsPlacementAuthorityRecoveryRefusal::IncompleteHolderIdentity => {
            "the permit has incomplete holder identity; use the explicit break-glass procedure"
                .to_string()
        }
        DfsPlacementAuthorityRecoveryRefusal::ForeignCluster => {
            "the permit belongs to a different cluster; use the explicit break-glass procedure"
                .to_string()
        }
        DfsPlacementAuthorityRecoveryRefusal::HolderMembershipPresent { state } => {
            format!("the holder remains in cluster membership with state {state:?}")
        }
        DfsPlacementAuthorityRecoveryRefusal::InvalidRemovedMembershipEvidence => {
            "cluster membership does not prove that the holder was removed".to_string()
        }
        DfsPlacementAuthorityRecoveryRefusal::BreakGlassAcknowledgementRequired => {
            "the exact break-glass acknowledgement was not supplied".to_string()
        }
        DfsPlacementAuthorityRecoveryRefusal::NormalRecoveryRequired => {
            "the identified removed-node permit must use normal recovery".to_string()
        }
    }
}

fn run_dfs_metadata_shard_transfer_primary(
    options: DfsMetadataShardTransferPrimaryOptions,
) -> Result<(), CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    default_main_daemon_status()?.ok_or_else(|| CliError::DaemonNotRunning {
        detail: "DFS metadata shard transfer requires the local main daemon; offline cluster mutation is reserved for explicit recovery"
            .to_string(),
    })?;
    let mut kv = SifsControlPlaneKvClient::new(&data_dir, &cluster_paths);
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

fn run_dfs_status_via_daemon_address(
    address: &str,
    timeout: std::time::Duration,
) -> Result<sinomo_runtime::dfs::DfsOperatorStatus, CliError> {
    let rpc_request = sinomo_runtime::dfs::dfs_status_rpc_request()?;
    let response = daemon_send_daemon_rpc(address, &rpc_request, timeout)?;
    sinomo_runtime::dfs::decode_dfs_status_rpc_response(&response).map_err(Into::into)
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
        call: sinomo_runtime::network::ObjectRpcCall,
        options: sinomo_runtime::dfs::DfsObjectRpcSendOptions,
    ) -> Result<sinomo_runtime::network::ObjectRpcResult, sinomo_runtime::dfs::DfsPeerTransportError>
    {
        daemon_dfs_object_rpc_to_target(target_node_id, call, options)
            .map_err(|error| sinomo_runtime::dfs::DfsPeerTransportError::new(error.to_string()))
    }
}

fn daemon_dfs_object_rpc_to_target(
    target_node_id: &sinomo_runtime::node::NodeId,
    call: sinomo_runtime::network::ObjectRpcCall,
    options: sinomo_runtime::dfs::DfsObjectRpcSendOptions,
) -> Result<sinomo_runtime::network::ObjectRpcResult, CliError> {
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::Network {
        address: target_node_id.to_string(),
        detail: "DFS cluster object RPC requires the local daemon to be running".to_string(),
    })?;
    let client = DaemonApiClient::at_address(status.address);
    let send = async {
        let mut result = client
            .object_rpc_call_to_target_with_deadline_and_affinity_async(
                target_node_id,
                call,
                options.connect_timeout,
                options.overall_timeout,
                options.observe_connectivity,
                sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            )
            .await
            .map_err(CliError::from)?;
        // This function is the synchronous CLI compatibility boundary. Keep a
        // semantic attachment stream-native, but gather an ordinary typed
        // value before a temporary runtime can be dropped.
        result
            .materialize_value_body(usize::MAX)
            .await
            .map_err(CliError::from)?;
        Ok::<_, CliError>(result)
    };
    block_on_daemon_dfs_rpc(target_node_id, send)
}

fn daemon_dfs_value_rpc_to_target<Req, Resp>(
    target_node_id: &sinomo_runtime::node::NodeId,
    route: &'static str,
    category: &'static str,
    value: Req,
    options: sinomo_runtime::dfs::DfsObjectRpcSendOptions,
) -> Result<Resp, CliError>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::Network {
        address: target_node_id.to_string(),
        detail: "DFS cluster object RPC requires the local daemon to be running".to_string(),
    })?;
    let client = DaemonApiClient::at_address(status.address);
    let call = sinomo_runtime::network::ObjectRpcCall::encode_value(route, category, value)?;
    let send = async {
        client
            .object_rpc_call_to_target_with_deadline_and_affinity_async(
                target_node_id,
                call,
                options.connect_timeout,
                options.overall_timeout,
                options.observe_connectivity,
                sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            )
            .await
            .map_err(CliError::from)?
            .decode_body::<Resp>()
            .await
            .map_err(CliError::from)
    };
    block_on_daemon_dfs_rpc(target_node_id, send)
}

fn block_on_daemon_dfs_rpc<T>(
    target_node_id: &sinomo_runtime::node::NodeId,
    send: impl std::future::Future<Output = Result<T, CliError>>,
) -> Result<T, CliError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| handle.block_on(send))
        } else {
            Err(CliError::Network {
                address: target_node_id.to_string(),
                detail: "DFS peer RPC cannot block a current-thread Tokio runtime".to_string(),
            })
        };
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| CliError::Network {
            address: target_node_id.to_string(),
            detail: source.to_string(),
        })?;
    runtime.block_on(send)
}

fn publish_dfs_metadata_journal_from_data_dir(
    data_dir: &SinomoDataDir,
) -> Result<sinomo_runtime::dfs::DfsMetadataJournalReplicationReport, CliError> {
    let cluster_paths = sinomo_runtime::network::ClusterPaths::new(data_dir);
    let options = sinomo_runtime::dfs::DfsMetadataJournalReplicationOptions {
        probe_caught_up_replicas: true,
        ..sinomo_runtime::dfs::DfsMetadataJournalReplicationOptions::default()
    };
    Ok(
        sinomo_runtime::dfs::publish_cluster_dfs_metadata_journal_with_peer_transport(
            data_dir,
            &cluster_paths,
            cli_dfs_peer_transport(),
            options,
        )?,
    )
}

pub(crate) fn run_dfs_mount_command(options: DfsMountOptions) -> Result<(), CliError> {
    validate_dfs_mount_options_for_platform(&options)?;
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

pub(crate) fn validate_dfs_mount_options_for_platform(
    options: &DfsMountOptions,
) -> Result<(), CliError> {
    #[cfg(not(windows))]
    {
        if !options.foreground {
            return Err(CliError::Parse {
                detail: "`--background` is not supported by the Unix FUSE mount yet; keep this command running in a supervised terminal or service".to_string(),
            });
        }
        if options.sync_local_changes {
            return Err(CliError::Parse {
                detail: "`--sync-local-changes` is only supported by Windows Cloud Files; Unix FUSE writes are already sent directly to SIFS".to_string(),
            });
        }
    }
    #[cfg(windows)]
    {
        let _ = options;
    }
    Ok(())
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
        request_body: None,
        response_body_path: None,
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
    let daemon_status = main_daemon_status_for_data_dir(&data_dir)?;
    let local_status = inspect_local_cluster_dfs_status_from_owner(
        daemon_status.as_ref().map(|status| status.address.as_str()),
        |daemon_address| {
            run_dfs_status_via_daemon_address(daemon_address, DFS_CLUSTER_STATUS_REMOTE_TIMEOUT)
        },
        || {
            sinomo_runtime::dfs::inspect_cluster_aware_dfs_status(&data_dir, &cluster_paths)
                .map_err(CliError::from)
        },
    )?;
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
    let route_graph = network::ClusterConnectivityGraph::read_for_local(&cluster_paths, &state);
    let mut probes = Vec::new();
    for node_id in remote_node_ids {
        if let Some(error) =
            dfs_cluster_status_unavailable_reason(&cluster_paths, &state, &route_graph, &node_id)
        {
            probes.push(DfsStatusProbePlan::Unavailable { node_id, error });
        } else {
            probes.push(DfsStatusProbePlan::Probe(spawn_dfs_status_probe(
                &cluster_paths,
                &state,
                &node_id,
                remote_timeout,
                trace,
            )?));
        }
    }
    for probe in probes {
        nodes.push(match probe {
            DfsStatusProbePlan::Unavailable { node_id, error } => DfsClusterNodeStatus {
                node_id,
                local: false,
                endpoint: None,
                status: None,
                error: Some(error),
            },
            DfsStatusProbePlan::Probe(probe) => {
                let node_id = probe.node_id.clone();
                match receive_dfs_status_probe(probe, trace, started_at) {
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
                }
            }
        });
    }
    trace_dfs_cluster_status(
        trace,
        started_at,
        format_args!("complete nodes={}", nodes.len()),
    );
    Ok(DfsClusterStatusReport { nodes })
}

fn inspect_local_cluster_dfs_status_from_owner<T>(
    daemon_address: Option<&str>,
    inspect_daemon: impl FnOnce(&str) -> Result<T, CliError>,
    inspect_standalone: impl FnOnce() -> Result<T, CliError>,
) -> Result<T, CliError> {
    match daemon_address {
        Some(address) => inspect_daemon(address),
        None => inspect_standalone(),
    }
}

#[cfg(test)]
mod local_cluster_dfs_status_tests {
    use super::inspect_local_cluster_dfs_status_from_owner;
    use std::cell::Cell;

    #[test]
    fn local_cluster_status_uses_daemon_owner_instead_of_standalone_storage() {
        let daemon_calls = Cell::new(0_u32);
        let standalone_calls = Cell::new(0_u32);

        let selected = inspect_local_cluster_dfs_status_from_owner(
            Some("127.0.0.1:21353"),
            |address| {
                daemon_calls.set(daemon_calls.get() + 1);
                Ok(address.to_string())
            },
            || {
                standalone_calls.set(standalone_calls.get() + 1);
                Ok("standalone".to_string())
            },
        )
        .expect("daemon-owned status");

        assert_eq!(selected, "127.0.0.1:21353");
        assert_eq!(daemon_calls.get(), 1);
        assert_eq!(standalone_calls.get(), 0);

        let selected = inspect_local_cluster_dfs_status_from_owner(
            None,
            |_| {
                daemon_calls.set(daemon_calls.get() + 1);
                Ok("daemon".to_string())
            },
            || {
                standalone_calls.set(standalone_calls.get() + 1);
                Ok("standalone".to_string())
            },
        )
        .expect("offline standalone status");

        assert_eq!(selected, "standalone");
        assert_eq!(daemon_calls.get(), 1);
        assert_eq!(standalone_calls.get(), 1);
    }
}

pub(crate) fn dfs_cluster_status_unavailable_reason(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    route_graph: &network::ClusterConnectivityGraph,
    node_id: &NodeId,
) -> Option<String> {
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)?;
    let connectivity =
        network::cluster_member_main_daemon_connectivity(cluster_paths, state, member);
    if matches!(
        connectivity,
        network::ClusterMemberMainDaemonConnectivity::Healthy
            | network::ClusterMemberMainDaemonConnectivity::Unknown
    ) || network::cluster_nodes_share_healthy_component(
        route_graph,
        &state.local_node_id,
        node_id,
    ) {
        return None;
    }
    match connectivity {
        network::ClusterMemberMainDaemonConnectivity::Degraded => Some(format!(
            "cluster member {node_id} has no currently healthy main-daemon route"
        )),
        network::ClusterMemberMainDaemonConnectivity::Unreachable => {
            Some(format!("cluster member {node_id} is currently unavailable"))
        }
        network::ClusterMemberMainDaemonConnectivity::Healthy
        | network::ClusterMemberMainDaemonConnectivity::Unknown => None,
    }
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

struct DfsStatusProbe {
    node_id: NodeId,
    started_at: std::time::Instant,
    timeout: std::time::Duration,
    receiver: std::sync::mpsc::Receiver<Result<sinomo_runtime::dfs::DfsOperatorStatus, CliError>>,
}

enum DfsStatusProbePlan {
    Probe(DfsStatusProbe),
    Unavailable { node_id: NodeId, error: String },
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
    let response = daemon_dfs_value_rpc_to_target(
        node_id,
        sinomo_runtime::dfs::DFS_STATUS_ROUTE,
        sinomo_runtime::dfs::DFS_RPC_CATEGORY,
        (),
        sinomo_runtime::dfs::DfsObjectRpcSendOptions::default().with_overall_timeout(timeout),
    );
    if trace {
        let outcome = if response.is_ok() { "ok" } else { "error" };
        eprintln!(
            "sinomo dfs cluster status node={node_id} +{}ms: rpc {outcome}",
            started_at.elapsed().as_millis()
        );
    }
    let decoded = response;
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
