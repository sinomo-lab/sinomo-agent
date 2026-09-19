//! DFS command grammar and intent. The host supplies mount and transport adapters.
use crate::{DfsFileSystemId, DfsObjectId, DfsOperatorInspectionCursor, DfsPinMode, DfsVersion};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use sinomo_api::dfs::sifs_file_system_id;
use sinomo_cli_registry::*;
use sinomo_core::node::NodeId;
use std::path::PathBuf;
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsCliCommand {
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
    RetryRepairs {
        target: Option<NodeId>,
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
    Nfs(DfsNfsCliCommand),
    Services {
        json: bool,
    },
    Pin(DfsPinCliCommand),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsNfsCliCommand {
    Serve(DfsNfsServerCliOptions),
    Mount(DfsNfsMountCliOptions),
    Unmount { target: String },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsNfsServerCliOptions {
    pub bind: String,
    pub export_name: String,
    pub root_path: String,
    pub data_dir: Option<PathBuf>,
    pub read_only: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsNfsMountCliOptions {
    pub target: String,
    pub server: DfsNfsServerCliOptions,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct DfsCommonOptions {
    pub metadata_root: Option<PathBuf>,
    pub cluster: bool,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct DfsFilterOptions {
    pub common: DfsCommonOptions,
    pub file_system_id: Option<DfsFileSystemId>,
    pub object_id: Option<DfsObjectId>,
    pub limit: Option<usize>,
    pub cursor: Option<DfsOperatorInspectionCursor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsConflictResolveOptions {
    pub common: DfsCommonOptions,
    pub file_system_id: DfsFileSystemId,
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub conflict_ids: Vec<String>,
    pub delete_file: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsConflictVersionsOptions {
    pub common: DfsCommonOptions,
    pub file_system_id: DfsFileSystemId,
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub conflict_ids: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsConflictUseVersionOptions {
    pub common: DfsCommonOptions,
    pub file_system_id: DfsFileSystemId,
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub version: DfsVersion,
    pub conflict_ids: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsConflictSaveVersionOptions {
    pub common: DfsCommonOptions,
    pub file_system_id: DfsFileSystemId,
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub version: DfsVersion,
    pub conflict_ids: Vec<String>,
    pub destination_path: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsMountOptions {
    Mount(DfsHostMountOptions),
    Status {
        json: bool,
    },
    Disable {
        json: bool,
    },
    RecoverCursorOnlyState {
        request: crate::SifsMountCursorOnlyRecoveryRequest,
        json: bool,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsHostMountOptions {
    pub mount_point: PathBuf,
    pub root_path: String,
    pub data_dir: Option<PathBuf>,
    pub read_only: bool,
    pub allow_other: bool,
    pub foreground: bool,
    pub sync_local_changes: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsMetadataShardTransferPrimaryOptions {
    pub file_system_id: DfsFileSystemId,
    pub shard_id: String,
    pub new_primary_node_id: NodeId,
    pub expected_epoch: Option<u64>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsPlacementAuthorityCliCommand {
    Inspect(DfsPlacementAuthorityInspectOptions),
    Clear(DfsPlacementAuthorityClearOptions),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsPlacementAuthorityInspectOptions {
    pub file_system_id: DfsFileSystemId,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsPlacementAuthorityClearOptions {
    pub file_system_id: DfsFileSystemId,
    pub expected_recovery_token: String,
    pub recovery: DfsPlacementAuthorityRecovery,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsPlacementAuthorityRecovery {
    RemovedNode,
    BreakGlass { acknowledgement: String },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DfsPinCliCommand {
    List(DfsPinListOptions),
    Add(DfsPinAddOptions),
    Remove(DfsPinRemoveOptions),
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct DfsPinListOptions {
    pub pin_root: Option<PathBuf>,
    pub file_system_id: Option<DfsFileSystemId>,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsPinAddOptions {
    pub pin_root: Option<PathBuf>,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub object_id: Option<DfsObjectId>,
    pub node_id: Option<NodeId>,
    pub mode: DfsPinMode,
    pub json: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DfsPinRemoveOptions {
    pub pin_root: Option<PathBuf>,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub node_id: Option<NodeId>,
    pub mode: Option<DfsPinMode>,
    pub json: bool,
}

pub fn dfs_cli_command_handler<E: Into<CliCommandError> + 'static>(
    run: fn(DfsCliCommand) -> Result<(), E>,
) -> DeclarativeCommandHandler<DfsCliCommand, E> {
    DeclarativeCommandHandler::new(
        "dfs",
        CliCommandPolicy::local_capable(),
        dfs_cli_spec,
        parse_dfs_matches,
        run,
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
    let nfs_options = || {
        [
            option("--bind", "loopback NFS listen address", "address"),
            option("--export", "NFS export name", "name"),
            option("--root", "SIFS root path", "path").with_aliases(["--path"]),
            option("--data-dir", "Sinomo data directory", "path"),
            CliOptionSpec::flag("--read-only", "serve the export read-only").with_aliases(["--ro"]),
        ]
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
            CliCommandSpec::new("repair", "list inferred replica repairs").with_options(filter())
                .with_children([CliCommandSpec::new("retry", "reconstruct pending repairs through the resident DFS owner")
                    .with_options([option("--target", "target cluster node", "node-id"), json()])]),
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
                        crate::DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT
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
                                crate::DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT
                            ),
                            CliValueSpec::new("text", "exact break-glass acknowledgement"),
                        ),
                        json(),
                    ]),
            ]),
            CliCommandSpec::new("mount", "expose SIFS locally")
                .with_options([
                    CliOptionSpec::flag(
                        "--status",
                        "query the local daemon's live SIFS mount supervisor status",
                    ),
                    CliOptionSpec::flag(
                        "--disable",
                        "disable the local supervised mount while preserving its identity",
                    ),
                    CliOptionSpec::flag(
                        "--recover-cursor-only-state",
                        "inspect or explicitly adopt retained external-provider cursor state",
                    ),
                    option(
                        "--expected-token",
                        "exact cursor-only recovery token returned by inspection",
                        "token",
                    ),
                    CliOptionSpec::flag(
                        "--acknowledge-authoritative-full-reconciliation",
                        "authorize clearing retained cursors and rebuilding them by authoritative full reconciliation",
                    ),
                    json(),
                    option("--root", "SIFS root path", "path").with_aliases(["--path"]),
                    option("--data-dir", "Sinomo data directory", "path"),
                    CliOptionSpec::flag(
                        "--read-only",
                        "mount read-only (Unix FUSE; external Windows providers may require durable writable sync)",
                    )
                    .with_aliases(["--ro"]),
                    CliOptionSpec::flag("--allow-other", "allow other local users"),
                    CliOptionSpec::flag("--foreground", "stay in the foreground"),
                    CliOptionSpec::flag(
                        "--background",
                        "configure the main daemon to supervise this mount",
                    ),
                    CliOptionSpec::flag("--sync-local-changes", "sync local mount changes"),
                    CliOptionSpec::flag(
                        "--no-sync-local-changes",
                        "disable local change synchronization",
                    ),
                ])
                .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                    "mount-point",
                    "local mount directory",
                ))]),
            CliCommandSpec::new("nfs", "expose SIFS directly through loopback NFSv3")
                .with_children([
                    CliCommandSpec::new("serve", "run the loopback SIFS NFS server")
                        .with_options(nfs_options()),
                    CliCommandSpec::new(
                        "mount",
                        "run the server and mount it through the native NFS client",
                    )
                    .with_options(nfs_options())
                    .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                        "target",
                        "Linux directory or Windows drive letter",
                    ))]),
                    CliCommandSpec::new("unmount", "unmount a native NFS mapping")
                        .with_positionals([CliPositionalSpec::required(CliValueSpec::new(
                            "target",
                            "Linux directory or Windows drive letter",
                        ))]),
                ]),
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
    pub common: DfsCommonOptions,
    pub file_system_id: DfsFileSystemId,
    pub path: Option<String>,
    pub object_id: Option<DfsObjectId>,
    pub conflict_ids: Vec<String>,
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
        ["dfs", "repair", "retry"] => {
            if let Some(option) = [
                "--metadata-root",
                "--filesystem",
                "--object",
                "--limit",
                "--cursor",
            ]
            .into_iter()
            .find(|option| matches.contains(option))
            {
                return Err(semantic_parse_error(CliParseError::Parse {
                    detail: format!("dfs repair retry does not accept inspection option {option}"),
                }));
            }
            Ok(DfsCliCommand::RetryRepairs {
                target: matches
                    .value("--target")
                    .map(parse_node_id)
                    .transpose()
                    .map_err(semantic_parse_error)?,
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
                        == crate::DFS_PLACEMENT_AUTHORITY_BREAK_GLASS_ACKNOWLEDGEMENT =>
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
        ["dfs", "nfs", "serve"] => Ok(DfsCliCommand::Nfs(DfsNfsCliCommand::Serve(
            parse_dfs_nfs_server_matches(matches, "127.0.0.1:111"),
        ))),
        ["dfs", "nfs", "mount"] => Ok(DfsCliCommand::Nfs(DfsNfsCliCommand::Mount(
            DfsNfsMountCliOptions {
                target: required_positional(matches, 0)?.to_string(),
                server: parse_dfs_nfs_server_matches(matches, default_nfs_mount_bind()),
            },
        ))),
        ["dfs", "nfs", "unmount"] => Ok(DfsCliCommand::Nfs(DfsNfsCliCommand::Unmount {
            target: required_positional(matches, 0)?.to_string(),
        })),
        ["dfs", "pin", command] => parse_dfs_pin_matches(command, matches).map(DfsCliCommand::Pin),
        _ => Err(CliParseError::Parse {
            detail: "dfs requires a subcommand".to_string(),
        }),
    }
}

fn parse_dfs_nfs_server_matches(
    matches: &CliCommandMatches,
    default_bind: &str,
) -> DfsNfsServerCliOptions {
    DfsNfsServerCliOptions {
        bind: matches.value("--bind").unwrap_or(default_bind).to_string(),
        export_name: matches.value("--export").unwrap_or("sifs").to_string(),
        root_path: matches.value("--root").unwrap_or("/").to_string(),
        data_dir: matches.value("--data-dir").map(PathBuf::from),
        read_only: matches.contains("--read-only"),
    }
}

pub const fn default_nfs_mount_bind() -> &'static str {
    if cfg!(target_os = "linux") {
        "127.0.0.1:0"
    } else {
        "127.0.0.1:111"
    }
}

fn parse_dfs_mount_matches(matches: &CliCommandMatches) -> Result<DfsMountOptions, CliParseError> {
    let status = matches.contains("--status");
    let disable = matches.contains("--disable");
    let recover_cursor_only_state = matches.contains("--recover-cursor-only-state");
    let expected_token = matches.value("--expected-token");
    let acknowledge_authoritative_full_reconciliation =
        matches.contains("--acknowledge-authoritative-full-reconciliation");
    if !recover_cursor_only_state
        && (expected_token.is_some() || acknowledge_authoritative_full_reconciliation)
    {
        return Err(CliParseError::Parse {
            detail: "dfs mount --expected-token and --acknowledge-authoritative-full-reconciliation require --recover-cursor-only-state"
                .to_string(),
        });
    }
    if usize::from(status) + usize::from(disable) + usize::from(recover_cursor_only_state) > 1 {
        return Err(CliParseError::Parse {
            detail: "dfs mount requires exactly one of --status, --disable, or --recover-cursor-only-state"
                .to_string(),
        });
    }
    if status || disable || recover_cursor_only_state {
        if !matches.positionals().is_empty() {
            return Err(CliParseError::Parse {
                detail: "dfs mount control operations do not accept a mount-point".to_string(),
            });
        }
        const MOUNT_ONLY_OPTIONS: [&str; 8] = [
            "--root",
            "--data-dir",
            "--read-only",
            "--allow-other",
            "--foreground",
            "--background",
            "--sync-local-changes",
            "--no-sync-local-changes",
        ];
        if let Some(option) = MOUNT_ONLY_OPTIONS
            .into_iter()
            .find(|option| matches.contains(option))
        {
            return Err(CliParseError::Parse {
                detail: format!(
                    "dfs mount {} cannot be combined with mount option {option}",
                    if status {
                        "--status"
                    } else if disable {
                        "--disable"
                    } else {
                        "--recover-cursor-only-state"
                    }
                ),
            });
        }
        return Ok(match (status, disable) {
            (true, false) => DfsMountOptions::Status {
                json: matches.contains("--json"),
            },
            (false, true) => DfsMountOptions::Disable {
                json: matches.contains("--json"),
            },
            (false, false) => {
                let request = match (
                    expected_token,
                    acknowledge_authoritative_full_reconciliation,
                ) {
                    (None, false) => crate::SifsMountCursorOnlyRecoveryRequest::Inspect,
                    (Some(expected_token), true)
                        if !expected_token.trim().is_empty()
                            && expected_token.trim() == expected_token =>
                    {
                        crate::SifsMountCursorOnlyRecoveryRequest::Apply {
                            expected_token: expected_token.to_string(),
                            acknowledge_authoritative_full_reconciliation: true,
                        }
                    }
                    (Some(_), true) => {
                        return Err(CliParseError::Parse {
                            detail: "dfs mount --expected-token must be non-empty without surrounding whitespace"
                                .to_string(),
                        });
                    }
                    _ => {
                        return Err(CliParseError::Parse {
                            detail: "dfs mount cursor-only recovery apply requires both --expected-token and --acknowledge-authoritative-full-reconciliation; omit both for read-only inspection"
                                .to_string(),
                        });
                    }
                };
                DfsMountOptions::RecoverCursorOnlyState {
                    request,
                    json: matches.contains("--json"),
                }
            }
            (true, true) => unreachable!("mutually exclusive mount controls were rejected"),
        });
    }
    if matches.contains("--json") {
        return Err(CliParseError::Parse {
            detail: "dfs mount --json requires --status, --disable, or --recover-cursor-only-state"
                .to_string(),
        });
    }
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
    let sync_local_changes = matches.contains("--sync-local-changes");
    Ok(DfsMountOptions::Mount(DfsHostMountOptions {
        mount_point: PathBuf::from(required_positional(matches, 0)?),
        root_path: matches.value("--root").unwrap_or("/").to_string(),
        data_dir: matches.value("--data-dir").map(PathBuf::from),
        read_only: matches.contains("--read-only") || (cfg!(windows) && !sync_local_changes),
        allow_other: matches.contains("--allow-other"),
        foreground: !matches.contains("--background"),
        sync_local_changes,
    }))
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

pub fn parse_dfs_file_system_id(value: &str) -> Result<DfsFileSystemId, CliParseError> {
    DfsFileSystemId::new(value).map_err(|source| CliParseError::Parse {
        detail: format!("invalid DFS filesystem id `{value}`: {source}"),
    })
}

pub fn default_dfs_file_system_id() -> DfsFileSystemId {
    sifs_file_system_id()
}

pub fn parse_dfs_object_id(value: &str) -> Result<DfsObjectId, CliParseError> {
    DfsObjectId::new(value).map_err(|source| CliParseError::Parse {
        detail: format!("invalid DFS object id `{value}`: {source}"),
    })
}

pub fn parse_dfs_version(value: &str) -> Result<DfsVersion, CliParseError> {
    DfsVersion::new(value).map_err(|source| CliParseError::Parse {
        detail: format!("invalid DFS version `{value}`: {source}"),
    })
}

pub fn parse_node_id(value: &str) -> Result<NodeId, CliParseError> {
    NodeId::new(value).map_err(|source| CliParseError::Parse {
        detail: format!("invalid node id `{value}`: {source}"),
    })
}

pub fn parse_dfs_pin_mode(value: &str) -> Result<DfsPinMode, CliParseError> {
    match value {
        "metadata" => Ok(DfsPinMode::Metadata),
        "content" => Ok(DfsPinMode::Content),
        _ => Err(CliParseError::Parse {
            detail: format!("invalid DFS pin mode `{value}`"),
        }),
    }
}

pub fn non_empty_string(value: &str, field: &str) -> Result<String, CliParseError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CliParseError::Parse {
            detail: format!("{field} cannot be empty"),
        });
    }
    Ok(trimmed.to_string())
}

pub fn decode_dfs_inspection_cursor(
    cursor: &str,
) -> Result<DfsOperatorInspectionCursor, CliParseError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|source| CliParseError::Parse {
            detail: format!("invalid DFS inspection cursor: {source}"),
        })?;
    serde_json::from_slice(&bytes).map_err(|source| CliParseError::Parse {
        detail: format!("invalid DFS inspection cursor: {source}"),
    })
}
pub fn dfs_command_requires_cluster_state(command: &DfsCliCommand) -> bool {
    match command {
        DfsCliCommand::Status(options) => options.cluster,
        DfsCliCommand::Services { .. }
        | DfsCliCommand::MetadataPublish { .. }
        | DfsCliCommand::RetryRepairs { .. }
        | DfsCliCommand::MetadataShardTransferPrimary(_) => true,
        _ => false,
    }
}
/// Parse this domain's arguments without executing a command.
pub fn parse_dfs_command(args: &[String]) -> Result<DfsCliCommand, CliParseError> {
    parse_command_args(dfs_cli_spec(), args, parse_dfs_matches)
}

/// Register the owner's grammar with a host-supplied execution adapter.
pub fn register_cli_commands<E: Into<CliCommandError> + 'static>(
    registry: CliCommandRegistry,
    run: fn(DfsCliCommand) -> Result<(), E>,
) -> Result<CliCommandRegistry, String> {
    registry.try_register(dfs_cli_command_handler(run))
}

#[cfg(test)]
mod repair_retry_tests {
    use super::*;
    #[test]
    fn repair_retry_is_an_explicit_online_command() {
        let target = NodeId::from_legacy_named("repair-retry-cli-target").unwrap();
        let args = ["repair", "retry", "--target", target.as_str(), "--json"].map(String::from);
        let parsed = parse_dfs_command(&args).unwrap();
        assert!(dfs_command_requires_cluster_state(&parsed));
        assert!(
            matches!(parsed, DfsCliCommand::RetryRepairs { target: Some(node), json: true } if node == target)
        );
        let offline = ["repair", "retry", "--metadata-root", "/tmp/offline"].map(String::from);
        assert!(parse_dfs_command(&offline).is_err());
        let inherited = ["repair", "--object", target.as_str(), "retry"].map(String::from);
        assert!(parse_dfs_command(&inherited).is_err());
        let inspect = ["repair"].map(String::from);
        assert!(matches!(
            parse_dfs_command(&inspect).unwrap(),
            DfsCliCommand::Repair(_)
        ));
    }
}
