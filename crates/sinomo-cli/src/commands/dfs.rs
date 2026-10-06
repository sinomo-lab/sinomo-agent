use super::*;
use sinomo_runtime::dfs_history::DfsHistoryExt;
mod attachments;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
#[cfg(test)]
pub(crate) use sinomo_runtime::dfs::cli::DfsFilterOptions;
#[cfg(test)]
pub(crate) use sinomo_runtime::dfs::cli::DfsPlacementAuthorityClearOptions;
#[cfg(test)]
pub(crate) use sinomo_runtime::dfs::cli::DfsPlacementAuthorityInspectOptions;
#[cfg(test)]
pub(crate) use sinomo_runtime::dfs::cli::default_dfs_file_system_id;
#[cfg(test)]
pub(crate) use sinomo_runtime::dfs::cli::default_nfs_mount_bind;
pub(crate) use sinomo_runtime::dfs::cli::{
    DfsCommonOptions, DfsConflictResolveOptions, DfsConflictSaveVersionOptions,
    DfsConflictUseVersionOptions, DfsConflictVersionsOptions, DfsHostMountOptions,
    DfsMetadataShardTransferPrimaryOptions, DfsMountOptions, DfsNfsCliCommand,
    DfsNfsMountCliOptions, DfsNfsServerCliOptions, DfsPinCliCommand,
    DfsPlacementAuthorityCliCommand, DfsPlacementAuthorityRecovery,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum DfsFeatureCliCommand {
    Attachment(sinomo_runtime::dfs_attachments::cli::DfsAttachmentCliOptions),
    History(sinomo_runtime::dfs_history::cli::DfsFileHistoryCliCommand),
    HistoryPin(sinomo_runtime::dfs_history::cli::DfsFileHistoryPinOptions),
    HistoryRetention(sinomo_runtime::dfs_history::cli::DfsFileHistoryRetentionOptions),
}
pub(crate) type DfsCliCommand = sinomo_runtime::dfs::cli::DfsCliCommand<DfsFeatureCliCommand>;

fn product_dfs_cli_spec() -> sinomo_runtime::cli_registry::CliCommandSpec {
    let mut spec = sinomo_runtime::dfs::cli::dfs_cli_spec();
    spec.children.extend([
        sinomo_runtime::dfs_attachments::cli::cli_spec(),
        sinomo_runtime::dfs_history::cli::cli_spec(),
    ]);
    spec
}

fn parse_product_dfs_matches(
    matches: &sinomo_runtime::cli_registry::CliCommandMatches,
) -> Result<DfsCliCommand, CliParseError> {
    match matches.path().get(1).map(String::as_str) {
        Some("attachment") => sinomo_runtime::dfs_attachments::cli::parse_matches(matches)
            .map(|command| DfsCliCommand::Extension(DfsFeatureCliCommand::Attachment(command))),
        Some("history") => {
            sinomo_runtime::dfs_history::cli::parse_matches(matches).map(|command| {
                use sinomo_runtime::dfs_history::cli::DfsHistoryCliCommand;
                let command = match command {
                    DfsHistoryCliCommand::History(value) => DfsFeatureCliCommand::History(value),
                    DfsHistoryCliCommand::Pin(value) => DfsFeatureCliCommand::HistoryPin(value),
                    DfsHistoryCliCommand::Retention(value) => {
                        DfsFeatureCliCommand::HistoryRetention(value)
                    }
                };
                DfsCliCommand::Extension(command)
            })
        }
        _ => sinomo_runtime::dfs::cli::parse_dfs_matches(matches),
    }
}

pub(crate) fn register_product_dfs_cli_commands(
    registry: CliCommandRegistry,
) -> Result<CliCommandRegistry, String> {
    use sinomo_runtime::cli_registry::{CliCommandPolicy, DeclarativeCommandHandler};
    registry.try_register(
        DeclarativeCommandHandler::new(
            "dfs",
            CliCommandPolicy::local_capable(),
            product_dfs_cli_spec,
            parse_product_dfs_matches,
            run_dfs_cli_command,
        )
        .with_parsed_policy(|command| {
            if sinomo_runtime::dfs::cli::dfs_command_requires_cluster_state(command) {
                CliCommandPolicy::cluster_required()
            } else {
                CliCommandPolicy::local_capable()
            }
        }),
    )
}

#[allow(unused_imports)]
use sinomo_runtime::cluster;
use sinomo_runtime::cluster::ClusterKvPathsExt;

use crate::sifs_file_manager::SifsControlPlaneKvClient;

const DFS_CLUSTER_STATUS_REMOTE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);
const DFS_SMOKE_DAEMON_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(9_500);
const DFS_PLACEMENT_AUTHORITY_ADMIN_RPC_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_handoff: Option<sinomo_runtime::dfs::DfsMetadataShardMembershipHandoff>,
    /// Current assignment. A pending handoff's desired assignment is not active.
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

#[cfg(test)]
pub(crate) fn parse_dfs_schema_for_test(args: &[String]) -> Result<DfsCliCommand, CliError> {
    sinomo_runtime::cli_registry::parse_command_args(
        product_dfs_cli_spec(),
        args,
        parse_product_dfs_matches,
    )
    .map_err(Into::into)
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
        DfsCliCommand::RetryRepairs {
            target,
            json,
            compact_namespace_jobs,
        } => {
            let client = DaemonApiClient::connect().map_err(|error| CliError::Parse {
                detail: error.to_string(),
            })?;
            let request = sinomo_runtime::api::dfs::DfsRepairRetryRequest {
                request_id: Id::generate(),
                compact_namespace_jobs,
            };
            let rpc = sinomo_runtime::network::ObjectRpcRequest::encode_body(
                sinomo_runtime::api::dfs::DFS_REPAIR_RETRY_ROUTE,
                sinomo_runtime::api::dfs::DFS_RPC_CATEGORY,
                &request,
            )?;
            let response = match target {
                Some(target) => client.object_rpc_to_target_with_deadline_and_affinity(
                    &target,
                    &rpc,
                    Duration::from_secs(5),
                    Some(Duration::from_secs(30)),
                    false,
                    sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                ),
                None => client.call_request(&rpc),
            }
            .map_err(|error| CliError::Parse {
                detail: error.to_string(),
            })?;
            let response: sinomo_runtime::api::dfs::DfsRepairRetryResponse =
                response.decode_body()?;
            if response.request_id != request.request_id {
                return Err(CliError::Parse {
                    detail: "DFS repair retry response does not match the request".into(),
                });
            }
            if response.compact_namespace_jobs != request.compact_namespace_jobs {
                return Err(CliError::Parse {
                    detail: "DFS server did not accept the namespace job compaction scope".into(),
                });
            }
            if json {
                print_json("<dfs-repair-retry>", &response)?;
            } else {
                println!(
                    "Requested repair reconciliation. Use `sinomo dfs status --cluster` to check remaining work."
                );
            }
            Ok(())
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
        DfsCliCommand::Nfs(command) => run_dfs_nfs_command(command),
        DfsCliCommand::Pin(command) => run_dfs_pin_command(command),
        DfsCliCommand::Extension(DfsFeatureCliCommand::HistoryPin(options)) => {
            run_dfs_history_pin_command(options)
        }
        DfsCliCommand::Extension(DfsFeatureCliCommand::History(command)) => {
            run_dfs_history_command(command)
        }
        DfsCliCommand::Extension(DfsFeatureCliCommand::HistoryRetention(options)) => {
            run_dfs_history_retention_command(options)
        }
        DfsCliCommand::Extension(DfsFeatureCliCommand::Attachment(options)) => {
            attachments::run(options)
        }
    }
}

fn run_dfs_placement_authority_command(
    command: DfsPlacementAuthorityCliCommand,
) -> Result<(), CliError> {
    match command {
        DfsPlacementAuthorityCliCommand::Inspect(options) => {
            let response = send_dfs_placement_authority_admin_request(
                &sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminRequest::Inspect {
                    file_system_id: options.file_system_id.clone(),
                },
            )?;
            let sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminResponse::Inspection {
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
                    sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminRequest::RecoverRemovedNode {
                        file_system_id: options.file_system_id.clone(),
                        expected_recovery_token: options.expected_recovery_token.clone(),
                        acknowledge_owner_stopped: true,
                    }
                }
                DfsPlacementAuthorityRecovery::BreakGlass { acknowledgement } => {
                    sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminRequest::BreakGlassRecover {
                        file_system_id: options.file_system_id.clone(),
                        expected_recovery_token: options.expected_recovery_token.clone(),
                        acknowledgement: acknowledgement.clone(),
                    }
                }
            };
            let response = send_dfs_placement_authority_admin_request(&request)?;
            let sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminResponse::Recovery { outcome } =
                &response
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
    request: &sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminRequest,
) -> Result<sinomo_runtime::dfs::DaemonDfsPlacementAuthorityAdminResponse, CliError> {
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::DaemonNotRunning {
        detail: "DFS placement-authority administration requires the local main daemon".to_string(),
    })?;
    let rpc_request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::dfs::DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
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
    // Administrative fencing must observe the current authority, not the
    // materialized snapshot that may predate a completed primary handoff.
    sinomo_runtime::dfs::DfsControlPlaneKvClient::refresh_latest(&mut kv)?;
    let mut store = sinomo_runtime::dfs::DfsControlPlaneStore::new(&mut kv);
    let previous = store
        .get_metadata_shard(&options.file_system_id, &options.shard_id)?
        .ok_or_else(|| CliError::Parse {
            detail: format!(
                "DFS metadata shard {} for filesystem {} was not found",
                options.shard_id, options.file_system_id
            ),
        })?;

    if let Some(handoff) =
        store.get_metadata_shard_membership_handoff(&options.file_system_id, &options.shard_id)?
    {
        if handoff.desired_shard.primary_node_id != options.new_primary_node_id
            || options.expected_epoch.is_some_and(|epoch| {
                epoch != handoff.retiring_shard.epoch && epoch != previous.epoch
            })
        {
            return Err(CliError::ClusterReadOnly {
                detail: format!(
                    "DFS metadata shard {} already has a different pending primary handoff {}",
                    options.shard_id, handoff.handoff_id
                ),
            });
        }
        let report = pending_metadata_primary_transfer_report(previous, handoff, false);
        return print_dfs_metadata_shard_transfer_primary_report(&report, options.json);
    }

    if let Some(expected_epoch) = options.expected_epoch
        && previous.epoch != expected_epoch
    {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "DFS metadata shard {} epoch is {}, expected {}",
                options.shard_id, previous.epoch, expected_epoch
            ),
        });
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
            pending_handoff: None,
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
    let handoff =
        store.request_metadata_shard_primary_handoff(&previous, &options.new_primary_node_id)?;
    let Some(handoff) = handoff else {
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

    let report = pending_metadata_primary_transfer_report(previous, handoff, true);
    print_dfs_metadata_shard_transfer_primary_report(&report, options.json)?;
    Ok(())
}

fn pending_metadata_primary_transfer_report(
    current: DfsMetadataShardConfig,
    handoff: sinomo_runtime::dfs::DfsMetadataShardMembershipHandoff,
    changed: bool,
) -> DfsMetadataShardTransferPrimaryReport {
    DfsMetadataShardTransferPrimaryReport {
        file_system_id: current.file_system_id.clone(),
        shard_id: current.shard_id.clone(),
        previous_primary_node_id: handoff.retiring_shard.primary_node_id.clone(),
        new_primary_node_id: handoff.desired_shard.primary_node_id.clone(),
        previous_epoch: handoff.retiring_shard.epoch,
        new_epoch: handoff.desired_shard.epoch,
        changed,
        pending_handoff: Some(handoff),
        shard: current,
    }
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
    if options.common.metadata_root.is_some() {
        return Err(CliError::Parse {
            detail: "version restoration requires the main daemon and cannot use --metadata-root"
                .into(),
        });
    }
    let (object_id, mut path) =
        if let (Some(object_id), Some(path)) = (&options.object_id, &options.path) {
            (object_id.clone(), Some(path.clone()))
        } else {
            resolve_dfs_conflict_target(
                &options.common,
                &options.file_system_id,
                options.path.as_ref(),
                options.object_id.as_ref(),
            )?
        };
    let mut expected_version = options.expected_version;
    if path.is_none() || expected_version.is_none() {
        let DaemonDfsInspectionResponse::Object {
            object: Some(object),
        } = run_dfs_inspection(
            &options.common,
            DaemonDfsInspectionRequest::Object {
                object_id: object_id.clone(),
            },
        )?
        else {
            return Err(CliError::MissingArtifact {
                artifact: format!("DFS object {object_id}"),
            });
        };
        if object.object.file_system_id != options.file_system_id {
            return Err(CliError::Parse {
                detail: "restore target belongs to another filesystem".into(),
            });
        }
        path.get_or_insert(object.object.path);
        expected_version.get_or_insert(object.object.version);
    }
    run_dfs_history_command(
        sinomo_runtime::dfs_history::cli::DfsFileHistoryCliCommand::Restore {
            source: sinomo_runtime::dfs::DfsFileVersionIdentity {
                file_system_id: options.file_system_id.clone(),
                object_id: object_id.clone(),
                version: options.version,
            },
            path: path.expect("restore destination resolved"),
            expected: Some(sinomo_runtime::dfs::DfsFileVersionIdentity {
                file_system_id: options.file_system_id,
                object_id,
                version: expected_version.expect("restore expectation resolved"),
            }),
            recover: false,
            operation_id: options.operation_id,
            json: options.common.json,
        },
    )
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

fn run_dfs_history_command(
    command: sinomo_runtime::dfs_history::cli::DfsFileHistoryCliCommand,
) -> Result<(), CliError> {
    use sinomo_runtime::dfs::*;
    use sinomo_runtime::dfs_history::cli::DfsFileHistoryCliCommand;
    use std::io::Write;
    let fail = |detail: String| CliError::Parse { detail };
    match command {
        DfsFileHistoryCliCommand::List { request, json } => {
            let page: DfsFileHistoryListResponse =
                dfs_history_rpc(DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE, &request)?;
            let next_cursor = page
                .next_cursor
                .map(|cursor| {
                    serde_json::to_vec(&cursor).map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                })
                .transpose()
                .map_err(|error| fail(error.to_string()))?;
            if json {
                #[derive(Serialize)]
                struct Page {
                    entries: Vec<DfsFileHistoryEntry>,
                    next_cursor: Option<String>,
                }
                print_json(
                    "<dfs-file-history>",
                    &Page {
                        entries: page.entries,
                        next_cursor,
                    },
                )?;
            } else {
                for entry in page.entries {
                    println!(
                        "{} {} {} pins={}{}",
                        entry.identity.object_id,
                        entry.identity.version,
                        entry.captured.object.path,
                        entry.pins.join(","),
                        if entry.deleted { " deleted" } else { "" }
                    );
                }
                if let Some(cursor) = next_cursor {
                    println!("Next cursor: {cursor}");
                }
            }
        }
        DfsFileHistoryCliCommand::Read { identity, output } => {
            let mut writer: Box<dyn Write> = match output {
                Some(path) => Box::new(
                    std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)
                        .map_err(|error| {
                            fail(format!("cannot create {}: {error}", path.display()))
                        })?,
                ),
                None => Box::new(std::io::stdout()),
            };
            let mut offset = 0;
            loop {
                let response: DfsFileHistoryReadOutcome = dfs_history_rpc(
                    DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
                    &DfsFileHistoryReadRequest {
                        identity: identity.clone(),
                        offset,
                        max_bytes: 64 * 1024,
                    },
                )?;
                let data = match response {
                    DfsFileHistoryReadOutcome::Found { data } => data,
                    DfsFileHistoryReadOutcome::NotRetained { .. } => {
                        return Err(fail("source version is not retained".into()));
                    }
                    DfsFileHistoryReadOutcome::ContentUnavailable { detail, .. } => {
                        return Err(fail(detail));
                    }
                };
                if data.identity != identity
                    || data.offset != offset
                    || data.bytes.len() as u64
                        > data.size_bytes.saturating_sub(offset).min(64 * 1024)
                {
                    return Err(fail("history reader returned an invalid range".into()));
                }
                writer
                    .write_all(&data.bytes)
                    .map_err(|error| fail(error.to_string()))?;
                offset += data.bytes.len() as u64;
                if offset == data.size_bytes {
                    break;
                }
                if data.bytes.is_empty() {
                    return Err(fail("history reader returned an incomplete range".into()));
                }
            }
            writer.flush().map_err(|error| fail(error.to_string()))?;
        }
        DfsFileHistoryCliCommand::Restore {
            source,
            path,
            mut expected,
            recover,
            operation_id,
            json,
        } => {
            if !recover && expected.is_none() {
                let stat: DaemonDfsBrowserStatOutcomeResponse = dfs_history_rpc(
                    DAEMON_DFS_BROWSER_STAT_ROUTE,
                    &DaemonDfsBrowserStatRequest {
                        path: path.clone(),
                        max_staleness_millis: 0,
                    },
                )?;
                let stat = stat
                    .into_result()
                    .map_err(|error| fail(error.to_string()))?;
                expected = Some(DfsFileVersionIdentity {
                    file_system_id: DfsFileSystemId::new(stat.file_system_id)
                        .map_err(|error| fail(error.to_string()))?,
                    object_id: DfsObjectId::new(stat.object_id)
                        .map_err(|error| fail(error.to_string()))?,
                    version: DfsVersion::new(stat.version)
                        .map_err(|error| fail(error.to_string()))?,
                });
            }
            let operation_id =
                operation_id.unwrap_or_else(|| sinomo_runtime::core::Id::generate().to_string());
            let response: DfsFileHistoryRestoreResponse = dfs_history_rpc(DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE, &DfsFileHistoryRestoreRequest {
                source, destination_path: path, expected_destination: expected, operation_id: operation_id.clone(),
            }).map_err(|error| fail(format!("restore operation {operation_id}: {error}; retry only with the same source and destination expectations")))?;
            if json {
                print_json("<dfs-file-history-restore>", &response)?;
            } else {
                println!(
                    "Restored {} as version {} (operation {}).",
                    response.path, response.committed.version, operation_id
                );
            }
        }
    }
    Ok(())
}

fn dfs_history_rpc<Req: Serialize, Resp: serde::de::DeserializeOwned>(
    route: &'static str,
    payload: &Req,
) -> Result<Resp, CliError> {
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::Parse {
        detail: "file history commands require the main daemon".into(),
    })?;
    let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        route,
        sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
        payload,
    )?;
    Ok(
        daemon_send_daemon_rpc(&status.address, &request, Duration::from_secs(60))?
            .decode_body()?,
    )
}

fn run_dfs_history_retention_command(
    options: sinomo_runtime::dfs_history::cli::DfsFileHistoryRetentionOptions,
) -> Result<(), CliError> {
    use sinomo_runtime::dfs::*;
    let policy: DfsFileHistoryPolicy = dfs_history_rpc(
        DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE,
        &DfsFileHistoryPolicyRequest {
            file_system_id: options.file_system_id.clone(),
        },
    )?;
    let policy = if let Some(retention) = options.retention {
        let mut desired = policy.clone();
        desired.rules.retain(|rule| rule.path != options.path);
        desired.rules.push(DfsFileHistoryRule {
            path: options.path.clone(),
            retention,
        });
        desired
            .rules
            .sort_by(|left, right| left.path.cmp(&right.path));
        desired
            .validate()
            .map_err(|detail| CliError::Parse { detail })?;
        dfs_history_rpc(
            DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE,
            &DfsFileHistoryPolicyUpdateRequest {
                file_system_id: options.file_system_id,
                expected: policy,
                policy: desired,
            },
        )?
    } else {
        policy
    };
    if options.json {
        print_json("<dfs-file-history-policy>", &policy)?;
    } else {
        let effective = policy.retention_for_path(&options.path);
        if effective.is_disabled() {
            println!(
                "Automatic history is disabled for {}. Named pins remain retained.",
                options.path
            );
        } else {
            println!(
                "Retention for {}: newest {} captures or {} seconds. Named pins remain retained.",
                options.path, effective.keep_last, effective.keep_for_seconds
            );
        }
    }
    Ok(())
}

fn run_dfs_history_pin_command(
    options: sinomo_runtime::dfs_history::cli::DfsFileHistoryPinOptions,
) -> Result<(), CliError> {
    use sinomo_runtime::dfs::{
        DfsFileHistoryPinRequest, DfsFileHistoryPinResponse, DfsFileVersionIdentity,
    };
    let status = default_main_daemon_status()?.ok_or_else(|| CliError::Parse {
        detail: "file history commands require the main daemon".into(),
    })?;
    let mut object_id = options.object_id;
    let mut version = options.version;
    let mut path = options.path;
    if object_id.is_none() || version.is_none() {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::dfs::DAEMON_DFS_BROWSER_STAT_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::dfs::DaemonDfsBrowserStatRequest {
                path: path.clone(),
                max_staleness_millis: 0,
            },
        )?;
        let response = daemon_send_daemon_rpc(&status.address, &request, Duration::from_secs(30))?;
        let response: sinomo_runtime::dfs::DaemonDfsBrowserStatOutcomeResponse =
            response.decode_body()?;
        let details = response.into_result().map_err(|error| CliError::Parse {
            detail: error.to_string(),
        })?;
        let actual_object = sinomo_runtime::dfs::cli::parse_dfs_object_id(&details.object_id)
            .map_err(|error| CliError::Parse {
                detail: error.to_string(),
            })?;
        if details.file_system_id != options.file_system_id.as_str()
            || object_id.as_ref().is_some_and(|id| id != &actual_object)
        {
            return Err(CliError::Parse {
                detail: "path identifies a different filesystem or object".into(),
            });
        }
        object_id = Some(actual_object);
        if version.is_none() {
            version = Some(
                sinomo_runtime::dfs::cli::parse_dfs_version(&details.version).map_err(|error| {
                    CliError::Parse {
                        detail: error.to_string(),
                    }
                })?,
            );
        }
        path = details.path;
    }
    let operation_id = options
        .operation_id
        .unwrap_or_else(|| sinomo_runtime::core::Id::generate().to_string());
    let request = DfsFileHistoryPinRequest {
        path,
        identity: DfsFileVersionIdentity {
            file_system_id: options.file_system_id,
            object_id: object_id.expect("resolved object"),
            version: version.expect("resolved version"),
        },
        operation: options.operation,
        operation_id: operation_id.clone(),
    };
    let rpc = sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sinomo_runtime::dfs::DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
        sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
        &request,
    )?;
    let response = daemon_send_daemon_rpc(&status.address, &rpc, Duration::from_secs(60)).map_err(|error| CliError::Parse {
        detail: format!("file history operation {operation_id}: {error}; reconcile or retry with the same --operation-id"),
    })?;
    let response: DfsFileHistoryPinResponse = response.decode_body()?;
    if options.json {
        print_json("<dfs-file-history-pin>", &response)?;
    } else {
        println!(
            "File history updated: object {} version {} (operation {}).",
            response.identity.object_id, response.identity.version, operation_id
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
            sinomo_runtime::dfs::DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::dfs::DaemonDfsBrowserReadObjectVersionRequest {
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
        let response: sinomo_runtime::dfs::DaemonDfsBrowserReadObjectVersionResponse =
            response.decode_body()?;
        return Ok(response.bytes);
    }

    let mut opened = sinomo_runtime::dfs::open_mounted_sifs_service(None)?;
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
            sinomo_runtime::dfs::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::dfs::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        let response = daemon_send_daemon_rpc(
            &status.address,
            &request,
            std::time::Duration::from_secs(30),
        )?;
        let _: sinomo_runtime::dfs::DaemonDfsBrowserWriteFileResponse = response.decode_body()?;
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
            sinomo_runtime::dfs::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            sinomo_runtime::daemon::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::dfs::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory: false,
            },
        )?;
        let response = daemon_send_daemon_rpc(
            &status.address,
            &request,
            std::time::Duration::from_secs(10),
        )?;
        let _: sinomo_runtime::dfs::DaemonDfsBrowserDeleteResponse = response.decode_body()?;
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
    if let Some(handoff) = &report.pending_handoff {
        println!(
            "DFS metadata shard primary transfer requested: {} shard={} {} -> {} epoch {} -> {} handoff={} (activation pending)",
            report.file_system_id,
            report.shard_id,
            report.previous_primary_node_id,
            report.new_primary_node_id,
            report.previous_epoch,
            report.new_epoch,
            handoff.handoff_id
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
    daemon_address: Option<&str>,
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
    let daemon_address = daemon_address.ok_or_else(|| CliError::Network {
        address: target_node_id.to_string(),
        detail: "DFS cluster object RPC requires the local daemon to be running".to_string(),
    })?;
    let client = DaemonApiClient::at_address(daemon_address);
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

const DFS_MOUNT_LOCAL_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
// Cursor recovery is supervisor-serialized and may need to replay durable
// authorization cuts. Keep the client budget above the supervisor's bounded
// control wait so an ordinary 5-second timeout cannot hide a successful Apply.
const DFS_MOUNT_CURSOR_RECOVERY_RPC_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(90);
const DFS_MOUNT_STATUS_RPC_CATEGORY: &str = "sinomo.dfs.mount.status";
const DFS_MOUNT_RECOVERY_RPC_CATEGORY: &str = "sinomo.dfs.mount.recovery";

trait DfsMountSupervisorControl {
    fn live_status(&self) -> Result<sinomo_runtime::dfs::SifsMountRuntimeStatus, CliError>;
    fn recover_cursor_only_state(
        &self,
        request: &sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryRequest,
    ) -> Result<sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryResponse, CliError>;
    fn mount_config(&self) -> Result<sinomo_runtime::dfs::SifsMountConfig, CliError>;
    fn write_mount_config(
        &self,
        config: &sinomo_runtime::dfs::SifsMountConfig,
    ) -> Result<(), CliError>;
}

struct LocalDfsMountSupervisorControl {
    daemon: DaemonApiClient,
}

impl LocalDfsMountSupervisorControl {
    fn connect() -> Result<Self, CliError> {
        Ok(Self {
            daemon: DaemonApiClient::connect()?.with_timeout(DFS_MOUNT_LOCAL_RPC_TIMEOUT),
        })
    }

    fn config_client(&self) -> sinomo_runtime::api::RemoteConfigClient {
        sinomo_runtime::api::RemoteConfigClient::new(
            self.daemon.address().to_string(),
            DFS_MOUNT_LOCAL_RPC_TIMEOUT,
        )
    }
}

impl DfsMountSupervisorControl for LocalDfsMountSupervisorControl {
    fn live_status(&self) -> Result<sinomo_runtime::dfs::SifsMountRuntimeStatus, CliError> {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::dfs::SIFS_MOUNT_STATUS_ROUTE,
            DFS_MOUNT_STATUS_RPC_CATEGORY,
            &sinomo_runtime::dfs::SifsMountStatusRequest,
        )?
        .with_trace_context_if_missing();
        let response = self.daemon.call_request(&request)?;
        Ok(response
            .decode_body::<sinomo_runtime::dfs::SifsMountStatusResponse>()?
            .status)
    }

    fn recover_cursor_only_state(
        &self,
        request: &sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryRequest,
    ) -> Result<sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryResponse, CliError> {
        let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
            sinomo_runtime::dfs::SIFS_MOUNT_CURSOR_ONLY_RECOVERY_ROUTE,
            DFS_MOUNT_RECOVERY_RPC_CATEGORY,
            request,
        )?
        .with_trace_context_if_missing();
        self.daemon
            .clone()
            .with_timeout(DFS_MOUNT_CURSOR_RECOVERY_RPC_TIMEOUT)
            .call_request(&request)?
            .decode_body::<sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryResponse>()
            .map_err(Into::into)
    }

    fn mount_config(&self) -> Result<sinomo_runtime::dfs::SifsMountConfig, CliError> {
        use sinomo_runtime::config::ConfigClientExt as _;

        self.config_client()
            .get_typed(sinomo_runtime::dfs::SIFS_MOUNT_CONFIG_KEY)
            .map_err(Into::into)
    }

    fn write_mount_config(
        &self,
        config: &sinomo_runtime::dfs::SifsMountConfig,
    ) -> Result<(), CliError> {
        use sinomo_runtime::config::ConfigClientExt as _;

        self.config_client()
            .set_typed(
                sinomo_runtime::dfs::SIFS_MOUNT_CONFIG_KEY,
                config,
                sinomo_runtime::config::ConfigWriteTarget::Local,
            )
            .map(drop)
            .map_err(Into::into)
    }
}

fn query_live_dfs_mount_status(
    control: &dyn DfsMountSupervisorControl,
) -> Result<sinomo_runtime::dfs::SifsMountRuntimeStatus, CliError> {
    control.live_status()
}

fn request_cursor_only_recovery(
    control: &dyn DfsMountSupervisorControl,
    request: &sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryRequest,
) -> Result<sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryResponse, CliError> {
    control.recover_cursor_only_state(request)
}

fn disable_managed_dfs_mount(
    control: &dyn DfsMountSupervisorControl,
) -> Result<sinomo_runtime::dfs::SifsMountConfig, CliError> {
    let mut config = control.mount_config()?;
    config.enabled = false;
    control.write_mount_config(&config)?;
    Ok(config)
}

fn print_live_dfs_mount_status(
    status: &sinomo_runtime::dfs::SifsMountRuntimeStatus,
    json: bool,
) -> Result<(), CliError> {
    if json {
        return print_json("<dfs-mount-status>", status);
    }
    println!(
        "SIFS mount {:?}: {} -> {} ({:?})",
        status.phase, status.configured_mount_point, status.root_path, status.platform
    );
    if let Some(resolved_mount_point) = &status.resolved_mount_point {
        println!("  resolved mount point: {resolved_mount_point}");
    }
    if status.platform == sinomo_runtime::dfs::SifsMountPlatform::ExternalProvider {
        println!(
            "  cloud provider: registered={} connected={} worker={} ready={} writeback={:?}",
            status.provider_registered,
            status.provider_connected,
            status.worker_alive,
            status.worker_ready,
            status.writeback_mode
        );
        println!(
            "  writeback: {} pending, {} retrying, {} failed, {} conflicts",
            status.pending_operations,
            status.retrying_operations,
            status.failed_operations,
            status.conflict_count
        );
        for attention in &status.attention {
            println!(
                "  attention: {:?} at {} [{}]",
                attention.kind, attention.path, attention.reason_code
            );
            println!("    reason: {}", attention.reason);
            println!(
                "    safe next action: {}",
                sifs_mount_attention_next_action(attention.safe_next_action)
            );
        }
        if status.attention_overflow > 0 {
            println!(
                "  attention: {} additional issues omitted from this bounded status",
                status.attention_overflow
            );
        }
    }
    if let Some(last_error) = &status.last_error {
        println!("  last error: {last_error}");
    }
    println!("  detail: {}", status.detail);
    println!("  updated: {}", status.updated_at);
    Ok(())
}

fn sifs_mount_attention_next_action(
    action: sinomo_runtime::dfs::SifsMountAttentionNextAction,
) -> &'static str {
    use sinomo_runtime::dfs::SifsMountAttentionNextAction;

    match action {
        SifsMountAttentionNextAction::ReviewAndResolve => {
            "preserve both local and authoritative evidence, then resolve explicitly"
        }
        SifsMountAttentionNextAction::RepairCauseAndRetry => {
            "repair the reported cause and retry without discarding retained evidence"
        }
        SifsMountAttentionNextAction::PreserveLocalCopyAndRescan => {
            "preserve or move the local copy outside the mount, then retry reconciliation"
        }
        SifsMountAttentionNextAction::ReviewUnsupportedEntry => {
            "review and preserve the unsupported entry outside the sync workflow"
        }
        SifsMountAttentionNextAction::WaitForAuthoritativeReconciliation => {
            "wait for authoritative reconciliation, then refresh status"
        }
    }
}

fn run_dfs_mount_status_command(json: bool) -> Result<(), CliError> {
    let control = LocalDfsMountSupervisorControl::connect()?;
    let status = query_live_dfs_mount_status(&control)?;
    print_live_dfs_mount_status(&status, json)
}

fn run_dfs_mount_disable_command(json: bool) -> Result<(), CliError> {
    let control = LocalDfsMountSupervisorControl::connect()?;
    let config = disable_managed_dfs_mount(&control)?;
    if json {
        print_json("<dfs-mount-config>", &config)
    } else {
        println!(
            "disabled supervised SIFS mount {} -> {}; mount identity was preserved",
            config.mount_point, config.root_path
        );
        Ok(())
    }
}

fn print_cursor_only_recovery_response(
    response: &sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryResponse,
    json: bool,
) -> Result<(), CliError> {
    use sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryResponse;

    if json {
        return print_json("<dfs-mount-cursor-only-recovery>", response);
    }
    match response {
        SifsMountCursorOnlyRecoveryResponse::Inspected {
            recovery_token,
            raw_state_fingerprint,
            sync_root_file_id,
            cursor_count,
            provider_content_hash,
            provider_interface_version,
            detail,
        } => {
            println!("External provider cursor-only recovery inspection:");
            println!("  recovery token: {recovery_token}");
            println!("  raw state fingerprint: {raw_state_fingerprint}");
            println!("  sync-root file ID: {sync_root_file_id}");
            println!("  retained cursors: {cursor_count}");
            println!("  provider content hash: {provider_content_hash}");
            println!("  provider interface: {provider_interface_version}");
            println!("  detail: {detail}");
        }
        SifsMountCursorOnlyRecoveryResponse::Applied {
            recovery_id,
            recovery_token,
            sync_root_file_id,
            cursor_count_cleared,
            detail,
        } => {
            println!("External provider cursor-only recovery was authorized:");
            println!("  recovery ID: {recovery_id}");
            println!("  recovery token: {recovery_token}");
            println!("  sync-root file ID: {sync_root_file_id}");
            println!("  cursors cleared under durable ownership: {cursor_count_cleared}");
            println!("  detail: {detail}");
        }
    }
    Ok(())
}

fn run_dfs_mount_cursor_only_recovery_command(
    request: sinomo_runtime::dfs::SifsMountCursorOnlyRecoveryRequest,
    json: bool,
) -> Result<(), CliError> {
    let control = LocalDfsMountSupervisorControl::connect()?;
    let response = request_cursor_only_recovery(&control, &request)?;
    print_cursor_only_recovery_response(&response, json)
}

pub(crate) fn run_dfs_mount_command(options: DfsMountOptions) -> Result<(), CliError> {
    validate_dfs_mount_options_for_platform(&options)?;
    match options {
        DfsMountOptions::Status { json } => run_dfs_mount_status_command(json),
        DfsMountOptions::Disable { json } => run_dfs_mount_disable_command(json),
        DfsMountOptions::RecoverCursorOnlyState { request, json } => {
            run_dfs_mount_cursor_only_recovery_command(request, json)
        }
        DfsMountOptions::Mount(options) => run_dfs_host_mount_command(options),
    }
}

fn run_dfs_nfs_command(command: DfsNfsCliCommand) -> Result<(), CliError> {
    match command {
        DfsNfsCliCommand::Serve(options) => run_dfs_nfs_server(options, None),
        DfsNfsCliCommand::Mount(options) => run_dfs_nfs_mount(options),
        DfsNfsCliCommand::Unmount { target } => run_dfs_nfs_unmount(&target),
    }
}

fn run_dfs_nfs_server(
    options: DfsNfsServerCliOptions,
    native_mount_target: Option<String>,
) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("sinomo-dfs-nfs")
        .build()
        .map_err(|source| CliError::Parse {
            detail: format!("create NFS runtime: {source}"),
        })?;
    runtime.block_on(async move {
        let server_options = sinomo_runtime::dfs_nfs::DfsNfsServerOptions {
            data_dir: options.data_dir,
            bind: options.bind,
            export_name: options.export_name,
            root_path: options.root_path,
            read_only: options.read_only,
        };
        let read_only = server_options.read_only;
        let server = sinomo_runtime::dfs_nfs::DfsNfsServer::bind(server_options)
            .await
            .map_err(|source| CliError::Parse {
                detail: source.to_string(),
            })?;
        println!(
            "serving SIFS through NFSv3 at {} export /{}{}",
            server.local_addr(),
            server.export_name(),
            if read_only { " (read-only)" } else { "" }
        );
        if let Some(target) = native_mount_target {
            let address = server.local_addr();
            let export_name = server.export_name().to_string();
            let serving = tokio::spawn(server.serve_until_unmounted());
            if let Err(error) =
                mount_native_nfs_target(&target, address, &export_name, read_only).await
            {
                serving.abort();
                let _ = serving.await;
                return Err(error);
            }
            println!(
                "mounted SIFS NFS export at {target}; keep this process running or use `sinomo dfs nfs unmount {target}` to stop it cleanly"
            );
            return serving
                .await
                .map_err(|source| CliError::Parse {
                    detail: format!("NFS server task failed: {source}"),
                })?
                .map_err(|source| CliError::Parse {
                    detail: source.to_string(),
                });
        }
        server.serve().await.map_err(|source| CliError::Parse {
            detail: source.to_string(),
        })
    })
}

fn run_dfs_nfs_mount(options: DfsNfsMountCliOptions) -> Result<(), CliError> {
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = options;
        Err(CliError::Parse {
            detail: "`sinomo dfs nfs mount` is currently supported on Linux and Windows"
                .to_string(),
        })
    }
    #[cfg(windows)]
    {
        let drive = normalize_windows_drive(&options.target)?;
        let address = options
            .server
            .bind
            .parse::<std::net::SocketAddr>()
            .map_err(|source| CliError::Parse {
                detail: format!("invalid NFS bind address: {source}"),
            })?;
        if address.ip() != std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            || address.port() != 111
        {
            return Err(CliError::Parse {
                detail: "Windows Client for NFS discovers the local service through 127.0.0.1:111; `dfs nfs mount` requires --bind 127.0.0.1:111"
                    .to_string(),
            });
        }
        run_dfs_nfs_server(options.server, Some(drive))
    }
    #[cfg(target_os = "linux")]
    {
        let mount_point = validate_linux_nfs_mount_point(&options.target, true)?;
        run_dfs_nfs_server(options.server, Some(mount_point.display().to_string()))
    }
}

#[cfg(windows)]
async fn mount_native_nfs_target(
    drive: &str,
    address: std::net::SocketAddr,
    export_name: &str,
    _read_only: bool,
) -> Result<(), CliError> {
    let drive = drive.to_string();
    let export = format!(r"\\{}\{}", address.ip(), export_name);
    tokio::task::spawn_blocking(move || {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let output = std::process::Command::new("mount.exe")
            .creation_flags(CREATE_NO_WINDOW)
            .args([
                "-o",
                "anon,nolock,mtype=soft,fileaccess=764,casesensitive,rsize=32,wsize=32,timeout=1,retry=2",
                &export,
                &drive,
            ])
            .output()
            .map_err(|source| CliError::Parse {
                detail: format!(
                    "start Windows Client for NFS mount utility (is Client for NFS installed?): {source}"
                ),
            })?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Err(CliError::Parse {
            detail: format!(
                "Windows Client for NFS could not map {export} to {drive}: {}",
                if stderr.is_empty() { stdout } else { stderr }
            ),
        })
    })
    .await
    .map_err(|source| CliError::Parse {
        detail: format!("Windows NFS mount worker failed: {source}"),
    })?
}

#[cfg(target_os = "linux")]
async fn mount_native_nfs_target(
    target: &str,
    address: std::net::SocketAddr,
    export_name: &str,
    read_only: bool,
) -> Result<(), CliError> {
    let target = target.to_string();
    let export_name = export_name.to_string();
    tokio::task::spawn_blocking(move || {
        let mount_point = validate_linux_nfs_mount_point(&target, true)?;
        let source = linux_nfs_source(address.ip(), &export_name);
        let options = linux_nfs_mount_options(address.port(), read_only);
        let direct = std::process::Command::new("mount.nfs")
            .args(["-o", &options, &source])
            .arg(&mount_point)
            .output();
        let output = match direct {
            Ok(output) => output,
            Err(direct_error) if direct_error.kind() == std::io::ErrorKind::NotFound => {
                std::process::Command::new("mount")
                    .args(["-t", "nfs", "-o", &options, &source])
                    .arg(&mount_point)
                    .output()
                    .map_err(|fallback| CliError::Parse {
                        detail: format!(
                            "start the Linux NFS mount helper (`mount.nfs` was not found and `mount -t nfs` also failed): {fallback}"
                        ),
                    })?
            }
            Err(direct_error) => {
                return Err(CliError::Parse {
                    detail: format!("start Linux NFS mount helper: {direct_error}"),
                });
            }
        };
        if output.status.success() {
            return Ok(());
        }
        Err(CliError::Parse {
            detail: format!(
                "Linux NFS client could not mount {source} at {}: {}",
                mount_point.display(),
                command_output_detail(&output)
            ),
        })
    })
    .await
    .map_err(|source| CliError::Parse {
        detail: format!("Linux NFS mount worker failed: {source}"),
    })?
}

#[cfg(target_os = "linux")]
fn linux_nfs_source(address: std::net::IpAddr, export_name: &str) -> String {
    match address {
        std::net::IpAddr::V4(address) => format!("{address}:/{export_name}"),
        std::net::IpAddr::V6(address) => format!("[{address}]:/{export_name}"),
    }
}

#[cfg(target_os = "linux")]
fn linux_nfs_mount_options(port: u16, read_only: bool) -> String {
    format!(
        "{},vers=3,proto=tcp,mountproto=tcp,nolock,soft,timeo=10,retrans=2,port={port},mountport={port},rsize=1048576,wsize=1048576,actimeo=1",
        if read_only { "ro" } else { "rw" }
    )
}

#[cfg(target_os = "linux")]
fn validate_linux_nfs_mount_point(value: &str, require_empty: bool) -> Result<PathBuf, CliError> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        return Err(CliError::Parse {
            detail: "Linux NFS mount point must be a non-empty path without surrounding whitespace or NUL bytes"
                .to_string(),
        });
    }
    let requested = PathBuf::from(value);
    let requested = if requested.is_absolute() {
        requested
    } else {
        std::env::current_dir()
            .map_err(|source| CliError::Io {
                path: ".".to_string(),
                detail: source.to_string(),
            })?
            .join(requested)
    };
    let metadata = std::fs::symlink_metadata(&requested).map_err(|source| CliError::Io {
        path: requested.display().to_string(),
        detail: format!("inspect Linux NFS mount point: {source}"),
    })?;
    if metadata.file_type().is_symlink() {
        return Err(CliError::Parse {
            detail: format!(
                "refusing symlink as Linux NFS mount point: {}",
                requested.display()
            ),
        });
    }
    if !metadata.is_dir() {
        return Err(CliError::Parse {
            detail: format!(
                "Linux NFS mount point is not a directory: {}",
                requested.display()
            ),
        });
    }
    let canonical = std::fs::canonicalize(&requested).map_err(|source| CliError::Io {
        path: requested.display().to_string(),
        detail: format!("resolve Linux NFS mount point: {source}"),
    })?;
    if canonical.parent().is_none() {
        return Err(CliError::Parse {
            detail: "refusing to use the filesystem root as an NFS mount point".to_string(),
        });
    }
    let mounted = linux_path_is_mount_point(&canonical)?;
    if require_empty {
        if mounted {
            return Err(CliError::Parse {
                detail: format!(
                    "Linux NFS mount point is already mounted: {}",
                    canonical.display()
                ),
            });
        }
        let mut entries = std::fs::read_dir(&canonical).map_err(|source| CliError::Io {
            path: canonical.display().to_string(),
            detail: format!("inspect Linux NFS mount-point contents: {source}"),
        })?;
        if entries
            .next()
            .transpose()
            .map_err(|source| CliError::Io {
                path: canonical.display().to_string(),
                detail: format!("inspect Linux NFS mount-point entry: {source}"),
            })?
            .is_some()
        {
            return Err(CliError::Parse {
                detail: format!(
                    "refusing to hide existing files beneath non-empty NFS mount point {}",
                    canonical.display()
                ),
            });
        }
    } else if !mounted {
        return Err(CliError::Parse {
            detail: format!(
                "refusing to unmount a path that is not a mount point: {}",
                canonical.display()
            ),
        });
    }
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn linux_path_is_mount_point(path: &Path) -> Result<bool, CliError> {
    let mountinfo =
        std::fs::read_to_string("/proc/self/mountinfo").map_err(|source| CliError::Io {
            path: "/proc/self/mountinfo".to_string(),
            detail: format!("inspect Linux mount topology: {source}"),
        })?;
    Ok(parse_linux_mountinfo_for_path(&mountinfo, path).is_some())
}

#[cfg(target_os = "linux")]
fn decode_linux_mountinfo_path(value: &str) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            let escape = bytes.get(index + 1..index + 4)?;
            let value = match escape {
                b"040" => b' ',
                b"011" => b'\t',
                b"012" => b'\n',
                b"134" => b'\\',
                _ => return None,
            };
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(decoded)))
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct LinuxMountInfo {
    file_system_type: String,
    source: String,
}

#[cfg(target_os = "linux")]
fn parse_linux_mountinfo_for_path(contents: &str, path: &Path) -> Option<LinuxMountInfo> {
    contents.lines().find_map(|line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let separator = fields.iter().position(|field| *field == "-")?;
        if separator + 2 >= fields.len()
            || decode_linux_mountinfo_path(fields.get(4)?)?.as_path() != path
        {
            return None;
        }
        Some(LinuxMountInfo {
            file_system_type: fields[separator + 1].to_string(),
            source: fields[separator + 2].to_string(),
        })
    })
}

#[cfg(target_os = "linux")]
fn command_output_detail(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("process exited with {}", output.status)
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
async fn mount_native_nfs_target(
    _target: &str,
    _address: std::net::SocketAddr,
    _export_name: &str,
    _read_only: bool,
) -> Result<(), CliError> {
    Err(CliError::Parse {
        detail: "native NFS mounting is unavailable on this platform".to_string(),
    })
}

fn run_dfs_nfs_unmount(target: &str) -> Result<(), CliError> {
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = target;
        Err(CliError::Parse {
            detail: "`sinomo dfs nfs unmount` is currently supported on Linux and Windows"
                .to_string(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;

        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let drive = normalize_windows_drive(target)?;
        let output = std::process::Command::new("umount.exe")
            .creation_flags(CREATE_NO_WINDOW)
            .arg(&drive)
            .output()
            .map_err(|source| CliError::Parse {
                detail: format!("start Windows Client for NFS unmount utility: {source}"),
            })?;
        if output.status.success() {
            println!("unmapped SIFS NFS drive {drive}");
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Err(CliError::Parse {
            detail: format!(
                "Windows Client for NFS could not unmap {drive}: {}",
                if stderr.is_empty() { stdout } else { stderr }
            ),
        })
    }
    #[cfg(target_os = "linux")]
    {
        unmount_linux_nfs_target(target)
    }
}

#[cfg(target_os = "linux")]
fn unmount_linux_nfs_target(target: &str) -> Result<(), CliError> {
    let mount_point = validate_linux_nfs_mount_point(target, false)?;
    let mountinfo =
        std::fs::read_to_string("/proc/self/mountinfo").map_err(|source| CliError::Io {
            path: "/proc/self/mountinfo".to_string(),
            detail: format!("revalidate Linux mount topology before unmount: {source}"),
        })?;
    let mounted = parse_linux_mountinfo_for_path(&mountinfo, &mount_point).ok_or_else(|| {
        CliError::Parse {
            detail: format!(
                "Linux NFS mount disappeared before unmount: {}",
                mount_point.display()
            ),
        }
    })?;
    let loopback_source = mounted.source.starts_with("127.0.0.1:/")
        || mounted.source.starts_with("[::1]:/")
        || mounted.source.starts_with("::1:/");
    if mounted.file_system_type != "nfs" || !loopback_source {
        return Err(CliError::Parse {
            detail: format!(
                "refusing to unmount non-Sinomo candidate {} (type={}, source={})",
                mount_point.display(),
                mounted.file_system_type,
                mounted.source
            ),
        });
    }
    let output = std::process::Command::new("umount")
        .arg(&mount_point)
        .output()
        .map_err(|source| CliError::Parse {
            detail: format!("start Linux unmount utility: {source}"),
        })?;
    if output.status.success() {
        println!("unmounted SIFS NFS mount {}", mount_point.display());
        return Ok(());
    }
    Err(CliError::Parse {
        detail: format!(
            "Linux could not unmount {}: {}",
            mount_point.display(),
            command_output_detail(&output)
        ),
    })
}

#[cfg(windows)]
fn normalize_windows_drive(value: &str) -> Result<String, CliError> {
    let bytes = value.as_bytes();
    if bytes.len() != 2 || !bytes[0].is_ascii_alphabetic() || bytes[1] != b':' {
        return Err(CliError::Parse {
            detail: format!("invalid Windows drive `{value}`; expected a drive letter such as S:"),
        });
    }
    Ok(format!("{}:", (bytes[0] as char).to_ascii_uppercase()))
}

fn run_dfs_host_mount_command(options: DfsHostMountOptions) -> Result<(), CliError> {
    #[cfg(windows)]
    if options.foreground {
        return Err(CliError::Parse {
            detail: "external Windows provider foreground mounts are unsupported because only the managed supervisor durably pins ownership and state-preserving cleanup; use `sinomo dfs mount --background ...` with the main daemon"
                .to_string(),
        });
    }
    let data_dir = match options.data_dir.clone() {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home()?,
    };
    if !options.foreground {
        let config = sinomo_runtime::dfs::SifsMountConfig {
            enabled: true,
            mount_point: options.mount_point.display().to_string(),
            root_path: options.root_path,
            read_only: options.read_only,
            allow_other: options.allow_other,
            sync_local_changes: options.sync_local_changes,
        };
        let resolved =
            crate::dfs_mount_supervisor::configure_managed_sifs_mount(&data_dir, &config)
                .map_err(|detail| CliError::Parse { detail })?;
        println!(
            "configured supervised SIFS mount {} -> {}; the main daemon will keep it connected",
            resolved.display(),
            config.root_path
        );
        return Ok(());
    }
    if crate::dfs_mount_supervisor::managed_sifs_mount_config(&data_dir)
        .map_err(|detail| CliError::Parse { detail })?
        .enabled
    {
        return Err(CliError::Parse {
            detail: "the supervised SIFS mount is enabled; disable `storage.sifs-mount` before starting a second foreground mount".to_string(),
        });
    }
    let lease_owner = format!("foreground CLI mount {}", options.mount_point.display());
    let _mount_lease =
        crate::dfs_mount_supervisor::SifsMountLease::try_acquire(&data_dir, &lease_owner)
            .map_err(|detail| CliError::Parse { detail })?;
    #[cfg(windows)]
    {
        unreachable!("Windows foreground mounts are rejected before data-dir or lease effects")
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
    if matches!(options, DfsMountOptions::RecoverCursorOnlyState { .. }) {
        #[cfg(not(windows))]
        {
            return Err(CliError::Parse {
                detail: "`--recover-cursor-only-state` is only supported by an external Windows provider"
                    .to_string(),
            });
        }
        #[cfg(windows)]
        {
            return Ok(());
        }
    }
    let DfsMountOptions::Mount(options) = options else {
        return Ok(());
    };
    #[cfg(not(windows))]
    {
        if options.sync_local_changes {
            return Err(CliError::Parse {
                detail: "`--sync-local-changes` is only supported by an external Windows provider; Unix FUSE writes are already sent directly to SIFS".to_string(),
            });
        }
    }
    #[cfg(windows)]
    {
        if options.allow_other {
            return Err(CliError::Parse {
                detail: "`--allow-other` is only supported by Unix FUSE".to_string(),
            });
        }
        if options.read_only || !options.sync_local_changes {
            return Err(CliError::Parse {
                detail: "the external Windows provider requires durable writable synchronization; pass `--sync-local-changes` without `--read-only`"
                    .to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod dfs_mount_tests {
    use std::cell::{Cell, RefCell};
    #[cfg(target_os = "linux")]
    use std::path::{Path, PathBuf};

    use super::{
        DfsCliCommand, DfsHostMountOptions, DfsMountOptions, DfsMountSupervisorControl,
        DfsNfsCliCommand, DfsNfsMountCliOptions, DfsNfsServerCliOptions, disable_managed_dfs_mount,
        parse_dfs_schema_for_test, query_live_dfs_mount_status, request_cursor_only_recovery,
        run_dfs_mount_command,
    };
    use sinomo_runtime::dfs::{
        SifsMountConfig, SifsMountCursorOnlyRecoveryRequest, SifsMountCursorOnlyRecoveryResponse,
        SifsMountRuntimePhase, SifsMountRuntimeStatus,
    };
    #[cfg(not(windows))]
    use sinomo_runtime::local::SinomoDataDir;

    struct FakeMountSupervisorControl {
        live_status: SifsMountRuntimeStatus,
        live_error: Option<&'static str>,
        config: SifsMountConfig,
        live_calls: Cell<usize>,
        cursor_recovery_requests: RefCell<Vec<SifsMountCursorOnlyRecoveryRequest>>,
        cursor_recovery_response: SifsMountCursorOnlyRecoveryResponse,
        config_reads: Cell<usize>,
        config_writes: RefCell<Vec<SifsMountConfig>>,
    }

    impl FakeMountSupervisorControl {
        fn new(live_status: SifsMountRuntimeStatus, config: SifsMountConfig) -> Self {
            Self {
                live_status,
                live_error: None,
                config,
                live_calls: Cell::new(0),
                cursor_recovery_requests: RefCell::new(Vec::new()),
                cursor_recovery_response: SifsMountCursorOnlyRecoveryResponse::Inspected {
                    recovery_token: "cursor-recovery-token".to_string(),
                    raw_state_fingerprint: "blake3:cursor-state".to_string(),
                    sync_root_file_id: 42,
                    cursor_count: 2,
                    provider_content_hash: "blake3:provider".to_string(),
                    provider_interface_version:
                        sinomo_runtime::dfs::HOST_FILESYSTEM_PROVIDER_INTERFACE_VERSION.to_string(),
                    detail: "eligible cursor-only state".to_string(),
                },
                config_reads: Cell::new(0),
                config_writes: RefCell::new(Vec::new()),
            }
        }
    }

    #[test]
    fn nfs_schema_parses_server_and_native_mount_commands() {
        assert_eq!(
            parse_dfs_schema_for_test(&[
                "nfs".to_string(),
                "serve".to_string(),
                "--bind".to_string(),
                "127.0.0.1:2049".to_string(),
                "--export".to_string(),
                "agents".to_string(),
                "--root".to_string(),
                "/agents".to_string(),
                "--read-only".to_string(),
            ])
            .expect("parse NFS server"),
            DfsCliCommand::Nfs(DfsNfsCliCommand::Serve(DfsNfsServerCliOptions {
                bind: "127.0.0.1:2049".to_string(),
                export_name: "agents".to_string(),
                root_path: "/agents".to_string(),
                data_dir: None,
                read_only: true,
            }))
        );
        assert_eq!(
            parse_dfs_schema_for_test(&["nfs".to_string(), "mount".to_string(), "S:".to_string(),])
                .expect("parse native NFS mount"),
            DfsCliCommand::Nfs(DfsNfsCliCommand::Mount(DfsNfsMountCliOptions {
                target: "S:".to_string(),
                server: DfsNfsServerCliOptions {
                    bind: super::default_nfs_mount_bind().to_string(),
                    export_name: "sifs".to_string(),
                    root_path: "/".to_string(),
                    data_dir: None,
                    read_only: false,
                },
            }))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_nfs_mount_plan_uses_explicit_ephemeral_server_port_and_safe_target() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-linux-nfs-mount-plan");
        let mount_point = root.path().join("mount");
        std::fs::create_dir(&mount_point).expect("create empty mount point");
        assert_eq!(
            super::validate_linux_nfs_mount_point(
                mount_point.to_str().expect("UTF-8 test path"),
                true,
            )
            .expect("validate empty mount point"),
            std::fs::canonicalize(&mount_point).expect("canonical mount point")
        );
        assert_eq!(
            super::linux_nfs_source("127.0.0.1".parse().unwrap(), "sifs"),
            "127.0.0.1:/sifs"
        );
        assert_eq!(
            super::linux_nfs_source("::1".parse().unwrap(), "agents"),
            "[::1]:/agents"
        );
        assert_eq!(
            super::decode_linux_mountinfo_path(r"/tmp/with\040space\134name"),
            Some(PathBuf::from("/tmp/with space\\name"))
        );
        assert_eq!(
            super::parse_linux_mountinfo_for_path(
                "41 30 0:52 / /tmp/sinomo\\040mount rw - nfs 127.0.0.1:/sifs rw,vers=3\n",
                Path::new("/tmp/sinomo mount"),
            ),
            Some(super::LinuxMountInfo {
                file_system_type: "nfs".to_string(),
                source: "127.0.0.1:/sifs".to_string(),
            })
        );
        assert!(super::linux_path_is_mount_point(Path::new("/")).expect("inspect mountinfo"));
        let options = super::linux_nfs_mount_options(43123, false);
        assert!(options.starts_with("rw,"));
        assert!(options.contains("vers=3"));
        assert!(options.contains("port=43123"));
        assert!(options.contains("mountport=43123"));
        assert!(options.contains("rsize=1048576"));
        assert!(options.contains("wsize=1048576"));
        assert!(super::linux_nfs_mount_options(43123, true).starts_with("ro,"));

        let mount_link = root.path().join("mount-link");
        std::os::unix::fs::symlink(&mount_point, &mount_link).expect("create mount-point symlink");
        assert!(
            super::validate_linux_nfs_mount_point(
                mount_link.to_str().expect("UTF-8 test path"),
                true,
            )
            .expect_err("symlink mount point must be rejected")
            .to_string()
            .contains("symlink")
        );

        std::fs::write(mount_point.join("existing.txt"), b"do not hide")
            .expect("create existing file");
        assert!(
            super::validate_linux_nfs_mount_point(
                mount_point.to_str().expect("UTF-8 test path"),
                true,
            )
            .expect_err("non-empty mount point must be rejected")
            .to_string()
            .contains("non-empty")
        );
    }

    impl DfsMountSupervisorControl for FakeMountSupervisorControl {
        fn live_status(&self) -> Result<SifsMountRuntimeStatus, crate::CliError> {
            self.live_calls.set(self.live_calls.get() + 1);
            if let Some(detail) = self.live_error {
                return Err(crate::CliError::Parse {
                    detail: detail.to_string(),
                });
            }
            Ok(self.live_status.clone())
        }

        fn recover_cursor_only_state(
            &self,
            request: &SifsMountCursorOnlyRecoveryRequest,
        ) -> Result<SifsMountCursorOnlyRecoveryResponse, crate::CliError> {
            self.cursor_recovery_requests
                .borrow_mut()
                .push(request.clone());
            Ok(self.cursor_recovery_response.clone())
        }

        fn mount_config(&self) -> Result<SifsMountConfig, crate::CliError> {
            self.config_reads.set(self.config_reads.get() + 1);
            Ok(self.config.clone())
        }

        fn write_mount_config(&self, config: &SifsMountConfig) -> Result<(), crate::CliError> {
            self.config_writes.borrow_mut().push(config.clone());
            Ok(())
        }
    }

    #[test]
    fn status_uses_only_the_live_supervisor_and_never_falls_back_to_config() {
        let live_status = SifsMountRuntimeStatus {
            phase: SifsMountRuntimePhase::Mounted,
            detail: "live supervisor observation".to_string(),
            updated_at: "2026-08-10T00:00:00Z".to_string(),
            ..SifsMountRuntimeStatus::default()
        };
        let stale_config = SifsMountConfig {
            enabled: true,
            mount_point: "/stale/persisted/mount".to_string(),
            ..SifsMountConfig::default()
        };
        let control = FakeMountSupervisorControl::new(live_status.clone(), stale_config.clone());

        assert_eq!(
            query_live_dfs_mount_status(&control).expect("live status"),
            live_status
        );
        assert_eq!(control.live_calls.get(), 1);
        assert_eq!(control.config_reads.get(), 0);
        assert!(control.config_writes.borrow().is_empty());

        let mut unavailable =
            FakeMountSupervisorControl::new(SifsMountRuntimeStatus::default(), stale_config);
        unavailable.live_error = Some("live supervisor unavailable");
        assert!(
            query_live_dfs_mount_status(&unavailable)
                .expect_err("live failure must not fall back")
                .to_string()
                .contains("live supervisor unavailable")
        );
        assert_eq!(unavailable.live_calls.get(), 1);
        assert_eq!(unavailable.config_reads.get(), 0);
        assert!(unavailable.config_writes.borrow().is_empty());
    }

    #[test]
    fn disable_changes_only_enabled_and_preserves_mount_identity() {
        let original = SifsMountConfig {
            enabled: true,
            mount_point: "~/sinomo".to_string(),
            root_path: "/sinomo/agents/owner/home".to_string(),
            read_only: false,
            allow_other: true,
            sync_local_changes: true,
        };
        let control =
            FakeMountSupervisorControl::new(SifsMountRuntimeStatus::default(), original.clone());

        let disabled = disable_managed_dfs_mount(&control).expect("disable mount");
        let expected = SifsMountConfig {
            enabled: false,
            ..original
        };
        assert_eq!(disabled, expected);
        assert_eq!(control.config_reads.get(), 1);
        assert_eq!(control.live_calls.get(), 0);
        assert!(control.cursor_recovery_requests.borrow().is_empty());
        assert_eq!(control.config_writes.borrow().as_slice(), &[expected]);
    }

    #[test]
    fn cursor_only_recovery_parser_builds_only_inspect_or_fully_authorized_apply_requests() {
        assert_eq!(
            parse_dfs_schema_for_test(&[
                "mount".to_string(),
                "--recover-cursor-only-state".to_string(),
                "--json".to_string(),
            ])
            .expect("parse read-only cursor recovery inspection"),
            DfsCliCommand::Mount(DfsMountOptions::RecoverCursorOnlyState {
                request: SifsMountCursorOnlyRecoveryRequest::Inspect,
                json: true,
            })
        );
        assert_eq!(
            parse_dfs_schema_for_test(&[
                "mount".to_string(),
                "--recover-cursor-only-state".to_string(),
                "--expected-token".to_string(),
                "cursor-recovery-token".to_string(),
                "--acknowledge-authoritative-full-reconciliation".to_string(),
            ])
            .expect("parse explicitly authorized cursor recovery"),
            DfsCliCommand::Mount(DfsMountOptions::RecoverCursorOnlyState {
                request: SifsMountCursorOnlyRecoveryRequest::Apply {
                    expected_token: "cursor-recovery-token".to_string(),
                    acknowledge_authoritative_full_reconciliation: true,
                },
                json: false,
            })
        );

        for invalid in [
            vec![
                "mount",
                "--recover-cursor-only-state",
                "--expected-token",
                "cursor-recovery-token",
            ],
            vec![
                "mount",
                "--recover-cursor-only-state",
                "--acknowledge-authoritative-full-reconciliation",
            ],
            vec!["mount", "--expected-token", "cursor-recovery-token"],
            vec!["mount", "--acknowledge-authoritative-full-reconciliation"],
            vec!["mount", "--recover-cursor-only-state", "--status"],
            vec!["mount", "--recover-cursor-only-state", "--disable"],
            vec!["mount", "--recover-cursor-only-state", "C:\\SinomoSIFS"],
            vec![
                "mount",
                "--recover-cursor-only-state",
                "--root",
                "/services",
            ],
            vec!["mount", "--recover-cursor-only-state", "--background"],
            vec![
                "mount",
                "--recover-cursor-only-state",
                "--expected-token",
                " cursor-recovery-token",
                "--acknowledge-authoritative-full-reconciliation",
            ],
        ] {
            assert!(
                parse_dfs_schema_for_test(
                    &invalid.into_iter().map(str::to_string).collect::<Vec<_>>()
                )
                .is_err(),
                "partial or mixed cursor-only recovery unexpectedly parsed"
            );
        }
    }

    #[test]
    fn cursor_only_recovery_forwards_exact_request_without_mount_or_config_effects() {
        let control = FakeMountSupervisorControl::new(
            SifsMountRuntimeStatus::default(),
            SifsMountConfig::default(),
        );
        let inspect = SifsMountCursorOnlyRecoveryRequest::Inspect;
        let expected_response = control.cursor_recovery_response.clone();
        assert_eq!(
            request_cursor_only_recovery(&control, &inspect).expect("inspect recovery state"),
            expected_response
        );
        assert_eq!(
            control.cursor_recovery_requests.borrow().as_slice(),
            &[inspect]
        );
        assert_eq!(control.live_calls.get(), 0);
        assert_eq!(control.config_reads.get(), 0);
        assert!(control.config_writes.borrow().is_empty());

        let apply = SifsMountCursorOnlyRecoveryRequest::Apply {
            expected_token: "cursor-recovery-token".to_string(),
            acknowledge_authoritative_full_reconciliation: true,
        };
        request_cursor_only_recovery(&control, &apply).expect("apply recovery state");
        assert_eq!(
            control.cursor_recovery_requests.borrow().as_slice(),
            &[SifsMountCursorOnlyRecoveryRequest::Inspect, apply]
        );
        assert_eq!(control.live_calls.get(), 0);
        assert_eq!(control.config_reads.get(), 0);
        assert!(control.config_writes.borrow().is_empty());
    }

    #[cfg(not(windows))]
    #[test]
    fn cursor_only_recovery_is_rejected_off_windows_before_rpc_effects() {
        let options = DfsMountOptions::RecoverCursorOnlyState {
            request: SifsMountCursorOnlyRecoveryRequest::Inspect,
            json: false,
        };
        let error = super::validate_dfs_mount_options_for_platform(&options)
            .expect_err("cursor recovery must remain Windows-only");
        assert!(error.to_string().contains("external Windows provider"));
    }

    #[cfg(not(windows))]
    #[test]
    fn foreground_mount_is_rejected_before_platform_side_effect_when_lease_is_owned() {
        let temp = sinomo_testdir::TestDir::new("sinomo-cli-foreground-mount-lease");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let mount_point = temp.path().join("mount");
        let _lease = crate::dfs_mount_supervisor::SifsMountLease::try_acquire(
            &data_dir,
            "foreground collision test",
        )
        .expect("test lease");

        let error = run_dfs_mount_command(DfsMountOptions::Mount(DfsHostMountOptions {
            mount_point: mount_point.clone(),
            root_path: "/".to_string(),
            data_dir: Some(data_dir.root()),
            read_only: true,
            allow_other: false,
            foreground: true,
            sync_local_changes: false,
        }))
        .expect_err("a held lease must reject a foreground mount");

        assert!(
            error
                .to_string()
                .contains("another managed or foreground mount")
        );
        assert!(
            !mount_point.exists(),
            "lease rejection must happen before the platform mount creates its mount point"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_foreground_mount_is_rejected_before_data_or_mount_effects() {
        let temp = sinomo_testdir::TestDir::new("sinomo-cli-windows-foreground-rejected");
        let data_root = temp.path().join("missing-parent").join("data");
        let mount_point = temp.path().join("mount");

        let error = run_dfs_mount_command(DfsMountOptions::Mount(DfsHostMountOptions {
            mount_point: mount_point.clone(),
            root_path: "/".to_string(),
            data_dir: Some(data_root.clone()),
            read_only: false,
            allow_other: false,
            foreground: true,
            sync_local_changes: true,
        }))
        .expect_err("Windows foreground mount must delegate to the managed supervisor");

        assert!(error.to_string().contains("managed supervisor"));
        assert!(
            !data_root.exists(),
            "rejection must precede data-dir creation"
        );
        assert!(
            !mount_point.exists(),
            "rejection must precede root creation"
        );
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
    let daemon_status = main_daemon_status_for_data_dir(&data_dir)?;
    let mut provider_node_ids =
        sinomo_runtime::dfs::discovered_cluster_dfs_provider_node_ids(&cluster_paths, &state)?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
    let mut nodes = Vec::new();
    if provider_node_ids.remove(&state.local_node_id) {
        let local_status_started_at = std::time::Instant::now();
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
        nodes.push(DfsClusterNodeStatus {
            node_id: state.local_node_id.clone(),
            local: true,
            endpoint: Some("local-daemon".to_string()),
            status: Some(local_status),
            error: None,
        });
    } else {
        trace_dfs_cluster_status(trace, started_at, "local node is not a DFS provider");
    }
    let remote_node_ids = provider_node_ids;
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
    let probes = spawn_remote_dfs_status_probes_with(
        remote_node_ids,
        remote_timeout,
        |node_id, timeout| {
            spawn_dfs_status_probe(
                daemon_status.as_ref().map(|status| status.address.as_str()),
                node_id,
                timeout,
                trace,
            )
        },
    )?;
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
    use super::*;
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

    #[test]
    fn unreachable_builtin_lan_projection_still_spawns_bounded_routed_probe() {
        crate::ensure_cluster_directory_installed();
        let root = sinomo_testdir::TestDir::new("sinomo-cli-dfs-status-routed-recovery");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        data_dir.ensure_root_layout().expect("data dir");

        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_status_routed_recovery").expect("cluster");
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_status_routed_recovery_local").expect("local");
        let remote_node_id =
            NodeId::from_legacy_named("node_dfs_status_routed_recovery_remote").expect("remote");
        let second_remote_node_id =
            NodeId::from_legacy_named("node_dfs_status_routed_recovery_remote_second")
                .expect("second remote");
        let now = OffsetDateTime::now_utc();
        let mut state = sinomo_runtime::core::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs status routed recovery",
            local_node_id.clone(),
            "local",
            "local-public-key",
            now,
        );
        let remote_endpoint =
            network::daemon_lan_endpoint(&remote_node_id, "198.51.100.42:21353".to_string(), 0);
        let mut remote = sinomo_runtime::core::ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node_id.clone(),
            "remote",
            "remote-public-key",
            now,
        );
        remote.capabilities =
            sinomo_runtime::node::main_daemon_capabilities(&ClusterMemberRole::Voter);
        remote.allowed_capabilities = remote.capabilities.clone();
        remote.endpoints = vec![remote_endpoint.clone()];
        state.members.push(remote);
        sinomo_runtime::cluster::write_cluster_state(&paths, &state).expect("write state");
        sinomo_runtime::cluster::sync_cluster_state_to_local_kv_unchecked(&paths, &state)
            .expect("sync state");
        let direct_unreachable = network::cluster_connectivity_record(
            &state,
            &remote_endpoint,
            network::ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(now.to_string()),
            now,
            120,
        );
        sinomo_runtime::cluster::sync_cluster_connectivity_records_to_kv(
            &paths,
            &local_node_id,
            vec![direct_unreachable],
        )
        .expect("sync connectivity");
        let route_graph = network::ClusterConnectivityGraph::read_for_local(&paths, &state);
        assert!(
            dfs_cluster_status_unavailable_reason(&paths, &state, &route_graph, &remote_node_id,)
                .is_some(),
            "the persisted built-in LAN projection models the live failure that used to suppress the routed probe"
        );

        let mut attempts = Vec::new();
        let targets =
            std::collections::BTreeSet::from([second_remote_node_id, remote_node_id.clone()]);
        let expected_order = targets.iter().cloned().collect::<Vec<_>>();
        let outcomes = spawn_remote_dfs_status_probes_with(
            targets,
            DFS_CLUSTER_STATUS_REMOTE_TIMEOUT,
            |node_id, timeout| {
                attempts.push((node_id.clone(), timeout));
                Ok(node_id.clone())
            },
        )
        .expect("canonical routed probe can recover through an unobserved module path");

        assert_eq!(
            attempts,
            expected_order
                .iter()
                .cloned()
                .map(|node_id| (node_id, DFS_CLUSTER_STATUS_REMOTE_TIMEOUT))
                .collect::<Vec<_>>()
        );
        assert_eq!(outcomes, expected_order);
    }

    #[test]
    fn routed_probe_error_is_preserved_by_probe_receiver() {
        let node_id = NodeId::from_legacy_named("node_dfs_status_routed_error").expect("node");
        let (sender, receiver) = std::sync::mpsc::channel();
        sender
            .send(Err(CliError::Network {
                address: "routed object rpc".to_string(),
                detail: "no executable route".to_string(),
            }))
            .expect("send routed error");
        let probe = DfsStatusProbe {
            node_id,
            started_at: std::time::Instant::now(),
            timeout: DFS_CLUSTER_STATUS_REMOTE_TIMEOUT,
            receiver,
        };

        match receive_dfs_status_probe(probe, false, std::time::Instant::now()) {
            Err(CliError::Network { address, detail }) => {
                assert_eq!(address, "routed object rpc");
                assert_eq!(detail, "no executable route");
            }
            other => panic!("expected routed network error, got {other:?}"),
        }
    }

    #[test]
    fn routed_probe_timeout_retains_the_absolute_probe_deadline() {
        let node_id = NodeId::from_legacy_named("node_dfs_status_routed_timeout").expect("node");
        let (_sender, receiver) = std::sync::mpsc::channel();
        let probe = DfsStatusProbe {
            node_id: node_id.clone(),
            started_at: std::time::Instant::now(),
            timeout: std::time::Duration::ZERO,
            receiver,
        };

        match receive_dfs_status_probe(probe, false, std::time::Instant::now()) {
            Err(CliError::Network { address, detail }) => {
                assert_eq!(address, format!("cluster dfs status {node_id}"));
                assert_eq!(detail, "timed out after 0ms");
            }
            other => panic!("expected timeout error, got {other:?}"),
        }
    }
}

#[cfg(test)]
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

struct DfsStatusProbe {
    node_id: NodeId,
    started_at: std::time::Instant,
    timeout: std::time::Duration,
    receiver: std::sync::mpsc::Receiver<Result<sinomo_runtime::dfs::DfsOperatorStatus, CliError>>,
}

fn spawn_remote_dfs_status_probes_with<T>(
    remote_node_ids: impl IntoIterator<Item = NodeId>,
    timeout: std::time::Duration,
    mut spawn: impl FnMut(&NodeId, std::time::Duration) -> Result<T, CliError>,
) -> Result<Vec<T>, CliError> {
    remote_node_ids
        .into_iter()
        .map(|node_id| spawn(&node_id, timeout))
        .collect()
}

fn spawn_dfs_status_probe(
    daemon_address: Option<&str>,
    node_id: &NodeId,
    timeout: std::time::Duration,
    trace: bool,
) -> Result<DfsStatusProbe, CliError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let daemon_address = daemon_address.map(str::to_string);
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
                daemon_address.as_deref(),
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
    daemon_address: Option<&str>,
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
        daemon_address,
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

pub(crate) fn default_dfs_node_id() -> NodeId {
    cluster_paths_from_defaults()
        .and_then(|paths| read_cluster_state(&paths))
        .map(|state| state.local_node_id)
        .unwrap_or_else(|_| fallback_dfs_node_id())
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
