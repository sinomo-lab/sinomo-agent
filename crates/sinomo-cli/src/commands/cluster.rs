use super::*;

#[cfg(test)]
pub(crate) use network::advertised_peer_route_targets;
#[cfg(test)]
pub(crate) use network::apply_cluster_node_role;
#[cfg(test)]
pub(crate) use network::cluster_state_sync_txn;
#[cfg(test)]
pub(crate) use network::invite_channels_for_join_registration;
#[cfg(test)]
pub(crate) use network::invite_communication_channels;
#[cfg(test)]
pub(crate) use network::invite_introducer_endpoint;
#[cfg(test)]
pub(crate) use network::read_cluster_members_from_kv;
#[cfg(test)]
pub(crate) use network::read_cluster_node_statuses_from_kv;
#[cfg(test)]
pub(crate) use network::sign_cluster_join_invite;
pub(crate) use network::{
    cluster_join_command, cluster_state_needs_join_registration, cluster_status_summary_label,
    decode_cluster_join_invite, encode_cluster_join_invite, extract_invite_from_join_command,
    invite_introducer_endpoint_from_state, is_remote_usable_invite_endpoint,
    verify_cluster_join_invite_signature,
};
#[cfg(test)]
pub(crate) use network::{compact_default_invite_introducers, push_unique_introducer};
#[cfg(test)]
pub(crate) use sinomo_runtime::node::apply_cluster_raft_role;
use sinomo_runtime::node::cluster_member_is_local_process;

#[cfg(test)]
pub(crate) fn build_cluster_raft_append_request_from_next_index(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    next_index: u64,
) -> Result<sinomo_runtime::kv::ClusterRaftAppendRequest, network::NetworkError> {
    paths
        .kv_paths()
        .raft_store()
        .build_cluster_raft_append_request_from_next_index(state, next_index)
        .map_err(Into::into)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClusterCliCommand {
    Help,
    Init(ClusterInitOptions),
    Invite(ClusterInviteOptions),
    Join(ClusterJoinOptions),
    Migrate(ClusterMigrateCommand),
    Role(ClusterRoleOptions),
    RaftRole(ClusterRaftRoleOptions),
    RepairMembers(ClusterRepairMembersOptions),
    Remove(ClusterRemoveOptions),
    CleanupLegacyShadowMembers,
    Status,
    Health,
    Doctor,
    Peers,
    Leave,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterInitOptions {
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) name: String,
}

impl Default for ClusterInitOptions {
    fn default() -> Self {
        Self {
            cluster_id: None,
            node_id: None,
            name: "sinomo-local".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterJoinOptions {
    pub(crate) invite: String,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClusterMigrateCommand {
    Plan(ClusterMigrationPlanOptions),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterMigrationPlanOptions {
    pub(crate) invite: String,
    pub(crate) mode: ClusterMigrationMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterInviteOptions {
    pub(crate) ttl_seconds: i64,
    pub(crate) introducers: Vec<String>,
    pub(crate) output: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRoleOptions {
    pub(crate) node_id: NodeId,
    pub(crate) role: ClusterNodeRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRaftRoleOptions {
    pub(crate) node_id: NodeId,
    pub(crate) role: ClusterMemberRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRepairMembersOptions {
    pub(crate) from: String,
    pub(crate) promote: Option<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRemoveOptions {
    pub(crate) node_ids: Vec<NodeId>,
}

impl Default for ClusterInviteOptions {
    fn default() -> Self {
        Self {
            ttl_seconds: CLUSTER_INVITE_DEFAULT_TTL_SECONDS,
            introducers: Vec::new(),
            output: None,
        }
    }
}

pub(crate) fn parse_cluster_command(args: &[String]) -> Result<ClusterCliCommand, CliError> {
    match args.split_first() {
        None => Ok(ClusterCliCommand::Help),
        Some((command, [])) if matches!(command.as_str(), "help" | "--help" | "-h") => {
            Ok(ClusterCliCommand::Help)
        }
        Some((command, rest)) if command == "init" => {
            Ok(ClusterCliCommand::Init(parse_cluster_init_options(rest)?))
        }
        Some((command, rest)) if command == "invite" => Ok(ClusterCliCommand::Invite(
            parse_cluster_invite_options(rest)?,
        )),
        Some((command, rest)) if command == "join" => {
            Ok(ClusterCliCommand::Join(parse_cluster_join_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "migrate" | "migration") => Ok(
            ClusterCliCommand::Migrate(parse_cluster_migrate_command(rest)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "role" | "set-role") => {
            Ok(ClusterCliCommand::Role(parse_cluster_role_options(rest)?))
        }
        Some((command, rest)) if matches!(command.as_str(), "raft-role" | "set-raft-role") => Ok(
            ClusterCliCommand::RaftRole(parse_cluster_raft_role_options(rest)?),
        ),
        Some((command, rest)) if command == "promote" => Ok(ClusterCliCommand::RaftRole(
            parse_cluster_promote_options(rest)?,
        )),
        Some((command, rest)) if matches!(command.as_str(), "repair-members" | "repair") => Ok(
            ClusterCliCommand::RepairMembers(parse_cluster_repair_members_options(rest)?),
        ),
        Some((command, rest)) if matches!(command.as_str(), "remove" | "rm") => Ok(
            ClusterCliCommand::Remove(parse_cluster_remove_options(rest)?),
        ),
        Some((command, []))
            if matches!(
                command.as_str(),
                "cleanup-legacy-shadow-members" | "cleanup-shadow-members" | "cleanup-shadows"
            ) =>
        {
            Ok(ClusterCliCommand::CleanupLegacyShadowMembers)
        }
        Some((command, [])) if command == "status" => Ok(ClusterCliCommand::Status),
        Some((command, [])) if matches!(command.as_str(), "health" | "nodes") => {
            Ok(ClusterCliCommand::Health)
        }
        Some((command, [])) if matches!(command.as_str(), "doctor" | "diagnose") => {
            Ok(ClusterCliCommand::Doctor)
        }
        Some((command, [])) if command == "peers" => Ok(ClusterCliCommand::Peers),
        Some((command, [])) if command == "leave" => Ok(ClusterCliCommand::Leave),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_cluster_init_options(args: &[String]) -> Result<ClusterInitOptions, CliError> {
    let mut options = ClusterInitOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--cluster-id" => {
                index += 1;
                let Some(cluster_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cluster_id = Some(parse_cluster_id(cluster_id)?);
            }
            "--node-id" => {
                index += 1;
                let Some(node_id) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.node_id =
                    Some(
                        NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
                            detail: format!("invalid node id `{node_id}`: {source}"),
                        })?,
                    );
            }
            "--name" => {
                index += 1;
                let Some(name) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.name = non_empty_cluster_name(name)?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_cluster_invite_options(
    args: &[String],
) -> Result<ClusterInviteOptions, CliError> {
    let mut options = ClusterInviteOptions::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--ttl-seconds" => {
                index += 1;
                let Some(ttl) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.ttl_seconds = ttl.parse::<i64>().map_err(|source| CliError::Parse {
                    detail: format!("invalid invite ttl `{ttl}`: {source}"),
                })?;
                if options.ttl_seconds <= 0 {
                    return Err(CliError::Parse {
                        detail: "invite ttl must be greater than zero seconds".to_string(),
                    });
                }
            }
            "--introducer" => {
                index += 1;
                let Some(introducer) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.introducers.push(non_empty_address(introducer)?);
            }
            "--output" => {
                index += 1;
                let Some(output) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                options.output = Some(PathBuf::from(output));
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_cluster_join_options(args: &[String]) -> Result<ClusterJoinOptions, CliError> {
    let Some((invite, rest)) = args.split_first() else {
        return Err(CliError::Usage);
    };
    if invite.starts_with("--") {
        return Err(CliError::Usage);
    }

    let mut options = ClusterJoinOptions {
        invite: invite.clone(),
        cluster_id: None,
        node_id: None,
        name: "sinomo-node".to_string(),
    };
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--cluster-id" => {
                index += 1;
                let Some(cluster_id) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.cluster_id = Some(parse_cluster_id(cluster_id)?);
            }
            "--node-id" => {
                index += 1;
                let Some(node_id) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.node_id =
                    Some(
                        NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
                            detail: format!("invalid node id `{node_id}`: {source}"),
                        })?,
                    );
            }
            "--name" => {
                index += 1;
                let Some(name) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.name = non_empty_cluster_name(name)?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_cluster_migrate_command(
    args: &[String],
) -> Result<ClusterMigrateCommand, CliError> {
    match args.split_first() {
        Some((command, rest)) if command == "plan" => Ok(ClusterMigrateCommand::Plan(
            parse_cluster_migration_plan_options(rest)?,
        )),
        _ => Err(CliError::Usage),
    }
}

pub(crate) fn parse_cluster_migration_plan_options(
    args: &[String],
) -> Result<ClusterMigrationPlanOptions, CliError> {
    let Some((invite, rest)) = args.split_first() else {
        return Err(CliError::Usage);
    };
    if invite.starts_with("--") {
        return Err(CliError::Usage);
    }

    let mut options = ClusterMigrationPlanOptions {
        invite: invite.clone(),
        mode: ClusterMigrationMode::SingleNode,
    };
    let mut index = 0;
    while index < rest.len() {
        match rest[index].as_str() {
            "--mode" => {
                index += 1;
                let Some(mode) = rest.get(index) else {
                    return Err(CliError::Usage);
                };
                options.mode = parse_cluster_migration_mode(mode)?;
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(options)
}

pub(crate) fn parse_cluster_migration_mode(mode: &str) -> Result<ClusterMigrationMode, CliError> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "single-node" | "single_node" | "single" => Ok(ClusterMigrationMode::SingleNode),
        "cluster-merge" | "cluster_merge" | "merge" | "absorb-cluster" | "absorb_cluster" => {
            Ok(ClusterMigrationMode::ClusterMerge)
        }
        _ => Err(CliError::Parse {
            detail: format!(
                "invalid cluster migration mode `{mode}`; expected single-node or cluster-merge"
            ),
        }),
    }
}

pub(crate) fn parse_cluster_role_options(args: &[String]) -> Result<ClusterRoleOptions, CliError> {
    let [node_id, role] = args else {
        return Err(CliError::Usage);
    };
    Ok(ClusterRoleOptions {
        node_id: NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
            detail: format!("invalid node id `{node_id}`: {source}"),
        })?,
        role: parse_cluster_node_role(role)?,
    })
}

pub(crate) fn parse_cluster_raft_role_options(
    args: &[String],
) -> Result<ClusterRaftRoleOptions, CliError> {
    let [node_id, role] = args else {
        return Err(CliError::Usage);
    };
    Ok(ClusterRaftRoleOptions {
        node_id: NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
            detail: format!("invalid node id `{node_id}`: {source}"),
        })?,
        role: parse_cluster_member_role(role)?,
    })
}

pub(crate) fn parse_cluster_promote_options(
    args: &[String],
) -> Result<ClusterRaftRoleOptions, CliError> {
    match args {
        [node_id] => Ok(ClusterRaftRoleOptions {
            node_id: NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
                detail: format!("invalid node id `{node_id}`: {source}"),
            })?,
            role: ClusterMemberRole::Voter,
        }),
        _ => parse_cluster_raft_role_options(args),
    }
}

pub(crate) fn parse_cluster_repair_members_options(
    args: &[String],
) -> Result<ClusterRepairMembersOptions, CliError> {
    let mut from = None;
    let mut promote = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--from" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                from = Some(non_empty_address(value)?);
            }
            "--promote" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(CliError::Usage);
                };
                promote = Some(
                    NodeId::new(value.clone()).map_err(|source| CliError::Parse {
                        detail: format!("invalid node id `{value}`: {source}"),
                    })?,
                );
            }
            value if from.is_none() => {
                from = Some(non_empty_address(value)?);
            }
            _ => return Err(CliError::Usage),
        }
        index += 1;
    }
    Ok(ClusterRepairMembersOptions {
        from: from.ok_or_else(|| CliError::Parse {
            detail: "cluster repair-members requires --from <host:port>".to_string(),
        })?,
        promote,
    })
}

pub(crate) fn parse_cluster_remove_options(
    args: &[String],
) -> Result<ClusterRemoveOptions, CliError> {
    if args.is_empty() {
        return Err(CliError::Usage);
    }
    let mut node_ids = Vec::with_capacity(args.len());
    for node_id in args {
        node_ids.push(
            NodeId::new(node_id.clone()).map_err(|source| CliError::Parse {
                detail: format!("invalid node id `{node_id}`: {source}"),
            })?,
        );
    }
    Ok(ClusterRemoveOptions { node_ids })
}

pub(crate) fn parse_cluster_node_role(role: &str) -> Result<ClusterNodeRole, CliError> {
    match role.trim().to_ascii_lowercase().as_str() {
        "workstation" => Ok(ClusterNodeRole::Workstation),
        "mobile" => Ok(ClusterNodeRole::Mobile),
        "server" => Ok(ClusterNodeRole::Server),
        "sensor" => Ok(ClusterNodeRole::Sensor),
        "test" => Ok(ClusterNodeRole::Test),
        _ => Err(CliError::Parse {
            detail: format!(
                "invalid cluster node role `{role}`; expected workstation, mobile, server, sensor, or test"
            ),
        }),
    }
}

pub(crate) fn parse_cluster_member_role(role: &str) -> Result<ClusterMemberRole, CliError> {
    match role.trim().to_ascii_lowercase().as_str() {
        "voter" => Ok(ClusterMemberRole::Voter),
        "learner" => Ok(ClusterMemberRole::Learner),
        "non-voting" | "non_voting" | "nonvoting" => Ok(ClusterMemberRole::NonVoting),
        _ => Err(CliError::Parse {
            detail: format!(
                "invalid cluster member role `{role}`; expected voter, learner, or non-voting"
            ),
        }),
    }
}

pub(crate) fn parse_cluster_id(cluster_id: &str) -> Result<ClusterId, CliError> {
    ClusterId::new(cluster_id.to_string()).map_err(|source| CliError::Parse {
        detail: format!("invalid cluster id `{cluster_id}`: {source}"),
    })
}

pub(crate) fn non_empty_cluster_name(name: &str) -> Result<String, CliError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(CliError::Parse {
            detail: "cluster node name cannot be empty".to_string(),
        });
    }
    Ok(name.to_string())
}

pub(crate) fn non_empty_address(address: &str) -> Result<String, CliError> {
    let address = address.trim();
    if address.is_empty() {
        return Err(CliError::Parse {
            detail: "cluster introducer address cannot be empty".to_string(),
        });
    }
    Ok(address.to_string())
}

pub(crate) fn run_cluster_cli_command(command: ClusterCliCommand) -> Result<(), CliError> {
    match command {
        ClusterCliCommand::Help => {
            print_cluster_help();
            Ok(())
        }
        ClusterCliCommand::Init(options) => cluster_init(options),
        ClusterCliCommand::Invite(options) => cluster_invite(options),
        ClusterCliCommand::Join(options) => cluster_join(options),
        ClusterCliCommand::Migrate(command) => cluster_migrate(command),
        ClusterCliCommand::Role(options) => cluster_role(options),
        ClusterCliCommand::RaftRole(options) => cluster_raft_role(options),
        ClusterCliCommand::RepairMembers(options) => cluster_repair_members(options),
        ClusterCliCommand::Remove(options) => cluster_remove(options),
        ClusterCliCommand::CleanupLegacyShadowMembers => cluster_cleanup_legacy_shadow_members(),
        ClusterCliCommand::Status => cluster_status(),
        ClusterCliCommand::Health => cluster_health(),
        ClusterCliCommand::Doctor => cluster_doctor(),
        ClusterCliCommand::Peers => cluster_peers(),
        ClusterCliCommand::Leave => cluster_leave(),
    }
}

pub(crate) fn cluster_paths_from_defaults() -> Result<ClusterPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(ClusterPaths::new(&data_dir))
}

fn cluster_kv_raft_store(paths: &ClusterPaths) -> sinomo_runtime::kv::ClusterKvRaftStore {
    paths.kv_paths().raft_store()
}

fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: sinomo_runtime::kv::KvCommand,
) -> Result<KvSnapshot, network::NetworkError> {
    cluster_kv_raft_store(paths)
        .append_command_and_publish(
            node_id,
            command,
            sinomo_runtime::kv::cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(Into::into)
}

pub(crate) fn cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, network::NetworkError> {
    Ok(cluster_kv_raft_store(paths)
        .known_leader_id(&state.local_node_id)?
        .filter(|leader_id| sinomo_runtime::kv::cluster_kv_raft_node_is_voter(state, leader_id)))
}

pub(crate) fn locked_cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, network::NetworkError> {
    Ok(cluster_kv_raft_store(paths)
        .locked_known_leader_id(&state.local_node_id)?
        .filter(|leader_id| sinomo_runtime::kv::cluster_kv_raft_node_is_voter(state, leader_id)))
}

pub(crate) fn locked_cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, network::NetworkError> {
    Ok(sinomo_runtime::kv::cluster_kv_raft_local_is_voter(state)
        && locked_cluster_raft_known_leader_id(paths, state)?.as_ref()
            == Some(&state.local_node_id))
}

#[cfg(test)]
pub(crate) fn set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, network::NetworkError> {
    cluster_kv_raft_store(paths)
        .set_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

pub(crate) fn locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, network::NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_set_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn clear_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, network::NetworkError> {
    cluster_kv_raft_store(paths)
        .clear_leader_and_write_snapshot_if_changed(node_id)
        .map_err(Into::into)
}

pub(crate) fn require_cluster_bootstrap_state() -> Result<ClusterBootstrapState, CliError> {
    let paths = cluster_paths_from_defaults()?;
    read_cluster_state(&paths)
}

pub(crate) fn require_cluster_runtime_quorum_with_paths_and_daemon(
    paths: &ClusterPaths,
    daemon: Option<&DaemonStatus>,
) -> Result<(), CliError> {
    let state = read_cluster_state(&paths).map_err(|error| match error {
        CliError::ClusterOrphan => CliError::ClusterReadOnly {
            detail: "local node has no cluster state; cluster-dependent operations are paused"
                .to_string(),
        },
        other => other,
    })?;
    if daemon.is_none() {
        return Err(CliError::ClusterReadOnly {
            detail: "local daemon is not running; cluster-dependent operations are paused"
                .to_string(),
        });
    }
    if let Some(daemon) = daemon
        && local_daemon_can_serve_cluster_kv_snapshot(daemon, &state)
    {
        return Ok(());
    }
    if cluster_has_local_quorum(&paths, &state) {
        Ok(())
    } else {
        Err(CliError::ClusterReadOnly {
            detail: format!(
                "quorum unavailable; cluster-dependent operations are paused ({} reachable voters, {} required)",
                cluster_reachable_voter_count(&paths, &state),
                sinomo_runtime::kv::cluster_kv_raft_quorum_size_for_state(&state)
            ),
        })
    }
}

pub(crate) fn local_daemon_can_serve_cluster_kv_snapshot(
    daemon: &DaemonStatus,
    state: &ClusterBootstrapState,
) -> bool {
    let Some(cluster) = daemon.cluster.as_ref() else {
        return false;
    };
    if cluster.cluster_id != state.cluster_id || cluster.node_id != state.local_node_id {
        return false;
    }
    daemon_fetch_cluster_kv_snapshot(&daemon.address, DAEMON_PROBE_TIMEOUT).is_ok()
}

pub(crate) fn cluster_init(options: ClusterInitOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    cluster_init_with_paths(options, &paths)
}

pub(crate) fn cluster_init_with_paths(
    options: ClusterInitOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    let state = initialize_single_node_cluster_with_paths(options, paths)?;
    println!("cluster initialized: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("state: {}", paths.state.display());
    Ok(())
}

pub(crate) fn initialize_single_node_cluster_with_paths(
    options: ClusterInitOptions,
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, CliError> {
    paths.ensure()?;
    if paths.state.exists() {
        return Err(CliError::Parse {
            detail: format!(
                "cluster is already initialized at {}; use `sinomo cluster status`",
                paths.state.display()
            ),
        });
    }

    let now = OffsetDateTime::now_utc();
    let local_node_id = options.node_id.unwrap_or_else(NodeId::generate);
    let cluster_id = options.cluster_id.unwrap_or_else(ClusterId::generate);
    let name = options.name;
    let (main_public_key, shadow_public_key) = local_daemon_public_keys(paths)?;
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        name.clone(),
        local_node_id,
        name,
        main_public_key.clone(),
        now,
    );
    ensure_local_daemon_cluster_members(
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        now,
    );
    network::assign_local_daemon_member_public_keys(&mut state, main_public_key, shadow_public_key);
    network::write_cluster_state(paths, &state)?;
    locked_set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
    sync_cluster_state_to_kv(paths, &state)?;

    Ok(state)
}

pub(crate) fn cluster_invite(options: ClusterInviteOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let rendered = cluster_invite_with_paths(options, &paths)?;
    println!("invite: {}", rendered.uri);
    println!("join command: {}", rendered.join_command);
    println!("cluster: {}", rendered.invite.cluster_id);
    println!("expires: {}", format_rfc3339(rendered.invite.expires_at));
    println!("saved: {}", rendered.persisted_key);
    if let Some(output) = rendered.output_path {
        println!("output: {}", output.display());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RenderedClusterInvite {
    pub(crate) invite: ClusterJoinInvite,
    pub(crate) uri: String,
    pub(crate) join_command: String,
    pub(crate) persisted_key: String,
    pub(crate) output_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterMigrationPlanReport {
    pub(crate) migration: ClusterMigrationRecord,
    pub(crate) target_invite_id: ClusterInviteId,
    pub(crate) source_member_count: usize,
    pub(crate) source_remote_member_count: usize,
    pub(crate) target_introducer_count: usize,
    pub(crate) target_communication_channel_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterLeaveReport {
    pub(crate) previous_cluster_id: Option<ClusterId>,
    pub(crate) new_cluster_id: ClusterId,
    pub(crate) local_node_id: NodeId,
    pub(crate) quarantine_root: Option<PathBuf>,
    pub(crate) state_path: PathBuf,
}

pub(crate) fn cluster_migrate(command: ClusterMigrateCommand) -> Result<(), CliError> {
    match command {
        ClusterMigrateCommand::Plan(options) => {
            let paths = cluster_paths_from_defaults()?;
            let report = cluster_migration_plan_with_paths(options, &paths)?;
            print_cluster_migration_plan_report(&report);
            Ok(())
        }
    }
}

pub(crate) fn cluster_migration_plan_with_paths(
    options: ClusterMigrationPlanOptions,
    paths: &ClusterPaths,
) -> Result<ClusterMigrationPlanReport, CliError> {
    paths.ensure()?;
    let source = read_cluster_state(paths).map_err(|error| match error {
        CliError::ClusterOrphan => CliError::Parse {
            detail: "local node has no initialized source cluster; use `sinomo cluster join <invite>` for a fresh node".to_string(),
        },
        other => other,
    })?;
    if cluster_state_needs_join_registration(&source) {
        return Err(CliError::Parse {
            detail:
                "local cluster state is already a pending join; retry `sinomo cluster join <invite>`"
                    .to_string(),
        });
    }
    if options.mode == ClusterMigrationMode::SingleNode
        && !network::cluster_is_settled_solo(&source)
    {
        return Err(CliError::Parse {
            detail:
                "single-node migration requires a settled source cluster with no remote peers; use `--mode cluster-merge` to plan a cluster merge"
                    .to_string(),
        });
    }

    let timestamp = OffsetDateTime::now_utc();
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    if network::invite_has_embedded_signature(&invite) {
        verify_cluster_join_invite_signature(&invite)?;
    }
    if invite.expires_at != OffsetDateTime::UNIX_EPOCH && invite.expires_at <= timestamp {
        return Err(CliError::Parse {
            detail: format!(
                "cluster invite expired at {}",
                format_rfc3339(invite.expires_at)
            ),
        });
    }
    if invite.cluster_id == source.cluster_id {
        return Err(CliError::Parse {
            detail: format!(
                "source cluster `{}` already matches the target invite cluster",
                source.cluster_id
            ),
        });
    }

    let source_member_count = source
        .members
        .iter()
        .filter(|member| !matches!(member.state, ClusterMemberState::Removed))
        .count();
    let source_remote_member_count = source
        .members
        .iter()
        .filter(|member| {
            !matches!(member.state, ClusterMemberState::Removed)
                && !cluster_member_is_local_process(&source, member)
        })
        .count();
    let migration = ClusterMigrationRecord {
        migration_id: Id::generate(),
        mode: options.mode,
        phase: ClusterMigrationPhase::Planned,
        source_cluster_id: source.cluster_id.clone(),
        source_cluster_name: source.cluster_name.clone(),
        target_cluster_id: invite.cluster_id.clone(),
        target_cluster_name: invite.cluster_name.clone(),
        source_local_node_id: source.local_node_id.clone(),
        target_node_id: source.local_node_id.clone(),
        preserved_node_ids: vec![source.local_node_id.clone()],
        remapped_node_ids: Vec::new(),
        evidence_refs: vec![format!("cluster-state:{}", paths.state.display())],
        detail: Some(
            "migration plan only; execution must freeze the source, quarantine old cluster KV/Raft state, register with the target cluster, then verify imported state"
                .to_string(),
        ),
        created_at: timestamp,
        updated_at: timestamp,
    };

    Ok(ClusterMigrationPlanReport {
        migration,
        target_invite_id: invite.invite_id,
        source_member_count,
        source_remote_member_count,
        target_introducer_count: invite.introducers.len(),
        target_communication_channel_count: invite.communication_channels.len(),
    })
}

fn print_cluster_migration_plan_report(report: &ClusterMigrationPlanReport) {
    let target_name = report
        .migration
        .target_cluster_name
        .as_deref()
        .unwrap_or("unknown");
    println!("cluster migration plan: {}", report.migration.migration_id);
    println!("mode: {}", report.migration.mode.as_str());
    println!("phase: {}", report.migration.phase.as_str());
    println!(
        "source cluster: {} ({})",
        report.migration.source_cluster_name, report.migration.source_cluster_id
    );
    println!(
        "target cluster: {} ({})",
        target_name, report.migration.target_cluster_id
    );
    println!(
        "local node: {} -> {}",
        report.migration.source_local_node_id, report.migration.target_node_id
    );
    println!(
        "source members: {} (remote: {})",
        report.source_member_count, report.source_remote_member_count
    );
    println!("target invite: {}", report.target_invite_id);
    println!(
        "target endpoints: {} introducers, {} signed channels",
        report.target_introducer_count, report.target_communication_channel_count
    );
    println!(
        "next: freeze source cluster state, quarantine old KV/Raft storage, register with the target, import local data, then verify before finalizing"
    );
}

pub(crate) fn cluster_invite_with_paths(
    options: ClusterInviteOptions,
    paths: &ClusterPaths,
) -> Result<RenderedClusterInvite, CliError> {
    paths.ensure()?;
    let state = read_cluster_state(paths)?;
    if !sinomo_runtime::kv::cluster_kv_raft_local_is_voter(&state) {
        return Err(cluster_invite_requires_voter_error(paths, &state));
    }
    let now = OffsetDateTime::now_utc();
    let invite = build_cluster_invite(&state, &options, paths, now)?;
    if invite
        .introducers
        .iter()
        .all(|endpoint| !is_remote_usable_invite_endpoint(endpoint))
    {
        println!(
            "warning: invite only contains local-only introducers; remote machines should use `sinomo cluster invite --introducer <host-lan-ip>:21353`"
        );
    }
    let uri = encode_cluster_join_invite(&invite)?;
    let join_command = cluster_join_command(&uri);
    publish_cluster_invitation(paths, &state, &invite)?;
    if let Some(output) = &options.output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).map_err(|source| CliError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(output, format!("{join_command}\n")).map_err(|source| CliError::Io {
            path: output.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(RenderedClusterInvite {
        persisted_key: sinomo_runtime::node::cluster_invitation_key(&invite.invite_id),
        invite,
        uri,
        join_command,
        output_path: options.output,
    })
}

pub(crate) fn publish_cluster_invitation(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
) -> Result<(), CliError> {
    match network::publish_cluster_invitation_to_quorum_with_host(
        paths,
        state,
        invite,
        &mut CliClusterInvitePublishHost,
    )
    .map(|_| ())
    {
        Ok(()) => Ok(()),
        Err(CliError::ClusterReadOnly { .. })
            if !sinomo_runtime::kv::cluster_kv_raft_local_is_voter(state) =>
        {
            publish_cluster_invitation_via_voter(paths, state, invite)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn publish_cluster_invitation_via_voter(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    _invite: &ClusterJoinInvite,
) -> Result<(), CliError> {
    Err(cluster_invite_requires_voter_error(paths, state))
}

fn cluster_invite_requires_voter_error(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> CliError {
    let targets = network::ClusterEndpointSelector::new(paths, state)
        .select(network::ClusterEndpointSelectionPurpose::InvitePublish);
    CliError::ClusterReadOnly {
        detail: format!(
            "local node is not a voter; create the invite on a voter node or restore voter connectivity{}",
            if targets.is_empty() {
                String::new()
            } else {
                format!(
                    " (best known voter target: {} via {:?})",
                    targets[0].address, targets[0].transport
                )
            }
        ),
    }
}

pub(crate) fn build_cluster_invite(
    state: &ClusterBootstrapState,
    options: &ClusterInviteOptions,
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<ClusterJoinInvite, CliError> {
    let introducers = if options.introducers.is_empty() {
        default_invite_introducers(state, paths)
    } else {
        options
            .introducers
            .iter()
            .map(|address| invite_introducer_endpoint_from_state(state, address.clone()))
            .collect()
    };
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let issuer_identity = load_or_create_main_node_identity(data_dir)?;
    network::build_cluster_join_invite(
        state,
        &issuer_identity,
        introducers,
        options.ttl_seconds,
        now,
    )
    .map_err(Into::into)
}

pub(crate) fn default_invite_introducers(
    state: &ClusterBootstrapState,
    paths: &ClusterPaths,
) -> Vec<ClusterEndpointRecord> {
    let mut introducers = Vec::new();
    if let Some(data_dir) = paths.root.parent() {
        let daemon_paths = DaemonPaths::with_root(data_dir.join("daemon"));
        if let DaemonProbeStatus::Running(status) =
            probe_daemon_status(&daemon_paths, DAEMON_PROBE_TIMEOUT)
        {
            for endpoint in invite_introducers_from_daemon_status(&state.local_node_id, &status) {
                network::push_unique_introducer(&mut introducers, endpoint);
            }
            if let Ok(record) = network::read_iroh_endpoint_record(paths, &state.local_node_id) {
                network::push_unique_introducer(&mut introducers, record.endpoint());
            }
        }
    }

    for endpoint in state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized()
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == "daemon.main.run")
        })
        .flat_map(|member| member.endpoints.iter().cloned())
    {
        network::push_unique_introducer(&mut introducers, endpoint);
    }
    network::sort_default_invite_introducers(&mut introducers);
    network::compact_default_invite_introducers(&mut introducers);
    introducers
}

pub(crate) fn cluster_join(options: ClusterJoinOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    cluster_join_with_paths(options, &paths)
}

pub(crate) fn cluster_join_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    paths.ensure()?;
    if paths.state.exists() {
        let existing = read_cluster_state(paths)?;
        if cluster_state_needs_join_registration(&existing) {
            return cluster_join_retry_with_paths(options, paths, existing);
        }
        return cluster_join_single_node_migration_with_paths(options, paths, existing);
    }
    cluster_join_fresh_with_paths(options, paths)
}

fn cluster_join_fresh_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    let local_node_id = options.node_id.unwrap_or_else(NodeId::generate);
    let timestamp = OffsetDateTime::now_utc();
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    if network::invite_has_embedded_signature(&invite) {
        verify_cluster_join_invite_signature(&invite)?;
    }
    if invite.expires_at != OffsetDateTime::UNIX_EPOCH && invite.expires_at <= timestamp {
        return Err(CliError::Parse {
            detail: format!(
                "cluster invite expired at {}",
                format_rfc3339(invite.expires_at)
            ),
        });
    }
    if let Some(cluster_id) = &options.cluster_id
        && network::invite_has_embedded_cluster_identity(&invite)
        && cluster_id != &invite.cluster_id
    {
        return Err(CliError::Parse {
            detail: format!(
                "cluster id override `{cluster_id}` does not match invite cluster `{}`",
                invite.cluster_id
            ),
        });
    }
    let cluster_id = options
        .cluster_id
        .clone()
        .unwrap_or_else(|| invite.cluster_id.clone());
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    let (main_public_key, shadow_public_key) = local_daemon_public_keys(paths)?;
    let mut state = network::prepare_pending_cluster_join_state(
        cluster_id,
        invite
            .cluster_name
            .clone()
            .unwrap_or_else(|| options.name.clone()),
        local_node_id.clone(),
        options.name,
        main_public_key,
        shadow_public_key,
        timestamp,
    );
    ensure_local_daemon_cluster_members(&mut state, &main_options, &shadow_options, timestamp);
    network::write_cluster_state(paths, &state)?;
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    network::write_cluster_state(paths, &state)?;
    println!("cluster join recorded: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
}

fn cluster_join_single_node_migration_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
    source: ClusterBootstrapState,
) -> Result<(), CliError> {
    let report = cluster_migration_plan_with_paths(
        ClusterMigrationPlanOptions {
            invite: options.invite.clone(),
            mode: ClusterMigrationMode::SingleNode,
        },
        paths,
    )?;
    if report.migration.source_cluster_id != source.cluster_id {
        return Err(CliError::Parse {
            detail: "source cluster changed while planning migration; retry cluster join"
                .to_string(),
        });
    }

    let mut migration = report.migration;
    write_cluster_migration_record_to_source_kv(paths, &migration)?;
    let quarantine = quarantine_cluster_storage_for_migration(paths, &migration)?;
    let now = OffsetDateTime::now_utc();
    migration.phase = ClusterMigrationPhase::SourceFrozen;
    migration.updated_at = now;
    migration
        .evidence_refs
        .push(format!("source-quarantine:{}", quarantine.root.display()));
    migration.evidence_refs.push(format!(
        "dfs-import-namespace:{}",
        dfs_import_namespace_root(&migration.source_cluster_id)
    ));
    write_local_cluster_migration_record(&quarantine.root, &migration)?;

    let target_join_options = ClusterJoinOptions {
        node_id: Some(migration.target_node_id.clone()),
        ..options
    };

    match cluster_join_fresh_with_paths(target_join_options, paths) {
        Ok(()) => {
            let joined = read_cluster_state(paths)?;
            migration.target_cluster_id = joined.cluster_id.clone();
            migration.target_cluster_name = Some(joined.cluster_name.clone());
            migration.target_node_id = joined.local_node_id.clone();
            let kv_import =
                import_node_local_configs_from_source_kv(paths, &quarantine, &migration)?;
            migration.phase = ClusterMigrationPhase::Finalized;
            migration.updated_at = OffsetDateTime::now_utc();
            migration
                .evidence_refs
                .push("source-kv-policy:target-authoritative".to_string());
            migration.evidence_refs.push(format!(
                "source-kv-import-report:{}",
                cluster_migration_kv_import_report_path(&quarantine.root).display()
            ));
            migration.detail = Some(format!(
                "single-node migration finalized; target cluster KV is authoritative; source cluster storage is quarantined locally; imported {} node-local config keys, deferred {}, skipped {} source cluster config keys; DFS shared import is deferred",
                kv_import.imported_node_local_config_keys.len(),
                kv_import.deferred_node_local_config_keys.len(),
                kv_import.skipped_cluster_level_config_count,
            ));
            write_local_cluster_migration_record(&quarantine.root, &migration)?;
            println!("cluster migration finalized: {}", migration.migration_id);
            println!("source cluster: {}", migration.source_cluster_id);
            println!("source quarantine: {}", quarantine.root.display());
            Ok(())
        }
        Err(error) => {
            migration.phase = ClusterMigrationPhase::RolledBack;
            migration.updated_at = OffsetDateTime::now_utc();
            migration.detail = Some(format!(
                "target join failed and source cluster storage was restored: {error}"
            ));
            let _ = write_local_cluster_migration_record(&quarantine.root, &migration);
            rollback_cluster_storage_quarantine(&quarantine)?;
            Err(error)
        }
    }
}

#[derive(Clone, Debug)]
struct ClusterMigrationQuarantine {
    root: PathBuf,
    moved: Vec<ClusterMigrationQuarantinedPath>,
}

#[derive(Clone, Debug)]
struct ClusterMigrationQuarantinedPath {
    original: PathBuf,
    quarantined: PathBuf,
}

fn quarantine_cluster_storage_for_migration(
    paths: &ClusterPaths,
    migration: &ClusterMigrationRecord,
) -> Result<ClusterMigrationQuarantine, CliError> {
    let root = paths
        .root
        .join("migrations")
        .join(migration.migration_id.to_string())
        .join("source");
    fs::create_dir_all(&root).map_err(|source| CliError::Io {
        path: root.display().to_string(),
        detail: source.to_string(),
    })?;

    let mut quarantine = ClusterMigrationQuarantine {
        root,
        moved: Vec::new(),
    };
    move_cluster_path_if_exists(
        paths.state.clone(),
        quarantine.root.join("state.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.connectivity_local.clone(),
        quarantine.root.join("connectivity-local.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_snapshot.clone(),
        quarantine.root.join("kv-snapshot.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_raft.clone(),
        quarantine.root.join("kv-raft"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watches.clone(),
        quarantine.root.join("watches"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watches_lock.clone(),
        quarantine.root.join("watches.lock"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watches_next_id.clone(),
        quarantine.root.join("watches-next.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watch_events.clone(),
        quarantine.root.join("watch-events.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watch_events_log.clone(),
        quarantine.root.join("watch-events.sobjlog"),
        &mut quarantine,
    )?;
    if let Some(data_dir_root) = paths.root.parent() {
        let data_dir = SinomoDataDir::new(data_dir_root);
        move_cluster_path_if_exists(
            default_dfs_metadata_shards_root(&data_dir),
            dfs_import_metadata_root(&data_dir, &migration.source_cluster_id),
            &mut quarantine,
        )?;
    }
    Ok(quarantine)
}

fn quarantine_cluster_storage_for_leave(
    paths: &ClusterPaths,
    leave_id: &Id,
) -> Result<ClusterMigrationQuarantine, CliError> {
    let root = paths
        .root
        .join("migrations")
        .join(leave_id.to_string())
        .join("leave-source");
    fs::create_dir_all(&root).map_err(|source| CliError::Io {
        path: root.display().to_string(),
        detail: source.to_string(),
    })?;

    let mut quarantine = ClusterMigrationQuarantine {
        root,
        moved: Vec::new(),
    };
    move_cluster_path_if_exists(
        paths.state.clone(),
        quarantine.root.join("state.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.connectivity_local.clone(),
        quarantine.root.join("connectivity-local.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_snapshot.clone(),
        quarantine.root.join("kv-snapshot.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_raft.clone(),
        quarantine.root.join("kv-raft"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watches.clone(),
        quarantine.root.join("watches"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watches_lock.clone(),
        quarantine.root.join("watches.lock"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watches_next_id.clone(),
        quarantine.root.join("watches-next.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watch_events.clone(),
        quarantine.root.join("watch-events.sobj"),
        &mut quarantine,
    )?;
    move_cluster_path_if_exists(
        paths.kv_watch_events_log.clone(),
        quarantine.root.join("watch-events.sobjlog"),
        &mut quarantine,
    )?;
    Ok(quarantine)
}

fn cluster_storage_has_live_artifacts(paths: &ClusterPaths) -> bool {
    paths.state.exists()
        || paths.connectivity_local.exists()
        || paths.kv_snapshot.exists()
        || paths.kv_raft.exists()
        || paths.kv_watches.exists()
        || paths.kv_watches_lock.exists()
        || paths.kv_watches_next_id.exists()
        || paths.kv_watch_events.exists()
        || paths.kv_watch_events_log.exists()
}

fn move_cluster_path_if_exists(
    original: PathBuf,
    quarantined: PathBuf,
    quarantine: &mut ClusterMigrationQuarantine,
) -> Result<(), CliError> {
    if !original.exists() {
        return Ok(());
    }
    if quarantined.exists() {
        return Err(CliError::Io {
            path: quarantined.display().to_string(),
            detail: "migration quarantine target already exists".to_string(),
        });
    }
    if let Some(parent) = quarantined.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::rename(&original, &quarantined).map_err(|source| CliError::Io {
        path: original.display().to_string(),
        detail: source.to_string(),
    })?;
    quarantine.moved.push(ClusterMigrationQuarantinedPath {
        original,
        quarantined,
    });
    Ok(())
}

fn rollback_cluster_storage_quarantine(
    quarantine: &ClusterMigrationQuarantine,
) -> Result<(), CliError> {
    for moved in quarantine.moved.iter().rev() {
        if !moved.quarantined.exists() {
            continue;
        }
        if moved.original.exists() {
            return Err(CliError::Io {
                path: moved.original.display().to_string(),
                detail: "cannot rollback migration quarantine because original path exists"
                    .to_string(),
            });
        }
        if let Some(parent) = moved.original.parent() {
            fs::create_dir_all(parent).map_err(|source| CliError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::rename(&moved.quarantined, &moved.original).map_err(|source| CliError::Io {
            path: moved.quarantined.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    Ok(())
}

fn write_local_cluster_migration_record(
    quarantine_root: &Path,
    migration: &ClusterMigrationRecord,
) -> Result<(), CliError> {
    fs::create_dir_all(quarantine_root).map_err(|source| CliError::Io {
        path: quarantine_root.display().to_string(),
        detail: source.to_string(),
    })?;
    let path = quarantine_root.join("migration.json");
    let json = serde_json::to_vec_pretty(migration).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(&path, json).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn write_cluster_migration_record_to_source_kv(
    paths: &ClusterPaths,
    migration: &ClusterMigrationRecord,
) -> Result<(), CliError> {
    let op = KvOp::put_serde(
        cluster_migration_key(&migration.migration_id),
        migration,
        None,
    )?;
    append_cluster_raft_command(
        paths,
        &migration.source_local_node_id,
        sinomo_runtime::kv::KvCommand::Op(op),
    )
    .map(|_| ())
    .map_err(Into::into)
}

const CLUSTER_MIGRATION_NODE_LOCAL_CONFIG_ROOT: &str = "/config/nodes/";
const CLUSTER_MIGRATION_KEY_SAMPLE_LIMIT: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct ClusterMigrationKvImportReport {
    source_cluster_id: ClusterId,
    target_cluster_id: ClusterId,
    source_node_id: NodeId,
    target_node_id: NodeId,
    target_kv_authoritative: bool,
    source_snapshot_present: bool,
    source_entry_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    imported_node_local_config_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deferred_node_local_config_keys: Vec<String>,
    skipped_cluster_level_config_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    skipped_cluster_level_config_key_samples: Vec<String>,
    skipped_runtime_or_source_count: usize,
}

fn import_node_local_configs_from_source_kv(
    paths: &ClusterPaths,
    quarantine: &ClusterMigrationQuarantine,
    migration: &ClusterMigrationRecord,
) -> Result<ClusterMigrationKvImportReport, CliError> {
    let mut report = ClusterMigrationKvImportReport {
        source_cluster_id: migration.source_cluster_id.clone(),
        target_cluster_id: migration.target_cluster_id.clone(),
        source_node_id: migration.source_local_node_id.clone(),
        target_node_id: migration.target_node_id.clone(),
        target_kv_authoritative: true,
        source_snapshot_present: false,
        source_entry_count: 0,
        imported_node_local_config_keys: Vec::new(),
        deferred_node_local_config_keys: Vec::new(),
        skipped_cluster_level_config_count: 0,
        skipped_cluster_level_config_key_samples: Vec::new(),
        skipped_runtime_or_source_count: 0,
    };

    let source_kv_paths = sinomo_runtime::kv::ClusterKvPaths::new(quarantine.root.clone());
    if !source_kv_paths.snapshot.exists() {
        write_cluster_migration_kv_import_report(&quarantine.root, &report)?;
        return Ok(report);
    }

    let source_snapshot = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&source_kv_paths)
        .map_err(network::NetworkError::from)?;
    report.source_snapshot_present = true;
    report.source_entry_count = source_snapshot.entries.len();

    let source_prefix = cluster_migration_node_local_config_prefix(&migration.source_local_node_id);
    let target_prefix = cluster_migration_node_local_config_prefix(&migration.target_node_id);
    let mut import_ops = Vec::new();
    let mut import_keys = Vec::new();
    for entry in &source_snapshot.entries {
        if let Some(suffix) = entry.key.strip_prefix(&source_prefix) {
            let target_key = format!("{target_prefix}{suffix}");
            import_keys.push(target_key.clone());
            import_ops.push(KvOp::Put {
                key: target_key,
                value: entry.value.clone(),
                lease_id: None,
            });
            continue;
        }

        if entry.key.starts_with("/config/") {
            report.skipped_cluster_level_config_count =
                report.skipped_cluster_level_config_count.saturating_add(1);
            if report.skipped_cluster_level_config_key_samples.len()
                < CLUSTER_MIGRATION_KEY_SAMPLE_LIMIT
            {
                report
                    .skipped_cluster_level_config_key_samples
                    .push(entry.key.clone());
            }
        } else {
            report.skipped_runtime_or_source_count =
                report.skipped_runtime_or_source_count.saturating_add(1);
        }
    }

    if import_ops.is_empty() {
        write_cluster_migration_kv_import_report(&quarantine.root, &report)?;
        return Ok(report);
    }

    let mut target_state = read_cluster_state(paths)?;
    if cluster_state_needs_join_registration(&target_state) {
        report.deferred_node_local_config_keys = import_keys;
        write_cluster_migration_kv_import_report(&quarantine.root, &report)?;
        return Ok(report);
    }

    cli_commit_cluster_kv_txn_with_quorum(
        paths,
        &mut target_state,
        KvTxn {
            compares: Vec::new(),
            success: import_ops,
            failure: Vec::new(),
        },
    )?;
    report.imported_node_local_config_keys = import_keys;
    write_cluster_migration_kv_import_report(&quarantine.root, &report)?;
    Ok(report)
}

fn cluster_migration_node_local_config_prefix(node_id: &NodeId) -> String {
    format!("{CLUSTER_MIGRATION_NODE_LOCAL_CONFIG_ROOT}{node_id}/")
}

fn cluster_migration_kv_import_report_path(quarantine_root: &Path) -> PathBuf {
    quarantine_root.join("kv-import.json")
}

fn write_cluster_migration_kv_import_report(
    quarantine_root: &Path,
    report: &ClusterMigrationKvImportReport,
) -> Result<(), CliError> {
    fs::create_dir_all(quarantine_root).map_err(|source| CliError::Io {
        path: quarantine_root.display().to_string(),
        detail: source.to_string(),
    })?;
    let path = cluster_migration_kv_import_report_path(quarantine_root);
    let json = serde_json::to_vec_pretty(report).map_err(|source| CliError::Json {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(&path, json).map_err(|source| CliError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

pub(crate) fn cluster_join_retry_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
) -> Result<(), CliError> {
    let timestamp = OffsetDateTime::now_utc();
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    if network::invite_has_embedded_signature(&invite) {
        verify_cluster_join_invite_signature(&invite)?;
    }
    if network::invite_has_embedded_cluster_identity(&invite)
        && invite.cluster_id != state.cluster_id
    {
        return Err(CliError::Parse {
            detail: format!(
                "join retry invite cluster `{}` does not match local cluster `{}`",
                invite.cluster_id, state.cluster_id
            ),
        });
    }
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    let (main_public_key, shadow_public_key) = local_daemon_public_keys(paths)?;
    state = network::prepare_cluster_join_retry_state(
        state,
        main_public_key,
        shadow_public_key,
        timestamp,
    );
    ensure_local_daemon_cluster_members(&mut state, &main_options, &shadow_options, timestamp);
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    state.updated_at = timestamp;
    network::write_cluster_state(paths, &state)?;
    println!("cluster join reconciled: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
}

fn local_daemon_public_keys(paths: &ClusterPaths) -> Result<(String, String), CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    Ok((main_identity.public_key, shadow_identity.public_key))
}

fn ensure_local_daemon_cluster_members(
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> bool {
    let (main_endpoints, shadow_endpoints) =
        local_daemon_member_endpoints(state, main_options, shadow_options);
    network::ensure_daemon_cluster_members(state, main_endpoints, shadow_endpoints, timestamp)
}

fn local_daemon_member_endpoints(
    state: &ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
) -> (Vec<ClusterEndpointRecord>, Vec<ClusterEndpointRecord>) {
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let lan_ips = network::discover_default_lan_ip_candidates();
    let route_ips = network::discover_peer_route_ip_candidates(state);
    let main_endpoints = daemon_advertised_endpoints_for_state(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
    );
    let shadow_endpoints = daemon_advertised_endpoints_for_state(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
    );
    (main_endpoints, shadow_endpoints)
}

fn refresh_local_daemon_membership_from_disk_for_cli(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<network::ClusterLocalDaemonMembershipRefresh, CliError> {
    let (main_endpoints, shadow_endpoints) =
        local_daemon_member_endpoints(state, main_options, shadow_options);
    let (main_public_key, shadow_public_key) = local_daemon_public_keys(paths)?;
    network::refresh_local_daemon_membership_from_disk(
        paths,
        state,
        network::ClusterLocalDaemonMembershipObservation {
            main_endpoints,
            shadow_endpoints,
            main_public_key,
            shadow_public_key,
            timestamp,
        },
    )
    .map_err(Into::into)
}

pub(crate) fn reconcile_cluster_join_with_introducers(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    paths: Option<&ClusterPaths>,
) -> Result<(), CliError> {
    fn unsupported_join_reconciliation_transport(operation: &str) -> network::NetworkError {
        network::NetworkError::Io {
            operation: operation.to_string(),
            detail: "cluster join reconciliation only supports membership RPC and snapshot fetch"
                .to_string(),
        }
    }

    struct CliClusterJoinReconciliationTransport;

    impl network::ClusterControlPlaneTransport for CliClusterJoinReconciliationTransport {
        fn send_raft_append(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &sinomo_runtime::kv::ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> Result<sinomo_runtime::kv::ClusterRaftAppendResponse, network::NetworkError> {
            Err(unsupported_join_reconciliation_transport(
                "cluster join raft append",
            ))
        }

        fn send_raft_vote(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &sinomo_runtime::kv::ClusterRaftVoteRequest,
            _timeout: Duration,
        ) -> Result<sinomo_runtime::kv::ClusterRaftVoteResponse, network::NetworkError> {
            Err(unsupported_join_reconciliation_transport(
                "cluster join raft vote",
            ))
        }

        fn probe_voter(&mut self, _endpoint: &ClusterEndpointRecord, _timeout: Duration) -> bool {
            false
        }

        fn fetch_cluster_kv_snapshot(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            timeout: Duration,
        ) -> Result<KvSnapshot, network::NetworkError> {
            daemon_fetch_cluster_kv_snapshot_from_endpoint(endpoint, timeout).map_err(|source| {
                network::NetworkError::Io {
                    operation: "cluster join snapshot fetch".to_string(),
                    detail: source.to_string(),
                }
            })
        }

        fn send_cluster_membership_rpc(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            request: &network::ObjectRpcRequest,
            timeout: Duration,
        ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
            send_join_rpc(endpoint, request, timeout).map_err(|source| network::NetworkError::Io {
                operation: "cluster join membership rpc".to_string(),
                detail: source.to_string(),
            })
        }

        fn send_object_rpc_to_node(
            &mut self,
            _state: &ClusterBootstrapState,
            _target_node_id: &NodeId,
            _request: &network::ObjectRpcRequest,
            _timeout: Duration,
        ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
            Err(unsupported_join_reconciliation_transport(
                "cluster join node rpc",
            ))
        }
    }

    let mut host = network::ClusterControlPlaneDriver::new(CliClusterJoinReconciliationTransport);
    let reconciliation = network::reconcile_cluster_join_with_introducers_with_host(
        state,
        invite,
        DAEMON_JOIN_TIMEOUT,
        IROH_DAEMON_JOIN_TIMEOUT,
        paths,
        &mut host,
    )
    .map_err(CliError::from)?;
    if let Some(snapshot) = reconciliation.kv_snapshot
        && let Some(paths) = paths
    {
        write_cluster_kv_snapshot(paths, &snapshot)?;
    }
    if let Some(endpoint) = reconciliation.accepted_endpoint {
        println!("introducer accepted join: {}", endpoint.address);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn send_cluster_join_registration_over_iroh(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    send_cluster_join_registration_to_endpoint(endpoint, request, IROH_DAEMON_JOIN_TIMEOUT)
}

#[cfg(test)]
fn send_cluster_join_registration_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    network::send_cluster_join_registration_over_membership_rpc(
        endpoint,
        request,
        timeout,
        |endpoint, request, timeout| {
            send_join_rpc(endpoint, request, timeout).map_err(|source| network::NetworkError::Io {
                operation: "cluster join membership rpc".to_string(),
                detail: source.to_string(),
            })
        },
    )
    .map_err(Into::into)
}

fn send_join_rpc(
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    match endpoint.transport {
        ClusterTransportKind::Iroh => send_join_endpoint_rpc(endpoint, request, timeout),
        // Join RPCs must carry an idle deadline, not just a connect timeout:
        // an introducer that accepts the stream and never answers otherwise
        // hangs the join forever, and the caller's bounded retry loop (the
        // only recovery during cold formation) never reaches a second
        // attempt. The idle budget is generous because a healthy introducer
        // may legitimately relay the registration to a voter across bridges.
        _ => crate::daemon_send_daemon_rpc_with_timeouts(
            &endpoint.address,
            request,
            sinomo_runtime::network::DaemonRpcTimeouts {
                connect_timeout: Some(timeout),
                idle_timeout: Some(CLUSTER_JOIN_RPC_IDLE_TIMEOUT),
            },
        )
        .map_err(Into::into),
    }
}

fn send_join_endpoint_rpc(
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    let timeouts = network::DaemonRpcTimeouts::connect_and_idle(timeout);
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            let runtime = handle.clone();
            tokio::task::block_in_place(|| {
                runtime.block_on(send_join_endpoint_rpc_async(endpoint, request, timeouts))
            })
            .map_err(CliError::from)
        } else {
            Err(CliError::Network {
                address: "cluster join membership rpc".to_string(),
                detail: "cluster join endpoint RPC sync client cannot run inside a current-thread Tokio runtime".to_string(),
            })
        };
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| CliError::Network {
            address: "cluster join membership rpc".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime
        .block_on(send_join_endpoint_rpc_async(endpoint, request, timeouts))
        .map_err(CliError::from);
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

async fn send_join_endpoint_rpc_async(
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeouts: network::DaemonRpcTimeouts,
) -> Result<network::ObjectRpcResponse, network::NetworkError> {
    let mut host = CliSignedEndpointRpcHost::new(None, "cluster join endpoint rpc signing");
    let mut client = network::ClusterObjectRpcClient::endpoint_only(&mut host);
    client
        .call_endpoint(
            endpoint,
            request,
            network::ClusterObjectRpcOptions::new(timeouts),
        )
        .await
}

/// Idle deadline for join challenge/registration streams; covers a
/// bridge-relayed registration round-trip while still bounding a silent peer.
const CLUSTER_JOIN_RPC_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn parse_cluster_join_invite_argument(raw: &str) -> Result<ClusterJoinInvite, CliError> {
    let raw = raw.trim();
    let invite = if let Some(path) = raw.strip_prefix('@') {
        fs::read_to_string(path).map_err(|source| CliError::Io {
            path: path.to_string(),
            detail: source.to_string(),
        })?
    } else if Path::new(raw).exists() {
        fs::read_to_string(raw).map_err(|source| CliError::Io {
            path: raw.to_string(),
            detail: source.to_string(),
        })?
    } else {
        raw.to_string()
    };
    decode_cluster_join_invite(extract_invite_from_join_command(invite.trim())).map_err(Into::into)
}

pub(crate) fn cluster_status() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let _ = refresh_cluster_kv_from_local_daemon(&paths);
    match read_cluster_state(&paths) {
        Ok(state) => {
            println!("cluster: {}", state.cluster_id);
            println!("name: {}", state.cluster_name);
            println!("local node: {}", state.local_node_id);
            println!("status: {}", cluster_status_summary_label(&state));
            println!("term: {}", state.term);
            println!("kv revision: {}", state.kv_revision);
            println!("kv snapshot: {}", paths.kv_snapshot.display());
            println!("members: {}", state.members.len());
            println!("state: {}", paths.state.display());
            Ok(())
        }
        Err(CliError::ClusterOrphan) => {
            println!("cluster: not initialized");
            println!("state: not initialized");
            println!("next: run `sinomo cluster init` or `sinomo cluster join <invite>`");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn cluster_health() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    if let Some(status) = default_main_daemon_status()?
        && let Ok(rows) =
            cli_fetch_cluster_health_rows(&status.address, CLUSTER_HEALTH_COMMAND_TIMEOUT)
    {
        print_cluster_health_rows(rows);
        return Ok(());
    }
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = read_cluster_state(&paths)?;
    let rows = cluster_health_rows_for_cli(&paths, &state)?;
    print_cluster_health_rows(rows);
    Ok(())
}

fn print_cluster_health_rows(rows: Vec<ClusterHealthRow>) {
    if rows.is_empty() {
        println!(
            "node\tprocess\trole\tmember\thealth\tversion\tcommit\tplatform\tpid\taddress\tlast-seen\trollout"
        );
        return;
    }
    println!(
        "node\tprocess\trole\tmember\thealth\tversion\tcommit\tplatform\tpid\taddress\tlast-seen\trollout"
    );
    for row in rows {
        let platform = match (&row.status.target_os, &row.status.target_arch) {
            (Some(os), Some(arch)) => format!("{os}/{arch}"),
            _ => "-".to_string(),
        };
        let pid = row
            .status
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string());
        let address = row
            .status
            .address
            .clone()
            .unwrap_or_else(|| "-".to_string());
        let rollout = row
            .status
            .rollout
            .as_ref()
            .map(format_rollout_status)
            .unwrap_or_else(|| "-".to_string());
        let commit = row.status.commit.clone().unwrap_or_else(|| "-".to_string());
        println!(
            "{}\t{}\t{}\t{} {}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.member.node_id,
            row.status.process,
            format_node_role(&row.member.node_role),
            format_member_role(&row.member.role),
            format_member_state(&row.member.state),
            format_node_availability(&row.availability),
            row.status.daemon_version,
            commit,
            platform,
            pid,
            address,
            row.status.observed_at,
            rollout
        );
    }
}

pub(crate) fn format_node_availability(availability: &ClusterNodeAvailability) -> &'static str {
    match availability {
        ClusterNodeAvailability::Online => "online",
        ClusterNodeAvailability::Offline => "offline",
        ClusterNodeAvailability::Unknown => "unknown",
    }
}

pub(crate) fn format_rollout_status(status: &ClusterNodeRolloutStatus) -> String {
    let mut label = match status.mode.as_deref() {
        Some(mode) if !mode.is_empty() => format!("{} ({mode})", status.status),
        _ => status.status.clone(),
    };
    if status.progress_total.is_some_and(|total| total > 0) {
        let phase = status
            .phase
            .as_deref()
            .filter(|phase| !phase.is_empty())
            .unwrap_or(status.status.as_str());
        label.push_str(&format!(" {} {phase}", rollout_progress_bar(Some(status))));
    }
    label
}

pub(crate) fn cluster_doctor() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = read_cluster_state(&paths)?;
    let rows = cluster_health_rows_for_cli(&paths, &state)?;
    let graph = network::read_cluster_connectivity_graph_from_kv(&paths).unwrap_or_default();
    let mut findings = cluster_doctor_findings(&state, &graph, &rows);
    if let Some(data_dir) = paths.root.parent() {
        findings.extend(data_dir_storage_contract_findings(data_dir)?);
    }
    println!(
        "cluster doctor: {}",
        if findings.is_empty() {
            "ok"
        } else {
            "warnings"
        }
    );
    println!("cluster: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    if findings.is_empty() {
        println!("no findings");
    } else {
        println!("severity\tcode\tdetail");
        for finding in findings {
            println!("{}\t{}\t{}", finding.severity, finding.code, finding.detail);
        }
    }
    Ok(())
}

fn cluster_health_rows_for_cli(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterHealthRow>, CliError> {
    if let Some(status) = default_main_daemon_status()?
        && let Ok(rows) =
            cli_fetch_cluster_health_rows(&status.address, CLUSTER_HEALTH_COMMAND_TIMEOUT)
    {
        return Ok(rows);
    }
    network::cluster_health_rows_with_probe(
        paths,
        state,
        &CliClusterStatusProbe { paths },
        true,
        OffsetDateTime::now_utc(),
    )
    .map_err(Into::into)
}

pub(crate) fn cluster_peers() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let members = cluster_peer_members(&paths)?;
    for member in members {
        println!(
            "{}\t{}\t{}\t{}",
            member.node_id,
            member.display_name,
            format_node_role(&member.node_role),
            format_member_state(&member.state)
        );
    }
    Ok(())
}

pub(crate) struct CliClusterMemberMutationHost;

impl network::ClusterMemberMutationHost for CliClusterMemberMutationHost {
    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, network::NetworkError> {
        cli_current_cluster_kv_store(paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, network::NetworkError> {
        cli_commit_cluster_kv_txn_with_quorum(paths, state, txn)
    }
}

pub(crate) fn cluster_role(options: ClusterRoleOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = network::read_cluster_state(&paths)?;
    let update = network::update_cluster_node_role_with_host(
        &paths,
        state,
        options.node_id.clone(),
        options.role.clone(),
        OffsetDateTime::now_utc(),
        &mut CliClusterMemberMutationHost,
    )?;
    if !update.changed {
        println!(
            "cluster member {} already has role {}",
            update.node_id,
            format_node_role(&update.role)
        );
        return Ok(());
    }

    println!(
        "cluster member {} role changed: {} -> {}",
        update.node_id,
        format_node_role(&update.previous_role),
        format_node_role(&update.role)
    );
    Ok(())
}

pub(crate) fn cluster_raft_role(options: ClusterRaftRoleOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = network::read_cluster_state(&paths)?;
    let update = network::update_cluster_raft_role_with_host(
        &paths,
        state,
        options.node_id.clone(),
        options.role.clone(),
        OffsetDateTime::now_utc(),
        &mut CliClusterMemberMutationHost,
    )?;
    if !update.changed {
        println!(
            "cluster member {} already has raft role {}",
            update.node_id,
            format_member_role(&update.role)
        );
        return Ok(());
    }

    println!(
        "cluster member {} raft role changed: {} {} -> {} {}",
        update.node_id,
        format_member_role(&update.previous_role),
        format_member_state(&update.previous_state),
        format_member_role(&update.role),
        format_member_state(&update.state)
    );
    Ok(())
}

pub(crate) fn cluster_repair_members(options: ClusterRepairMembersOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let mut state = read_cluster_state(&paths)?;
    let peer_status = daemon_fetch_daemon_status_from_address(&options.from, DAEMON_JOIN_TIMEOUT)?;
    let peer_snapshot = daemon_fetch_cluster_kv_snapshot(&options.from, DAEMON_JOIN_TIMEOUT)?;
    let peer_store = InProcessKvStore::restore(peer_snapshot);
    let mut imported = Vec::new();
    let mut endpoint_updates = Vec::new();
    for entry in peer_store.range_prefix("/cluster/members/") {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.cluster_id != state.cluster_id {
            continue;
        }
        for endpoint_entry in
            peer_store.range_prefix(&network::cluster_endpoint_prefix(&member.node_id))
        {
            let endpoint: ClusterEndpointRecord = endpoint_entry.decode()?;
            if !member
                .endpoints
                .iter()
                .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
            {
                member.endpoints.push(endpoint);
            }
        }
        if let Some(existing) = state
            .members
            .iter_mut()
            .find(|candidate| candidate.node_id == member.node_id)
        {
            let mut merged_endpoints = 0usize;
            for endpoint in member.endpoints {
                if !existing
                    .endpoints
                    .iter()
                    .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
                {
                    existing.endpoints.push(endpoint);
                    merged_endpoints += 1;
                }
            }
            if merged_endpoints > 0 {
                existing.updated_at = OffsetDateTime::now_utc();
                endpoint_updates.push((existing.node_id.clone(), merged_endpoints));
            }
            continue;
        }
        if matches!(member.role, ClusterMemberRole::Voter)
            || matches!(member.state, ClusterMemberState::Voter)
        {
            member.role = ClusterMemberRole::Learner;
            member.state = ClusterMemberState::Learner;
        }
        member.updated_at = OffsetDateTime::now_utc();
        imported.push(member);
    }
    if let Some(cluster) = peer_status.cluster.as_ref()
        && cluster.cluster_id == state.cluster_id
        && cluster.process == sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE
    {
        let observed_endpoint =
            network::daemon_lan_endpoint(&cluster.node_id, options.from.clone(), 0);
        if let Some(existing) = state
            .members
            .iter_mut()
            .find(|candidate| candidate.node_id == observed_endpoint.node_id)
            && !existing
                .endpoints
                .iter()
                .any(|candidate| candidate.endpoint_id == observed_endpoint.endpoint_id)
        {
            existing.endpoints.push(observed_endpoint);
            existing.updated_at = OffsetDateTime::now_utc();
            endpoint_updates.push((existing.node_id.clone(), 1));
        }
    }
    let imported_changed = !imported.is_empty();
    if !imported_changed {
        if endpoint_updates.is_empty() {
            println!(
                "cluster members already include peer records from {}",
                options.from
            );
        }
    } else {
        for member in &imported {
            println!(
                "importing cluster member {} as {} {}",
                member.node_id,
                format_member_role(&member.role),
                format_member_state(&member.state)
            );
        }
        state.members.extend(imported);
    }
    for (node_id, count) in &endpoint_updates {
        println!("merged {count} cluster endpoint(s) for existing member {node_id}");
    }
    if !endpoint_updates.is_empty() || imported_changed {
        sync_cluster_state_to_kv(&paths, &state)?;
    }

    if let Some(node_id) = options.promote {
        let state = network::read_cluster_state(&paths)?;
        let update = network::update_cluster_raft_role_with_host(
            &paths,
            state,
            node_id,
            ClusterMemberRole::Voter,
            OffsetDateTime::now_utc(),
            &mut CliClusterMemberMutationHost,
        )?;
        if update.changed {
            println!(
                "cluster member {} raft role changed: {} {} -> {} {}",
                update.node_id,
                format_member_role(&update.previous_role),
                format_member_state(&update.previous_state),
                format_member_role(&update.role),
                format_member_state(&update.state)
            );
        } else {
            println!(
                "cluster member {} already has raft role {}",
                update.node_id,
                format_member_role(&update.role)
            );
        }
    }
    Ok(())
}

pub(crate) fn cluster_remove(options: ClusterRemoveOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = network::read_cluster_state(&paths)?;
    let removal = network::remove_cluster_members_with_host(
        &paths,
        state,
        options.node_ids.clone(),
        &mut CliClusterMemberMutationHost,
    )?;
    for member in removal.removed {
        println!(
            "cluster member {} removed: {} {}",
            member.node_id,
            format_node_role(&member.node_role),
            format_member_state(&member.state)
        );
    }
    Ok(())
}

pub(crate) fn cluster_cleanup_legacy_shadow_members() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = network::read_cluster_state(&paths)?;
    let removal = network::cleanup_legacy_shadow_daemon_members_with_host(
        &paths,
        state,
        &mut CliClusterMemberMutationHost,
    )?;
    if removal.removed.is_empty() {
        println!("no legacy shadow-only cluster members found");
        return Ok(());
    }
    for member in removal.removed {
        println!(
            "legacy shadow cluster member {} removed: {} {}",
            member.node_id,
            format_node_role(&member.node_role),
            format_member_state(&member.state)
        );
    }
    Ok(())
}

pub(crate) fn cluster_peer_members(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, CliError> {
    if let Err(error) = refresh_cluster_kv_from_local_daemon(paths) {
        eprintln!("warning: cluster peer refresh skipped: {error}");
    }
    let mut state = read_cluster_state(paths)?;
    if network::cluster_state_needs_join_registration(&state) {
        let (main_options, shadow_options) = local_daemon_cluster_options()?;
        ensure_local_daemon_cluster_members(
            &mut state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        );
        network::write_cluster_state(paths, &state)?;
    }
    Ok(state.members)
}

pub(crate) fn cluster_leave() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let report = cluster_leave_with_paths(&paths)?;
    match &report.previous_cluster_id {
        Some(cluster_id) => println!("cluster left: {cluster_id}"),
        None => println!("cluster left: no previous local cluster state"),
    }
    println!("single-node cluster: {}", report.new_cluster_id);
    println!("local node: {}", report.local_node_id);
    if let Some(root) = &report.quarantine_root {
        println!("source quarantine: {}", root.display());
    }
    println!("state: {}", report.state_path.display());
    Ok(())
}

pub(crate) fn cluster_leave_with_paths(
    paths: &ClusterPaths,
) -> Result<ClusterLeaveReport, CliError> {
    paths.ensure()?;
    let existing = match network::read_cluster_state_without_kv(paths) {
        Ok(state) => Some(state),
        Err(network::NetworkError::ClusterOrphan) => None,
        Err(error) => return Err(error.into()),
    };
    let previous_cluster_id = existing.as_ref().map(|state| state.cluster_id.clone());
    let local_node_id = existing
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(NodeId::generate);
    let name = existing
        .as_ref()
        .map(|state| state.local_node_display_name.trim())
        .filter(|name| !name.is_empty())
        .or_else(|| {
            existing
                .as_ref()
                .map(|state| state.cluster_name.trim())
                .filter(|name| !name.is_empty())
        })
        .unwrap_or("sinomo-local")
        .to_string();

    let quarantine = if existing.is_some() || cluster_storage_has_live_artifacts(paths) {
        Some(quarantine_cluster_storage_for_leave(
            paths,
            &Id::generate(),
        )?)
    } else {
        None
    };

    match initialize_single_node_cluster_with_paths(
        ClusterInitOptions {
            cluster_id: Some(ClusterId::generate()),
            node_id: Some(local_node_id),
            name,
        },
        paths,
    ) {
        Ok(state) => Ok(ClusterLeaveReport {
            previous_cluster_id,
            new_cluster_id: state.cluster_id,
            local_node_id: state.local_node_id,
            quarantine_root: quarantine.map(|quarantine| quarantine.root),
            state_path: paths.state.clone(),
        }),
        Err(error) => {
            if let Some(quarantine) = quarantine {
                rollback_cluster_storage_quarantine(&quarantine)?;
            }
            Err(error)
        }
    }
}

pub(crate) fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, CliError> {
    let mut state = network::read_cluster_state(paths)?;
    prepare_local_cluster_state_for_cli(paths, &mut state)?;
    Ok(state)
}

fn prepare_local_cluster_state_for_cli(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), CliError> {
    refresh_local_daemon_membership_from_disk_for_cli(
        paths,
        state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        OffsetDateTime::now_utc(),
    )?;
    Ok(())
}

pub(crate) fn refresh_cluster_kv_from_local_daemon(paths: &ClusterPaths) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return refresh_cluster_kv_from_reachable_voters(paths);
    };
    match daemon_fetch_cluster_kv_snapshot(&status.address, DAEMON_PROBE_TIMEOUT) {
        Ok(snapshot) => write_imported_cluster_kv_snapshot(paths, &snapshot),
        Err(_) => refresh_cluster_kv_from_reachable_voters(paths),
    }
}

fn refresh_cluster_kv_from_reachable_voters(paths: &ClusterPaths) -> Result<(), CliError> {
    struct CliClusterKvSnapshotRefreshHost;

    impl network::ClusterKvSnapshotRefreshHost for CliClusterKvSnapshotRefreshHost {
        fn known_raft_leader_id(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
        ) -> Result<Option<NodeId>, network::NetworkError> {
            cluster_raft_known_leader_id(paths, state).or(Ok(None))
        }

        fn fetch_cluster_kv_snapshot(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            timeout: Duration,
        ) -> Result<KvSnapshot, network::NetworkError> {
            daemon_fetch_cluster_kv_snapshot_from_endpoint(endpoint, timeout).map_err(|source| {
                network::NetworkError::Io {
                    operation: "cluster kv snapshot refresh".to_string(),
                    detail: source.to_string(),
                }
            })
        }

        fn restore_cluster_raft_from_snapshot_if_newer(
            &mut self,
            paths: &ClusterPaths,
            local_node_id: &NodeId,
            snapshot: KvSnapshot,
        ) -> Result<bool, network::NetworkError> {
            let _raft_guard =
                sinomo_runtime::kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())
                    .map_err(network::NetworkError::from)?;
            paths
                .kv_paths()
                .local_cache()
                .restore_raft_from_snapshot_if_newer(local_node_id.clone(), snapshot)
                .map_err(network::NetworkError::from)
        }
    }

    let Ok(state) = network::read_cluster_state(paths) else {
        return Ok(());
    };
    match network::refresh_current_cluster_kv_store_with_host(
        paths,
        &state,
        network::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_snapshot_refresh_options(),
        &mut CliClusterKvSnapshotRefreshHost,
    ) {
        Ok(_) | Err(network::NetworkError::ClusterReadOnly { .. }) => Ok(()),
        Err(network::NetworkError::Io { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_store(
    paths: &ClusterPaths,
) -> Result<sinomo_runtime::kv::InProcessKvStore, CliError> {
    let state = network::read_cluster_state_without_kv(paths)?;
    sinomo_runtime::kv::read_cluster_kv_store_for_node(&paths.kv_paths(), state.local_node_id)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    match sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
        .map_err(network::NetworkError::from)
    {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => {
            eprintln!("warning: ignoring unreadable cluster kv snapshot: {error}");
            read_cluster_kv_store(paths)
                .map(|store| store.snapshot())
                .or_else(|_| Ok(sinomo_runtime::kv::InProcessKvStore::new().snapshot()))
        }
    }
}

pub(crate) fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), CliError> {
    sinomo_runtime::kv::write_cluster_kv_snapshot_if_changed(&paths.kv_paths(), snapshot)
        .map(|_| ())
        .map_err(network::NetworkError::from)
        .map_err(Into::into)
}

pub(crate) fn write_imported_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), CliError> {
    sinomo_runtime::kv::write_imported_cluster_kv_snapshot_if_changed(
        &paths.kv_paths(),
        snapshot,
        sinomo_runtime::kv::cluster_kv_key_can_defer_materialized_snapshot,
    )
    .map(|_| ())
    .map_err(Into::into)
}

pub(crate) fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, CliError> {
    network::sync_cluster_state_to_kv_with_host(paths, state, &mut CliClusterMemberMutationHost)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn expire_cluster_network_records_in_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
) -> Result<Option<KvSnapshot>, CliError> {
    struct CliClusterNetworkRecordExpiryHost;

    impl network::ClusterNetworkRecordExpiryHost for CliClusterNetworkRecordExpiryHost {
        type Error = CliError;

        fn commit_cluster_kv_txn_with_quorum(
            &mut self,
            paths: &ClusterPaths,
            state: &mut ClusterBootstrapState,
            txn: KvTxn,
        ) -> Result<KvSnapshot, Self::Error> {
            append_cluster_raft_command(
                paths,
                &state.local_node_id,
                sinomo_runtime::kv::KvCommand::Txn(txn),
            )
            .map_err(Into::into)
        }
    }

    network::expire_cluster_network_records_with_host(
        paths,
        state,
        now,
        &mut CliClusterNetworkRecordExpiryHost,
    )
}

struct CliClusterInvitePublishHost;

impl network::ClusterInvitePublishHost for CliClusterInvitePublishHost {
    type Error = CliError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        append_cluster_raft_command(
            paths,
            &state.local_node_id,
            sinomo_runtime::kv::KvCommand::Txn(txn),
        )
        .map_err(Into::into)
    }
}

pub(crate) fn local_daemon_cluster_options()
-> Result<(DaemonRunOptions, DaemonRunOptions), CliError> {
    let main = match default_main_daemon_status()? {
        Some(status) => daemon_options_from_status(&status),
        None => DaemonRunOptions::default(),
    };
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let data_root = data_dir.root();
    let shadow = current_shadow_daemon_options(&data_root, DAEMON_PROBE_TIMEOUT);
    Ok((main, shadow))
}

pub(crate) fn register_daemon_cluster_node(
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, CliError> {
    let paths = cluster_paths_from_defaults()?;
    let mut state = read_cluster_state(&paths)?;
    ensure_local_daemon_cluster_members(
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    );
    network::write_cluster_state(&paths, &state)?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| CliError::Parse {
        detail: "daemon cluster member could not be registered".to_string(),
    })
}

pub(crate) fn daemon_cluster_status_from_local_state(
    paths: &ClusterPaths,
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, CliError> {
    let mut state = network::read_cluster_state_without_kv(paths)?;
    refresh_local_daemon_membership_from_disk_for_cli(
        paths,
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    )?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| CliError::Parse {
        detail: "daemon cluster member could not be resolved from local state".to_string(),
    })
}

pub(crate) fn daemon_cluster_registration_if_initialized(
    registration: DaemonClusterRegistration,
) -> Result<Option<DaemonClusterRegistration>, CliError> {
    let paths = cluster_paths_from_defaults()?;
    Ok(paths.state.exists().then_some(registration))
}

#[cfg(test)]
pub(crate) fn read_cluster_rollout_status(
    paths: &SelfRolloutPaths,
) -> Result<Option<ClusterNodeRolloutStatus>, CliError> {
    read_rollout_status(paths).map_err(Into::into)
}

pub(crate) fn format_member_role(role: &ClusterMemberRole) -> &'static str {
    match role {
        ClusterMemberRole::Voter => "voter",
        ClusterMemberRole::Learner => "learner",
        ClusterMemberRole::NonVoting => "non-voting",
    }
}

pub(crate) fn format_node_role(role: &ClusterNodeRole) -> &'static str {
    match role {
        ClusterNodeRole::Workstation => "workstation",
        ClusterNodeRole::Mobile => "mobile",
        ClusterNodeRole::Server => "server",
        ClusterNodeRole::Sensor => "sensor",
        ClusterNodeRole::Test => "test",
    }
}

pub(crate) fn format_member_state(state: &ClusterMemberState) -> &'static str {
    match state {
        ClusterMemberState::Pending => "pending",
        ClusterMemberState::Active => "active",
        ClusterMemberState::Learner => "learner",
        ClusterMemberState::Voter => "voter",
        ClusterMemberState::NonVoting => "non-voting",
        ClusterMemberState::Suspect => "suspect",
        ClusterMemberState::Offline => "offline",
        ClusterMemberState::Removed => "removed",
    }
}
