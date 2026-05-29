use super::*;

#[cfg(test)]
pub(crate) use network::advertised_peer_route_targets;
#[cfg(test)]
pub(crate) use network::build_cluster_raft_append_request_from_next_index;
#[cfg(test)]
pub(crate) use network::cluster_connectivity_sync_ops;
#[cfg(test)]
pub(crate) use network::cluster_raft_known_leader_id;
#[cfg(test)]
pub(crate) use network::cluster_state_sync_txn;
#[cfg(test)]
pub(crate) use network::cluster_sync_targets;
#[cfg(test)]
pub(crate) use network::invite_channels_for_join_registration;
#[cfg(test)]
pub(crate) use network::invite_communication_channels;
#[cfg(test)]
pub(crate) use network::invite_introducer_endpoint;
#[cfg(test)]
pub(crate) use network::read_cluster_kv_store;
#[cfg(test)]
pub(crate) use network::read_cluster_members_from_kv;
#[cfg(test)]
pub(crate) use network::read_cluster_node_statuses_from_kv;
#[cfg(test)]
pub(crate) use network::sign_cluster_join_invite;
#[cfg(test)]
pub(crate) use network::{apply_cluster_node_role, apply_cluster_raft_role};
pub(crate) use network::{
    cluster_join_command, cluster_raft_local_is_voter, cluster_raft_quorum_size,
    cluster_state_needs_join_registration, cluster_status_summary_label,
    decode_cluster_join_invite, encode_cluster_join_invite, extract_invite_from_join_command,
    invite_introducer_endpoint_from_state, is_remote_usable_invite_endpoint,
    verify_cluster_join_invite_signature,
};
#[cfg(test)]
pub(crate) use network::{compact_default_invite_introducers, push_unique_introducer};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClusterCliCommand {
    Help,
    Init(ClusterInitOptions),
    Invite(ClusterInviteOptions),
    Join(ClusterJoinOptions),
    Role(ClusterRoleOptions),
    RaftRole(ClusterRaftRoleOptions),
    RepairMembers(ClusterRepairMembersOptions),
    Remove(ClusterRemoveOptions),
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
        ClusterCliCommand::Role(options) => cluster_role(options),
        ClusterCliCommand::RaftRole(options) => cluster_raft_role(options),
        ClusterCliCommand::RepairMembers(options) => cluster_repair_members(options),
        ClusterCliCommand::Remove(options) => cluster_remove(options),
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

pub(crate) fn require_cluster_bootstrap_state() -> Result<ClusterBootstrapState, CliError> {
    let paths = cluster_paths_from_defaults()?;
    read_cluster_state(&paths)
}

pub(crate) fn require_cluster_runtime_quorum() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let state = read_cluster_state(&paths).map_err(|error| match error {
        CliError::ClusterOrphan => CliError::ClusterReadOnly {
            detail: "local node has no cluster state; cluster-dependent operations are paused"
                .to_string(),
        },
        other => other,
    })?;
    if cluster_has_local_quorum(&paths, &state) {
        Ok(())
    } else {
        Err(CliError::ClusterReadOnly {
            detail: format!(
                "quorum unavailable; cluster-dependent operations are paused ({} reachable voters, {} required)",
                cluster_reachable_voter_count(&paths, &state),
                cluster_raft_quorum_size(&state)
            ),
        })
    }
}

pub(crate) fn cluster_init(options: ClusterInitOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    cluster_init_with_paths(options, &paths)
}

pub(crate) fn cluster_init_with_paths(
    options: ClusterInitOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    let state = daemon_initialize_local_cluster(
        paths,
        options.cluster_id,
        options.node_id,
        options.name,
        OffsetDateTime::now_utc(),
    )?;
    println!("cluster initialized: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("state: {}", paths.state.display());
    Ok(())
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

pub(crate) fn cluster_invite_with_paths(
    options: ClusterInviteOptions,
    paths: &ClusterPaths,
) -> Result<RenderedClusterInvite, CliError> {
    paths.ensure()?;
    let state = read_cluster_state(paths)?;
    if !cluster_raft_local_is_voter(&state) {
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
        persisted_key: network::cluster_invitation_key(&invite.invite_id),
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
    match publish_cluster_invitation_to_quorum(paths, state, invite).map_err(CliError::from) {
        Ok(()) => Ok(()),
        Err(CliError::ClusterReadOnly { .. }) if !cluster_raft_local_is_voter(state) => {
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
    let targets = cluster_invite_publish_targets(paths, state);
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

pub(crate) fn cluster_invite_publish_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut seen_nodes = std::collections::BTreeSet::new();
    let mut targets = Vec::new();
    for endpoint in network::cluster_voter_command_targets(paths, state) {
        if !seen_nodes.insert(endpoint.node_id.clone()) {
            continue;
        }
        targets.push(endpoint);
    }
    targets
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
    daemon_build_cluster_join_invite(state, data_dir, introducers, options.ttl_seconds, now)
        .map_err(Into::into)
}

pub(crate) fn default_invite_introducers(
    state: &ClusterBootstrapState,
    paths: &ClusterPaths,
) -> Vec<ClusterEndpointRecord> {
    daemon_default_cluster_invite_introducers(paths, state, DAEMON_PROBE_TIMEOUT)
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
        return Err(CliError::Parse {
            detail: format!(
                "cluster is already initialized at {}; leave before joining another cluster",
                paths.state.display()
            ),
        });
    }

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
    let mut state = daemon_prepare_pending_cluster_join_state(
        paths,
        cluster_id,
        invite
            .cluster_name
            .clone()
            .unwrap_or_else(|| options.name.clone()),
        local_node_id.clone(),
        options.name,
        &main_options,
        &shadow_options,
        timestamp,
    )?;
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    daemon_write_local_cluster_state(paths, &state)?;
    println!("cluster join recorded: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
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
    state = daemon_prepare_cluster_join_retry_state(
        paths,
        state,
        &main_options,
        &shadow_options,
        timestamp,
    )?;
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    state.updated_at = timestamp;
    daemon_write_local_cluster_state(paths, &state)?;
    println!("cluster join reconciled: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
}

pub(crate) fn reconcile_cluster_join_with_introducers(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    paths: Option<&ClusterPaths>,
) -> Result<(), CliError> {
    let reconciliation = daemon_reconcile_cluster_join_with_introducers(
        state,
        invite,
        DAEMON_JOIN_TIMEOUT,
        IROH_DAEMON_JOIN_TIMEOUT,
    )?;
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
    daemon_send_cluster_join_registration_to_endpoint(endpoint, request, IROH_DAEMON_JOIN_TIMEOUT)
        .map_err(Into::into)
}

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
            println!("cluster: orphan");
            println!("state: not initialized");
            println!("next: run `sinomo cluster init` or `sinomo cluster join <invite>`");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn cluster_health() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = read_cluster_state(&paths)?;
    let rows = cluster_health_rows(&paths, &state)?;
    if rows.is_empty() {
        println!(
            "node\tprocess\trole\tmember\thealth\tversion\tcommit\tplatform\tpid\taddress\tlast-seen\trollout"
        );
        return Ok(());
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
        let commit = row
            .status
            .commit
            .clone()
            .unwrap_or_else(|| "-".to_string());
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
    Ok(())
}

pub(crate) fn format_node_availability(availability: &ClusterNodeAvailability) -> &'static str {
    match availability {
        ClusterNodeAvailability::Online => "online",
        ClusterNodeAvailability::Offline => "offline",
        ClusterNodeAvailability::Unknown => "unknown",
    }
}

pub(crate) fn format_rollout_status(status: &ClusterNodeRolloutStatus) -> String {
    match status.mode.as_deref() {
        Some(mode) if !mode.is_empty() => format!("{} ({mode})", status.status),
        _ => status.status.clone(),
    }
}

pub(crate) fn cluster_doctor() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = read_cluster_state(&paths)?;
    let rows = cluster_health_rows(&paths, &state)?;
    let graph = daemon_read_cluster_connectivity_graph_from_kv(&paths).unwrap_or_default();
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

pub(crate) fn cluster_role(options: ClusterRoleOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let update = daemon_update_cluster_node_role(
        &paths,
        options.node_id.clone(),
        options.role.clone(),
        OffsetDateTime::now_utc(),
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
    let update = daemon_update_cluster_raft_role(
        &paths,
        options.node_id.clone(),
        options.role.clone(),
        OffsetDateTime::now_utc(),
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
        && cluster.process == "daemon-main"
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
        daemon_sync_cluster_state_to_kv(&paths, &state)?;
    }

    if let Some(node_id) = options.promote {
        let update = daemon_update_cluster_raft_role(
            &paths,
            node_id,
            ClusterMemberRole::Voter,
            OffsetDateTime::now_utc(),
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
    let removal =
        daemon_remove_cluster_members(&paths, options.node_ids.clone(), OffsetDateTime::now_utc())?;
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

pub(crate) fn cluster_peer_members(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, CliError> {
    refresh_cluster_kv_from_local_daemon(paths)?;
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    daemon_cluster_peer_members(
        paths,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
    )
    .map_err(Into::into)
}

pub(crate) fn cluster_leave() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    daemon_remove_local_cluster_state(&paths)?;
    println!("cluster state removed: {}", paths.state.display());
    Ok(())
}

pub(crate) fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, CliError> {
    daemon_read_local_cluster_state(paths).map_err(Into::into)
}

pub(crate) fn refresh_cluster_kv_from_local_daemon(paths: &ClusterPaths) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Ok(());
    };
    let Ok(snapshot) = daemon_fetch_cluster_kv_snapshot(&status.address, DAEMON_PROBE_TIMEOUT)
    else {
        return Ok(());
    };
    write_cluster_kv_snapshot(paths, &snapshot)
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    match daemon_read_cluster_kv_snapshot(paths) {
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
    daemon_write_cluster_kv_snapshot_if_changed(paths, snapshot)
        .map(|_| ())
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, CliError> {
    daemon_sync_cluster_state_to_kv(paths, state).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn expire_cluster_network_records_in_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
) -> Result<Option<KvSnapshot>, CliError> {
    sinomo_runtime::daemon::expire_cluster_network_records_in_kv(paths, state, now)
        .map_err(Into::into)
}

pub(crate) fn local_daemon_cluster_options()
-> Result<(DaemonRunOptions, DaemonRunOptions), CliError> {
    daemon_local_daemon_cluster_options(DAEMON_PROBE_TIMEOUT).map_err(Into::into)
}

pub(crate) fn register_daemon_cluster_node(
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, CliError> {
    let paths = cluster_paths_from_defaults()?;
    daemon_register_daemon_cluster_node(&paths, registration).map_err(Into::into)
}

pub(crate) fn daemon_cluster_registration_if_initialized(
    registration: DaemonClusterRegistration,
) -> Result<Option<DaemonClusterRegistration>, CliError> {
    let paths = cluster_paths_from_defaults()?;
    Ok(daemon_registration_if_initialized(&paths, registration))
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
