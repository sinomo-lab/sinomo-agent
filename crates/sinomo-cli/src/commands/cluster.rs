use super::*;

use cluster::{
    cluster_join_command, cluster_status_summary_label, decode_cluster_join_invite,
    encode_cluster_join_invite, extract_invite_from_join_command,
    invite_introducer_endpoint_from_state, is_remote_usable_invite_endpoint,
    verify_cluster_join_invite_signature,
};
pub(crate) use network::cluster_state_needs_join_registration;
use sinomo_runtime::cli_registry::{
    CliAutocompleteChoice, CliCommandMatches, CliCommandPolicy, CliCommandSpec, CliOptionSpec,
    CliParseError, CliPositionalSpec, CliValueSpec,
};
use sinomo_runtime::cluster;
#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;
use sinomo_runtime::node::cluster_member_is_legacy_shadow_daemon;

#[cfg(test)]
pub(crate) fn build_cluster_raft_append_request_from_next_index(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    next_index: u64,
) -> Result<sinomo_runtime::kv::ClusterRaftAppendRequest, network::NetworkError> {
    paths
        .kv_paths()
        .build_raft_append_request_from_next_index(state, next_index)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClusterCliCommand {
    Init(ClusterInitOptions),
    Invite(ClusterInviteOptions),
    Join(ClusterJoinOptions),
    Migrate(ClusterMigrateCommand),
    Rename(ClusterRenameOptions),
    RenameHostnames,
    Role(ClusterRoleOptions),
    StateAuthority(ClusterStateAuthorityOptions),
    RepairMembers(ClusterRepairMembersOptions),
    RecoverEndpoint { action: cluster::EndpointRecoveryAction, request_file: String },
    Remove(ClusterRemoveOptions),
    CleanupLegacyShadowMembers,
    Status,
    Health,
    Doctor,
    Connectivity,
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
            name: default_cluster_node_name("sinomo-local"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterJoinOptions {
    pub(crate) invite: String,
    pub(crate) cluster_id: Option<ClusterId>,
    pub(crate) node_id: Option<NodeId>,
    pub(crate) name: String,
    /// True when `--name` was passed explicitly (vs the hostname default);
    /// a daemon-mediated join then applies the name with a follow-up rename.
    pub(crate) name_explicit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRenameOptions {
    pub(crate) node_id: NodeId,
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
    pub(crate) channel_outputs: Vec<ClusterInviteChannelOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterInviteChannelOutput {
    pub(crate) introducer: String,
    pub(crate) output: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterRoleOptions {
    pub(crate) node_id: NodeId,
    pub(crate) role: ClusterNodeRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterStateAuthorityOptions {
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
            channel_outputs: Vec::new(),
        }
    }
}

pub(crate) fn cluster_cli_command_handler() -> DeclarativeCommandHandler<ClusterCliCommand> {
    DeclarativeCommandHandler::new(
        "cluster",
        CliCommandPolicy::default(),
        cluster_cli_spec,
        parse_cluster_matches,
        run_cluster_cli_command,
    )
}

fn cluster_cli_spec() -> CliCommandSpec {
    let option = |name: &'static str, help: &'static str, value: &'static str| {
        CliOptionSpec::value(name, help, CliValueSpec::new(value, help))
    };
    let positional = |name: &'static str, help: &'static str| {
        CliPositionalSpec::required(CliValueSpec::new(name, help))
    };
    let node_roles = CliValueSpec::new("role", "node role")
        .with_choices([
            CliAutocompleteChoice::new("workstation", "workstation node"),
            CliAutocompleteChoice::new("mobile", "mobile node"),
            CliAutocompleteChoice::new("server", "server node"),
            CliAutocompleteChoice::new("sensor", "sensor node"),
            CliAutocompleteChoice::new("test", "test node"),
        ])
        .restricted();
    let authority_roles = CliValueSpec::new("role", "state authority role").with_choices([
        CliAutocompleteChoice::new("authority", "state authority voter"),
        CliAutocompleteChoice::new("replica", "state replica learner"),
        CliAutocompleteChoice::new("observer", "non-authority observer"),
        CliAutocompleteChoice::new("voter", "state authority voter"),
        CliAutocompleteChoice::new("learner", "state replica learner"),
        CliAutocompleteChoice::new("non-voting", "non-authority observer"),
    ]);
    CliCommandSpec::new("cluster", "inspect and modify cluster membership")
        .help_on_empty()
        .with_children([
            CliCommandSpec::new("init", "bootstrap a new local cluster").with_options([
                option("--name", "local node name", "name"),
                option("--cluster-id", "explicit cluster id", "cluster-id"),
                option("--node-id", "explicit local node id", "node-id"),
            ]),
            CliCommandSpec::new("invite", "create a portable join invite").with_options([
                option(
                    "--introducer",
                    "advertised introducer endpoint",
                    "host:port",
                )
                .repeatable(),
                option("--ttl-seconds", "invite lifetime", "seconds"),
                option("--output", "invite output path", "path"),
                option(
                    "--channel-output",
                    "per-introducer invite output",
                    "introducer=path",
                )
                .repeatable(),
            ]),
            CliCommandSpec::new(
                "recover-endpoint",
                "recover one existing peer route without membership changes",
            )
            .with_positionals([
                positional("action", "apply|status|rollback"),
                positional("request", "immutable endpoint recovery JSON request file"),
            ]),
            CliCommandSpec::new("join", "join an existing cluster")
                .with_options([
                    option("--name", "local node name", "name"),
                    option("--cluster-id", "expected cluster id", "cluster-id"),
                    option("--node-id", "explicit local node id", "node-id"),
                ])
                .with_positionals([positional("invite", "join URI or invite file")]),
            CliCommandSpec::new("migrate", "plan cluster identity migration")
                .with_aliases(["migration"])
                .with_children([CliCommandSpec::new(
                    "plan",
                    "plan migration into another cluster",
                )
                .with_options([option(
                    "--mode",
                    "migration mode",
                    "single-node|cluster-merge",
                )])
                .with_positionals([positional("invite", "join URI or invite file")])]),
            CliCommandSpec::new("rename", "rename one cluster node")
                .with_aliases(["set-name"])
                .with_positionals([
                    positional("node-id", "cluster node id"),
                    positional("name", "new node name"),
                ]),
            CliCommandSpec::new("rename-hostnames", "rename members from live hostnames")
                .with_aliases(["rename-to-hostnames", "rename-nodes-to-hostnames"]),
            CliCommandSpec::new("role", "set a documented node role")
                .with_aliases(["set-role"])
                .with_positionals([
                    positional("node-id", "cluster node id"),
                    CliPositionalSpec::required(node_roles),
                ]),
            CliCommandSpec::new("state-authority", "set a state authority role")
                .with_aliases([
                    "authority-role",
                    "set-state-authority",
                    "set-authority-role",
                    "raft-role",
                    "set-raft-role",
                ])
                .with_positionals([
                    positional("node-id", "cluster node id"),
                    CliPositionalSpec::required(authority_roles.clone()),
                ]),
            CliCommandSpec::new("promote", "promote a node to state authority").with_positionals([
                positional("node-id", "cluster node id"),
                CliPositionalSpec::optional(authority_roles),
            ]),
            CliCommandSpec::new("repair-members", "import member records from a peer")
                .with_aliases(["repair"])
                .with_options([
                    option("--from", "source peer endpoint", "host:port|iroh://..."),
                    option("--promote", "promote imported node", "node-id"),
                ])
                .with_positionals([CliPositionalSpec::optional(CliValueSpec::new(
                    "endpoint",
                    "source peer endpoint",
                ))]),
            CliCommandSpec::new("remove", "remove cluster members")
                .with_aliases(["rm"])
                .with_positionals([positional("node-id", "cluster node id").repeatable()]),
            CliCommandSpec::new(
                "cleanup-legacy-shadow-members",
                "remove legacy shadow member records",
            )
            .with_aliases(["cleanup-shadow-members", "cleanup-shadows"]),
            CliCommandSpec::new("status", "show cluster status"),
            CliCommandSpec::new("health", "show cluster node health").with_aliases(["nodes"]),
            CliCommandSpec::new("doctor", "diagnose cluster reachability")
                .with_aliases(["diagnose"]),
            CliCommandSpec::new("connectivity", "show cluster connectivity routes")
                .with_aliases(["routes"]),
            CliCommandSpec::new("peers", "list known peers"),
            CliCommandSpec::new("leave", "remove local cluster state"),
        ])
}

fn parse_cluster_matches(matches: &CliCommandMatches) -> Result<ClusterCliCommand, CliParseError> {
    let path = matches
        .path()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let node_id = |value: &str| {
        NodeId::new(value.to_string()).map_err(|source| CliParseError::Parse {
            detail: format!("invalid node id `{value}`: {source}"),
        })
    };
    match path.as_slice() {
        ["cluster", "recover-endpoint"] => {
            let action = match required_positional(matches, 0)? {
                "apply" => cluster::EndpointRecoveryAction::Apply,
                "status" => cluster::EndpointRecoveryAction::Status,
                "rollback" => cluster::EndpointRecoveryAction::Rollback,
                _ => {
                    return Err(CliParseError::Parse {
                        detail: "expected apply, status or rollback".into(),
                    });
                }
            };
            Ok(ClusterCliCommand::RecoverEndpoint {
                action,
                request_file: required_positional(matches, 1)?.to_owned(),
            })
        }
        ["cluster", "init"] => {
            let defaults = ClusterInitOptions::default();
            Ok(ClusterCliCommand::Init(ClusterInitOptions {
                cluster_id: matches
                    .value("--cluster-id")
                    .map(parse_cluster_id)
                    .transpose()
                    .map_err(semantic_parse_error)?,
                node_id: matches.value("--node-id").map(node_id).transpose()?,
                name: matches
                    .value("--name")
                    .map(non_empty_cluster_name)
                    .transpose()
                    .map_err(semantic_parse_error)?
                    .unwrap_or(defaults.name),
            }))
        }
        ["cluster", "invite"] => {
            let ttl_seconds = matches
                .value("--ttl-seconds")
                .map(|value| {
                    value.parse::<i64>().map_err(|source| CliParseError::Parse {
                        detail: format!("invalid invite ttl `{value}`: {source}"),
                    })
                })
                .transpose()?
                .unwrap_or(CLUSTER_INVITE_DEFAULT_TTL_SECONDS);
            if ttl_seconds <= 0 {
                return Err(CliParseError::Parse {
                    detail: "invite ttl must be greater than zero seconds".to_string(),
                });
            }
            let mut options = ClusterInviteOptions {
                ttl_seconds,
                introducers: matches
                    .values("--introducer")
                    .map(|value| non_empty_address(value).map_err(semantic_parse_error))
                    .collect::<Result<Vec<_>, _>>()?,
                output: matches.value("--output").map(PathBuf::from),
                channel_outputs: matches
                    .values("--channel-output")
                    .map(|value| {
                        parse_cluster_invite_channel_output(value).map_err(semantic_parse_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            };
            normalize_cluster_invite_options(&mut options).map_err(semantic_parse_error)?;
            Ok(ClusterCliCommand::Invite(options))
        }
        ["cluster", "join"] => {
            let mut options = ClusterJoinOptions {
                invite: required_positional(matches, 0)?.to_string(),
                cluster_id: matches
                    .value("--cluster-id")
                    .map(parse_cluster_id)
                    .transpose()
                    .map_err(semantic_parse_error)?,
                node_id: matches.value("--node-id").map(node_id).transpose()?,
                name: default_cluster_node_name("sinomo-node"),
                name_explicit: matches.contains("--name"),
            };
            if let Some(name) = matches.value("--name") {
                options.name = non_empty_cluster_name(name).map_err(semantic_parse_error)?;
            }
            Ok(ClusterCliCommand::Join(options))
        }
        ["cluster", "migrate", "plan"] => Ok(ClusterCliCommand::Migrate(
            ClusterMigrateCommand::Plan(ClusterMigrationPlanOptions {
                invite: required_positional(matches, 0)?.to_string(),
                mode: matches
                    .value("--mode")
                    .map(parse_cluster_migration_mode)
                    .transpose()
                    .map_err(semantic_parse_error)?
                    .unwrap_or(ClusterMigrationMode::SingleNode),
            }),
        )),
        ["cluster", "rename"] => Ok(ClusterCliCommand::Rename(ClusterRenameOptions {
            node_id: node_id(required_positional(matches, 0)?)?,
            name: non_empty_cluster_name(required_positional(matches, 1)?)
                .map_err(semantic_parse_error)?,
        })),
        ["cluster", "rename-hostnames"] => Ok(ClusterCliCommand::RenameHostnames),
        ["cluster", "role"] => Ok(ClusterCliCommand::Role(ClusterRoleOptions {
            node_id: node_id(required_positional(matches, 0)?)?,
            role: parse_cluster_node_role(required_positional(matches, 1)?)
                .map_err(semantic_parse_error)?,
        })),
        ["cluster", "state-authority"] => Ok(ClusterCliCommand::StateAuthority(
            ClusterStateAuthorityOptions {
                node_id: node_id(required_positional(matches, 0)?)?,
                role: parse_cluster_state_authority_role(required_positional(matches, 1)?)
                    .map_err(semantic_parse_error)?,
            },
        )),
        ["cluster", "promote"] => Ok(ClusterCliCommand::StateAuthority(
            ClusterStateAuthorityOptions {
                node_id: node_id(required_positional(matches, 0)?)?,
                role: matches
                    .positionals()
                    .get(1)
                    .map(|role| parse_cluster_state_authority_role(role))
                    .transpose()
                    .map_err(semantic_parse_error)?
                    .unwrap_or(ClusterMemberRole::Voter),
            },
        )),
        ["cluster", "repair-members"] => Ok(ClusterCliCommand::RepairMembers(
            ClusterRepairMembersOptions {
                from: non_empty_address(
                    matches
                        .value("--from")
                        .or_else(|| matches.positionals().first().map(String::as_str))
                        .ok_or_else(|| CliParseError::Parse {
                            detail: "cluster repair-members requires --from <host:port|iroh://...>"
                                .to_string(),
                        })?,
                )
                .map_err(semantic_parse_error)?,
                promote: matches.value("--promote").map(node_id).transpose()?,
            },
        )),
        ["cluster", "remove"] => Ok(ClusterCliCommand::Remove(ClusterRemoveOptions {
            node_ids: matches
                .positionals()
                .iter()
                .map(|value| node_id(value))
                .collect::<Result<Vec<_>, _>>()?,
        })),
        ["cluster", "cleanup-legacy-shadow-members"] => {
            Ok(ClusterCliCommand::CleanupLegacyShadowMembers)
        }
        ["cluster", "status"] => Ok(ClusterCliCommand::Status),
        ["cluster", "health"] => Ok(ClusterCliCommand::Health),
        ["cluster", "doctor"] => Ok(ClusterCliCommand::Doctor),
        ["cluster", "connectivity"] => Ok(ClusterCliCommand::Connectivity),
        ["cluster", "peers"] => Ok(ClusterCliCommand::Peers),
        ["cluster", "leave"] => Ok(ClusterCliCommand::Leave),
        _ => Err(CliParseError::Parse {
            detail: "cluster requires a subcommand".to_string(),
        }),
    }
}

#[cfg(test)]
pub(crate) fn parse_cluster_schema_for_test(
    args: &[String],
) -> Result<ClusterCliCommand, CliError> {
    parse_schema_command_for_test(cluster_cli_spec(), args, parse_cluster_matches)
}

fn parse_cluster_invite_channel_output(
    value: &str,
) -> Result<ClusterInviteChannelOutput, CliError> {
    let Some((introducer, output)) = value.split_once('=') else {
        return Err(CliError::Parse {
            detail: "--channel-output must use <introducer>=<path>".to_string(),
        });
    };
    let output = output.trim();
    if output.is_empty() {
        return Err(CliError::Parse {
            detail: "--channel-output path cannot be empty".to_string(),
        });
    }
    Ok(ClusterInviteChannelOutput {
        introducer: non_empty_address(introducer)?,
        output: PathBuf::from(output),
    })
}

fn normalize_cluster_invite_options(options: &mut ClusterInviteOptions) -> Result<(), CliError> {
    let mut introducers = Vec::with_capacity(
        options
            .introducers
            .len()
            .saturating_add(options.channel_outputs.len()),
    );
    for introducer in &options.introducers {
        let introducer = non_empty_address(introducer)?;
        if !introducers.contains(&introducer) {
            introducers.push(introducer);
        }
    }

    let mut channel_introducers = Vec::with_capacity(options.channel_outputs.len());
    let mut output_paths = Vec::with_capacity(
        options
            .channel_outputs
            .len()
            .saturating_add(usize::from(options.output.is_some())),
    );
    if let Some(output) = &options.output {
        if output.as_os_str().is_empty() {
            return Err(CliError::Parse {
                detail: "cluster invite output path cannot be empty".to_string(),
            });
        }
        output_paths.push(output.clone());
    }
    for channel_output in &mut options.channel_outputs {
        channel_output.introducer = non_empty_address(&channel_output.introducer)?;
        if channel_output.output.as_os_str().is_empty() {
            return Err(CliError::Parse {
                detail: "--channel-output path cannot be empty".to_string(),
            });
        }
        if channel_introducers.contains(&channel_output.introducer) {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate --channel-output introducer `{}`",
                    channel_output.introducer
                ),
            });
        }
        if output_paths.contains(&channel_output.output) {
            return Err(CliError::Parse {
                detail: format!(
                    "duplicate cluster invite output path `{}`",
                    channel_output.output.display()
                ),
            });
        }
        channel_introducers.push(channel_output.introducer.clone());
        output_paths.push(channel_output.output.clone());
        if !introducers.contains(&channel_output.introducer) {
            introducers.push(channel_output.introducer.clone());
        }
    }
    options.introducers = introducers;
    Ok(())
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

pub(crate) fn parse_cluster_state_authority_role(
    role: &str,
) -> Result<ClusterMemberRole, CliError> {
    match role.trim().to_ascii_lowercase().as_str() {
        "authority" | "state-authority" | "state_authority" | "voter" => {
            Ok(ClusterMemberRole::Voter)
        }
        "replica" | "learner" => Ok(ClusterMemberRole::Learner),
        "observer" | "non-authority" | "non_authority" | "non-voting" | "non_voting"
        | "nonvoting" => Ok(ClusterMemberRole::NonVoting),
        _ => Err(CliError::Parse {
            detail: format!(
                "invalid cluster state authority role `{role}`; expected authority, replica, or observer"
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

pub(crate) fn default_cluster_node_name(fallback: &str) -> String {
    default_cluster_node_name_with(fallback, local_hostname())
}

pub(crate) fn default_cluster_node_name_with(fallback: &str, hostname: Option<String>) -> String {
    hostname
        .and_then(|hostname| clean_cluster_hostname(&hostname))
        .unwrap_or_else(|| fallback.to_string())
}

fn local_hostname() -> Option<String> {
    for key in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(key)
            && let Some(hostname) = clean_cluster_hostname(&value)
        {
            return Some(hostname);
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|hostname| clean_cluster_hostname(&hostname))
}

fn clean_cluster_hostname(hostname: &str) -> Option<String> {
    let hostname = hostname.trim().trim_end_matches('.');
    if hostname.is_empty()
        || hostname.eq_ignore_ascii_case("localhost")
        || hostname.eq_ignore_ascii_case("localhost.localdomain")
        || hostname.eq_ignore_ascii_case("(none)")
        || hostname.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some(hostname.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterHostnameRenameTarget {
    pub(crate) node_id: NodeId,
    pub(crate) name: String,
}

pub(crate) fn cluster_hostname_rename_targets(
    state: &ClusterBootstrapState,
    local_hostname: Option<&str>,
    statuses: &[ClusterNodeStatusRecord],
) -> Vec<ClusterHostnameRenameTarget> {
    let local_hostname = local_hostname.and_then(clean_cluster_hostname);
    state
        .members
        .iter()
        .filter(|member| {
            !matches!(member.state, ClusterMemberState::Removed)
                && !cluster_member_is_legacy_shadow_daemon(member)
        })
        .filter_map(|member| {
            let name = if member.node_id == state.local_node_id {
                local_hostname
                    .clone()
                    .or_else(|| cluster_node_status_hostname(statuses, &member.node_id))
                    .or_else(|| cluster_member_hostname_from_endpoints(member))
            } else {
                cluster_node_status_hostname(statuses, &member.node_id)
                    .or_else(|| cluster_member_hostname_from_endpoints(member))
            }?;
            Some(ClusterHostnameRenameTarget {
                node_id: member.node_id.clone(),
                name,
            })
        })
        .collect()
}

fn cluster_node_status_hostname(
    statuses: &[ClusterNodeStatusRecord],
    node_id: &NodeId,
) -> Option<String> {
    statuses
        .iter()
        .filter(|status| status.node_id == *node_id)
        .find(|status| status.process == sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE)
        .and_then(|status| status.hostname.as_deref())
        .and_then(clean_cluster_hostname)
        .or_else(|| {
            statuses
                .iter()
                .filter(|status| status.node_id == *node_id)
                .find_map(|status| status.hostname.as_deref().and_then(clean_cluster_hostname))
        })
}

pub(crate) fn cluster_member_hostname_from_endpoints(
    member: &ClusterMemberRecord,
) -> Option<String> {
    let mut candidates = member
        .endpoints
        .iter()
        .filter_map(|endpoint| {
            let hostname = cluster_endpoint_hostname(&endpoint.address)?;
            Some((
                endpoint_address_port(&endpoint.address) != Some(SINOMO_DAEMON_DEFAULT_PORT),
                endpoint.priority,
                hostname,
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    candidates
        .into_iter()
        .map(|(_, _, hostname)| hostname)
        .next()
}

pub(crate) fn cluster_endpoint_hostname(address: &str) -> Option<String> {
    let address = address.trim();
    if address.is_empty()
        || address.starts_with("iroh://")
        || address.parse::<std::net::SocketAddr>().is_ok()
        || address.parse::<std::net::IpAddr>().is_ok()
    {
        return None;
    }
    let host = if let Some(rest) = address.strip_prefix('[') {
        let end = rest.find(']')?;
        &rest[..end]
    } else {
        address
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(address)
    };
    if host.parse::<std::net::IpAddr>().is_ok()
        || host.contains('/')
        || host.contains('?')
        || host.contains('#')
        || host.contains('@')
    {
        return None;
    }
    clean_cluster_hostname(host)
}

fn endpoint_address_port(address: &str) -> Option<u16> {
    let address = address.trim();
    if let Ok(socket) = address.parse::<std::net::SocketAddr>() {
        return Some(socket.port());
    }
    address.rsplit_once(':')?.1.parse().ok()
}

pub(crate) fn run_cluster_cli_command(command: ClusterCliCommand) -> Result<(), CliError> {
    match command {
        ClusterCliCommand::Init(options) => cluster_init(options),
        ClusterCliCommand::Invite(options) => cluster_invite(options),
        ClusterCliCommand::Join(options) => cluster_join(options),
        ClusterCliCommand::Migrate(command) => cluster_migrate(command),
        ClusterCliCommand::Rename(options) => cluster_rename(options),
        ClusterCliCommand::RenameHostnames => cluster_rename_hostnames(),
        ClusterCliCommand::Role(options) => cluster_role(options),
        ClusterCliCommand::StateAuthority(options) => cluster_state_authority_role(options),
        ClusterCliCommand::RepairMembers(options) => cluster_repair_members(options),
        ClusterCliCommand::RecoverEndpoint {
            action,
            request_file,
        } => {
            let bytes = std::fs::read(&request_file).map_err(|e| CliError::Parse {
                detail: format!("read recovery request: {e}"),
            })?;
            let spec: cluster::EndpointRecoverySpec =
                serde_json::from_slice(&bytes).map_err(|e| CliError::Parse {
                    detail: format!("decode recovery request: {e}"),
                })?;
            let response = cluster_control_daemon_client("recover-endpoint")?
                .cluster_recover_endpoint(cluster::EndpointRecoveryRequest { spec, action })?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response).map_err(|e| CliError::Parse {
                    detail: e.to_string()
                })?
            );
            Ok(())
        }
        ClusterCliCommand::Remove(options) => cluster_remove(options),
        ClusterCliCommand::CleanupLegacyShadowMembers => cluster_cleanup_legacy_shadow_members(),
        ClusterCliCommand::Status => cluster_status(),
        ClusterCliCommand::Health => cluster_health(),
        ClusterCliCommand::Doctor => cluster_doctor(),
        ClusterCliCommand::Connectivity => cluster_connectivity(),
        ClusterCliCommand::Peers => cluster_peers(),
        ClusterCliCommand::Leave => cluster_leave(),
    }
}

pub(crate) fn cluster_paths_from_defaults() -> Result<ClusterPaths, CliError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(ClusterPaths::new(&data_dir))
}

/// Daemon-first access for cluster control commands: a client bound to the
/// reachable local main daemon, or `None` when no daemon answers its status
/// probe (or the CLI runs with `--local`).
fn reachable_cluster_control_daemon() -> Result<Option<DaemonApiClient>, CliError> {
    if cli_force_local_mode() {
        return Ok(None);
    }
    Ok(default_main_daemon_status()?.map(|status| DaemonApiClient::at_address(status.address)))
}

/// Cluster control mutations are executed by the daemon (`/cluster/control/*`);
/// without a reachable daemon they fail with guidance instead of mutating
/// cluster state directly.
fn cluster_control_daemon_client(action: &str) -> Result<DaemonApiClient, CliError> {
    reachable_cluster_control_daemon()?.ok_or_else(|| cluster_daemon_required_error(action))
}

/// Actionable failure for daemon-first cluster commands when no main daemon is
/// reachable.
pub(crate) fn cluster_daemon_required_error(action: &str) -> CliError {
    CliError::DaemonNotRunning {
        detail: format!(
            "`sinomo cluster {action}` is executed by the local main daemon; start it with `sinomo daemon start` and retry"
        ),
    }
}

pub(crate) fn cluster_kv_state_authority_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, network::NetworkError> {
    paths
        .kv_paths()
        .known_raft_voter_leader_id(state)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

#[cfg(test)]
pub(crate) fn set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, network::NetworkError> {
    paths
        .kv_paths()
        .set_raft_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

pub(crate) fn locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, network::NetworkError> {
    paths
        .kv_paths()
        .locked_set_raft_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

#[cfg(test)]
pub(crate) fn clear_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, network::NetworkError> {
    paths
        .kv_paths()
        .clear_raft_leader_and_write_snapshot_if_changed(node_id)
        .map_err(sinomo_runtime::network::NetworkError::directory)
}

pub(crate) fn require_cluster_bootstrap_state() -> Result<ClusterBootstrapState, CliError> {
    let paths = cluster_paths_from_defaults()?;
    read_cluster_state(&paths)
}

pub(crate) fn require_cluster_runtime_quorum_with_paths_and_daemon(
    paths: &ClusterPaths,
    daemon: Option<&DaemonStatus>,
) -> Result<(), CliError> {
    let state = read_cluster_state(paths).map_err(|error| match error {
        CliError::ClusterOrphan => CliError::ClusterReadOnly {
            detail: "local node has no cluster state; cluster-dependent operations are paused"
                .to_string(),
        },
        other => other,
    })?;
    let daemon = daemon.ok_or_else(|| CliError::ClusterReadOnly {
        detail: "local daemon is not running; cluster-dependent operations are paused".to_string(),
    })?;
    if !local_daemon_matches_cluster_state(daemon, &state) {
        return Err(CliError::ClusterReadOnly {
            detail: "local daemon identity does not match the local cluster state; cluster-dependent operations are paused"
                .to_string(),
        });
    }
    let quorum = cluster_state_authority_quorum_status(paths, &state, Some(daemon)).map_err(
        |error| CliError::ClusterReadOnly {
            detail: format!(
                "cannot project committed cluster voter configuration for quorum admission: {error}"
            ),
        },
    )?;
    if quorum.has_quorum() {
        Ok(())
    } else {
        Err(CliError::ClusterReadOnly {
            detail: format!(
                "quorum unavailable; cluster-dependent operations are paused ({} reachable cluster state authorities, {} required)",
                quorum.reachable, quorum.required
            ),
        })
    }
}

/// Returns whether the already-probed local daemon owns this cluster state.
///
/// CLI admission is observational: it must not issue an authoritative KV
/// snapshot RPC because that RPC commits a Raft barrier. Reachability is
/// checked separately through the read-only cluster authority probes, and the
/// requested operation remains responsible for its own authoritative commit.
pub(crate) fn local_daemon_matches_cluster_state(
    daemon: &DaemonStatus,
    state: &ClusterBootstrapState,
) -> bool {
    let Some(cluster) = daemon.cluster.as_ref() else {
        return false;
    };
    cluster.cluster_id == state.cluster_id && cluster.node_id == state.local_node_id
}

/// `sinomo cluster init`: daemon-first — a reachable main daemon initializes
/// the cluster through `/cluster/control/init`. Without a daemon the direct
/// local path stays available for first boot (it still fails with "already
/// initialized" guidance when cluster state exists).
pub(crate) fn cluster_init(options: ClusterInitOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    if let Some(client) = reachable_cluster_control_daemon()? {
        if options.cluster_id.is_some() || options.node_id.is_some() {
            return Err(CliError::Parse {
                detail: "--cluster-id/--node-id are only supported by the local first-boot fallback; the daemon assigns cluster and node ids (stop the daemon with `sinomo daemon stop` to initialize with explicit ids)".to_string(),
            });
        }
        let response = client.cluster_init(cluster::ClusterControlInitRequest {
            cluster_name: options.name.clone(),
            node_display_name: Some(options.name),
            node_role: None,
        })?;
        println!("cluster initialized: {}", response.cluster_id);
        println!("local node: {}", response.node_id);
        println!("state: {}", paths.state.display());
        print_daemon_cluster_identity_activation_notice();
        return Ok(());
    }
    cluster_init_with_paths(options, &paths)?;
    nudge_local_daemon_cluster_activation();
    Ok(())
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
        paths,
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        now,
    )?;
    cluster::assign_local_daemon_member_public_keys(&mut state, main_public_key, shadow_public_key);
    cluster::write_cluster_state(paths, &state)?;
    locked_set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
    cluster::bootstrap_single_voter_cluster_state_to_local_kv(paths, &state)?;

    Ok(state)
}

/// `sinomo cluster invite`: daemon-required — the daemon builds, signs, and
/// publishes the invite through `/cluster/control/invite` with its own
/// advertised introducers unless explicit invite options override them. The
/// direct implementation stays reachable through [`cluster_invite_with_paths`].
pub(crate) fn cluster_invite(mut options: ClusterInviteOptions) -> Result<(), CliError> {
    normalize_cluster_invite_options(&mut options)?;
    let client = cluster_control_daemon_client("invite")?;
    let response = client.cluster_invite_with_request(cluster::ClusterControlInviteRequest {
        ttl_seconds: (options.ttl_seconds != CLUSTER_INVITE_DEFAULT_TTL_SECONDS)
            .then_some(options.ttl_seconds),
        introducers: options.introducers.clone(),
    })?;
    let uri = response.invite;
    let invite = decode_cluster_join_invite(&uri)?;
    let channel_join_commands =
        cluster_invite_channel_join_commands(&invite, &options.channel_outputs)?;
    if !cluster_invite_has_remote_usable_introducer(&invite) {
        println!(
            "warning: invite only contains local-only introducers; remote machines cannot use it until the daemon advertises a LAN-reachable address"
        );
    }
    let join_command = cluster_join_command(&uri);
    println!("invite: {uri}");
    println!("join command: {join_command}");
    println!(
        "cluster: {}",
        response.cluster_id.as_ref().unwrap_or(&invite.cluster_id)
    );
    println!(
        "expires: {}",
        format_rfc3339(response.expires_at.unwrap_or(invite.expires_at))
    );
    println!(
        "saved: {}",
        sinomo_runtime::node::cluster_invitation_key(&invite.invite_id)
    );
    if let Some(output) = &options.output {
        write_cluster_invite_command(output, &join_command)?;
        println!("output: {}", output.display());
    }
    for (channel_output, join_command) in options
        .channel_outputs
        .iter()
        .zip(channel_join_commands.iter())
    {
        write_cluster_invite_command(&channel_output.output, join_command)?;
        println!(
            "channel output ({}): {}",
            channel_output.introducer,
            channel_output.output.display()
        );
    }
    Ok(())
}

fn cluster_invite_channel_join_commands(
    invite: &ClusterJoinInvite,
    channel_outputs: &[ClusterInviteChannelOutput],
) -> Result<Vec<String>, CliError> {
    channel_outputs
        .iter()
        .map(|channel_output| {
            let mut matches = invite
                .communication_channels
                .iter()
                .filter(|channel| channel.endpoint.address == channel_output.introducer);
            let channel = matches.next().ok_or_else(|| CliError::Parse {
                detail: format!(
                    "invite response did not contain requested channel `{}`",
                    channel_output.introducer
                ),
            })?;
            if matches.next().is_some() {
                return Err(CliError::Parse {
                    detail: format!(
                        "invite response contained duplicate channels for `{}`",
                        channel_output.introducer
                    ),
                });
            }

            let mut projected = invite.clone();
            projected.communication_channels = vec![channel.clone()];
            projected.introducers.clear();
            let uri = encode_cluster_join_invite(&projected)?;
            Ok(cluster_join_command(&uri))
        })
        .collect()
}

fn write_cluster_invite_command(output: &Path, join_command: &str) -> Result<(), CliError> {
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|source| CliError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    fs::write(output, format!("{join_command}\n")).map_err(|source| CliError::Io {
        path: output.display().to_string(),
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RenderedClusterInvite {
    pub(crate) invite: ClusterJoinInvite,
    pub(crate) uri: String,
    pub(crate) join_command: String,
    pub(crate) persisted_key: String,
    pub(crate) output_path: Option<PathBuf>,
}

pub(crate) use sinomo_runtime::cluster::ClusterMigrationPlanReport;

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
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    let mut source = read_cluster_state(paths)?;
    cluster::plan_cluster_migration_with_host(
        paths,
        &mut source,
        &invite,
        options.mode,
        OffsetDateTime::now_utc(),
        &mut CliClusterMemberMutationHost,
    )
    .map_err(Into::into)
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
    mut options: ClusterInviteOptions,
    paths: &ClusterPaths,
) -> Result<RenderedClusterInvite, CliError> {
    normalize_cluster_invite_options(&mut options)?;
    paths.ensure()?;
    let state = read_cluster_state(paths)?;
    if !sinomo_runtime::kv::cluster_kv_raft_local_is_voter(&state) {
        return Err(cluster_invite_requires_voter_error(paths, &state));
    }
    let now = OffsetDateTime::now_utc();
    let invite = build_cluster_invite(&state, &options, paths, now)?;
    if !cluster_invite_has_remote_usable_introducer(&invite) {
        println!(
            "warning: invite only contains local-only introducers; remote machines should use `sinomo cluster invite --introducer <host-lan-ip>:21353`"
        );
    }
    let uri = encode_cluster_join_invite(&invite)?;
    let join_command = cluster_join_command(&uri);
    let channel_join_commands =
        cluster_invite_channel_join_commands(&invite, &options.channel_outputs)?;
    publish_cluster_invitation(paths, &state, &invite)?;
    if let Some(output) = &options.output {
        write_cluster_invite_command(output, &join_command)?;
    }
    for (channel_output, join_command) in options
        .channel_outputs
        .iter()
        .zip(channel_join_commands.iter())
    {
        write_cluster_invite_command(&channel_output.output, join_command)?;
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
    match cluster::publish_cluster_invitation_to_quorum_with_host(
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
    cluster::build_cluster_join_invite(
        state,
        &issuer_identity,
        introducers,
        options.ttl_seconds,
        now,
    )
    .map_err(Into::into)
}

pub(crate) fn cluster_invite_has_remote_usable_introducer(invite: &ClusterJoinInvite) -> bool {
    invite
        .communication_channels
        .iter()
        .map(|channel| &channel.endpoint)
        .chain(invite.introducers.iter())
        .any(is_remote_usable_invite_endpoint)
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
                cluster::push_unique_introducer(&mut introducers, endpoint);
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
        cluster::push_unique_introducer(&mut introducers, endpoint);
    }
    cluster::sort_default_invite_introducers(&mut introducers);
    cluster::compact_default_invite_introducers(&mut introducers);
    introducers
}

/// `sinomo cluster join`: daemon-first — fresh joins, stale-pending retries,
/// AND single-node migrations (a node already settled in another cluster) all
/// run inside the daemon through `/cluster/control/join`; the daemon
/// quarantines the source storage and imports the node-local configs when it
/// migrates.
pub(crate) fn cluster_join(options: ClusterJoinOptions) -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    paths.ensure()?;
    let client = cluster_control_daemon_client("join <invite>")?;
    cluster_join_via_daemon(&client, options, &paths)
}

/// Sends a fresh (or stale-pending retry) cluster join through the daemon,
/// with a best-effort follow-up rename when `--name` was explicit.
fn cluster_join_via_daemon(
    client: &DaemonApiClient,
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    if options.node_id.is_some() {
        return Err(CliError::Parse {
            detail: "--node-id is not supported for daemon-mediated cluster join; the daemon keeps its own node identity (rerun without --node-id)".to_string(),
        });
    }
    let invite_uri = cluster_join_invite_argument_uri(&options.invite)?;
    let invite = decode_cluster_join_invite(&invite_uri)?;
    if let Some(cluster_id) = &options.cluster_id {
        if !cluster::invite_has_embedded_cluster_identity(&invite) {
            return Err(CliError::Parse {
                detail: "--cluster-id is not supported for daemon-mediated cluster join; the daemon uses the invite's embedded cluster identity".to_string(),
            });
        }
        if cluster_id != &invite.cluster_id {
            return Err(CliError::Parse {
                detail: format!(
                    "cluster id override `{cluster_id}` does not match invite cluster `{}`",
                    invite.cluster_id
                ),
            });
        }
    }
    let response = client.cluster_join(cluster::ClusterControlJoinRequest {
        invite: invite_uri,
        node_display_name: options.name_explicit.then_some(options.name.clone()),
        node_role: None,
    })?;
    if let Some(endpoint) = &response.accepted_endpoint {
        println!("introducer accepted join: {endpoint}");
    }
    if let Some(migration) = &response.migration {
        println!("cluster migration finalized: {}", migration.migration_id);
        println!("source cluster: {}", migration.source_cluster_id);
        println!("source quarantine: {}", migration.quarantine_root);
        println!(
            "node-local config keys: {} imported, {} deferred",
            migration.imported_node_local_config_keys, migration.deferred_node_local_config_keys
        );
    }
    println!("cluster join completed: {}", response.cluster_id);
    println!("local node: {}", response.node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    print_daemon_cluster_identity_activation_notice();
    Ok(())
}

pub(crate) fn cluster_join_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    paths.ensure()?;
    if paths.state.exists() {
        let existing = read_cluster_state(paths)?;
        if cluster_state_needs_join_registration(&existing) {
            return cluster_join_fresh_after_stale_pending_state(options, paths, existing);
        }
        return cluster_join_single_node_migration_with_paths(options, paths, existing);
    }
    cluster_join_fresh_with_paths(options, paths)
}

fn cluster_join_fresh_after_stale_pending_state(
    mut options: ClusterJoinOptions,
    paths: &ClusterPaths,
    stale: ClusterBootstrapState,
) -> Result<(), CliError> {
    if options.node_id.is_none() {
        options.node_id = Some(stale.local_node_id);
    }
    remove_stale_pending_cluster_join_state(paths)?;
    cluster_join_fresh_with_paths(options, paths)
}

fn remove_stale_pending_cluster_join_state(paths: &ClusterPaths) -> Result<(), CliError> {
    match fs::remove_file(&paths.state) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CliError::Io {
            path: paths.state.display().to_string(),
            detail: format!("failed to remove stale pending cluster join state: {source}"),
        }),
    }
}

fn cluster_join_fresh_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    let local_node_id = options.node_id.unwrap_or_else(NodeId::generate);
    let timestamp = OffsetDateTime::now_utc();
    let invite = parse_cluster_join_invite_argument(&options.invite)?;
    if cluster::invite_has_embedded_signature(&invite) {
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
        && cluster::invite_has_embedded_cluster_identity(&invite)
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
    let mut state = cluster::prepare_pending_cluster_join_state(
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
    ensure_local_daemon_cluster_members(
        paths,
        &mut state,
        &main_options,
        &shadow_options,
        timestamp,
    )?;
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    if cluster_state_needs_join_registration(&state) {
        return Err(CliError::ClusterReadOnly {
            detail: "cluster join was not accepted by any introducer; local cluster state was not changed".to_string(),
        });
    }
    cluster::write_cluster_state(paths, &state)?;
    println!("cluster join completed: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    println!("invite: {}", options.invite);
    println!("state: {}", paths.state.display());
    Ok(())
}

/// Direct single-node migration (the daemon-less variant of the daemon's
/// `/cluster/control/join` migration branch): shared machinery lives in
/// `sinomo_runtime::cluster`.
fn cluster_join_single_node_migration_with_paths(
    options: ClusterJoinOptions,
    paths: &ClusterPaths,
    mut source: ClusterBootstrapState,
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
            detail: "source cluster changed while planning migration; run cluster join again"
                .to_string(),
        });
    }

    let mut migration = report.migration;
    cluster::write_cluster_migration_record_to_source_kv_with_host(
        paths,
        &mut source,
        &migration,
        &mut CliClusterMemberMutationHost,
    )?;
    let dfs_moves: Vec<(PathBuf, PathBuf)> = paths
        .root
        .parent()
        .map(|data_dir_root| {
            let data_dir = SinomoDataDir::new(data_dir_root);
            vec![(
                default_dfs_metadata_shards_root(&data_dir),
                dfs_import_metadata_root(&data_dir, &migration.source_cluster_id),
            )]
        })
        .unwrap_or_default();
    let quarantine =
        cluster::quarantine_cluster_storage_for_migration(paths, &migration, &dfs_moves)?;
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
    cluster::write_local_cluster_migration_record(&quarantine.root, &migration)?;

    let target_join_options = ClusterJoinOptions {
        node_id: Some(migration.target_node_id.clone()),
        ..options
    };

    match cluster_join_fresh_with_paths(target_join_options, paths) {
        Ok(()) => {
            let mut joined = read_cluster_state(paths)?;
            migration.target_cluster_id = joined.cluster_id.clone();
            migration.target_cluster_name = Some(joined.cluster_name.clone());
            migration.target_node_id = joined.local_node_id.clone();
            let mut driver = cli_cluster_control_plane_driver(paths);
            let kv_import = cluster::import_node_local_configs_from_source_kv_with_host(
                paths,
                &mut joined,
                &quarantine.root,
                &migration,
                cli_cluster_kv_txn_quorum_options(true),
                &mut driver,
            )?;
            migration.phase = ClusterMigrationPhase::Finalized;
            migration.updated_at = OffsetDateTime::now_utc();
            migration
                .evidence_refs
                .push("source-kv-policy:target-authoritative".to_string());
            migration.evidence_refs.push(format!(
                "source-kv-import-report:{}",
                cluster::cluster_migration_kv_import_report_path(&quarantine.root).display()
            ));
            migration.detail = Some(format!(
                "single-node migration finalized; target cluster KV is authoritative; source cluster storage is quarantined locally; imported {} node-local config keys, deferred {}, skipped {} source cluster config keys; DFS shared import is deferred",
                kv_import.imported_node_local_config_keys.len(),
                kv_import.deferred_node_local_config_keys.len(),
                kv_import.skipped_cluster_level_config_count,
            ));
            cluster::write_local_cluster_migration_record(&quarantine.root, &migration)?;
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
            let _ = cluster::write_local_cluster_migration_record(&quarantine.root, &migration);
            cluster::rollback_cluster_storage_quarantine(&quarantine)?;
            Err(error)
        }
    }
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
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<bool, CliError> {
    let (main_endpoints, shadow_endpoints) =
        local_daemon_member_endpoints(paths, state, main_options, shadow_options)?;
    Ok(cluster::ensure_daemon_cluster_members(
        state,
        main_endpoints,
        shadow_endpoints,
        timestamp,
    ))
}

fn local_daemon_member_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
) -> Result<(Vec<ClusterEndpointRecord>, Vec<ClusterEndpointRecord>), CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let endpoint_config = read_daemon_endpoint_advertisement_config(data_dir)?;
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let lan_ips = network::discover_default_lan_ip_candidates();
    let route_ips =
        network::ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state(state);
    let main_endpoints = daemon_advertised_endpoints_for_state_with_manual_config(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
        &endpoint_config.main_endpoints,
    );
    let shadow_endpoints = daemon_advertised_endpoints_for_state_with_manual_config(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
        &endpoint_config.shadow_endpoints,
    );
    Ok((main_endpoints, shadow_endpoints))
}

fn refresh_local_daemon_membership_from_disk_for_cli(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> Result<cluster::ClusterLocalDaemonMembershipRefresh, CliError> {
    let (main_endpoints, shadow_endpoints) =
        local_daemon_member_endpoints(paths, state, main_options, shadow_options)?;
    let (main_public_key, shadow_public_key) = local_daemon_public_keys(paths)?;
    cluster::refresh_local_daemon_membership_from_disk(
        paths,
        state,
        cluster::ClusterLocalDaemonMembershipObservation {
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

    struct CliClusterJoinReconciliationTransport {
        state: ClusterBootstrapState,
    }

    impl cluster::ClusterControlPlaneTransport for CliClusterJoinReconciliationTransport {
        fn send_kv_replication(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &sinomo_runtime::kv::ClusterKvReplicationRequest,
            _timeout: Duration,
        ) -> Result<sinomo_runtime::kv::ClusterKvReplicationResponse, network::NetworkError>
        {
            Err(unsupported_join_reconciliation_transport(
                "cluster join kv replication",
            ))
        }

        fn request_kv_state_authority_vote(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _request: &sinomo_runtime::kv::ClusterKvStateAuthorityVoteRequest,
            _timeout: Duration,
        ) -> Result<sinomo_runtime::kv::ClusterKvStateAuthorityVoteResponse, network::NetworkError>
        {
            Err(unsupported_join_reconciliation_transport(
                "cluster join kv state authority vote",
            ))
        }

        fn probe_cluster_state_authority(
            &mut self,
            _endpoint: &ClusterEndpointRecord,
            _timeout: Duration,
        ) -> bool {
            false
        }

        fn fetch_cluster_kv_snapshot(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            timeout: Duration,
        ) -> Result<KvSnapshot, network::NetworkError> {
            daemon_fetch_cluster_kv_local_snapshot_from_endpoint(endpoint, timeout)
                .map(|read| read.value)
                .map_err(|source| network::NetworkError::Io {
                    operation: "cluster join snapshot fetch".to_string(),
                    detail: source.to_string(),
                })
        }

        fn call_endpoint_object_rpc(
            &mut self,
            endpoint: &ClusterEndpointRecord,
            request: &network::ObjectRpcRequest,
            timeout: Duration,
        ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
            send_join_rpc(&self.state, endpoint, request, timeout).map_err(|source| {
                network::NetworkError::Io {
                    operation: "cluster join membership rpc".to_string(),
                    detail: source.to_string(),
                }
            })
        }

        fn call_node_object_rpc(
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

        fn record_endpoint_command_connectivity(
            &mut self,
            paths: &ClusterPaths,
            state: &ClusterBootstrapState,
            endpoint: &ClusterEndpointRecord,
            connectivity_state: network::ClusterConnectivityState,
            latency_ms: Option<u64>,
        ) {
            let _ = network::record_local_cluster_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                connectivity_state,
                latency_ms,
                true,
            );
        }
    }

    let mut host = cluster::ClusterControlPlaneDriver::new(CliClusterJoinReconciliationTransport {
        state: state.clone(),
    });
    let reconciliation = cluster::reconcile_cluster_join_with_introducers_with_host(
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

fn send_join_rpc(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    match endpoint.transport {
        ClusterTransportKind::Iroh => send_join_endpoint_rpc(state, endpoint, request, timeout),
        _ => crate::daemon_send_daemon_rpc_with_timeouts(
            &endpoint.address,
            request,
            sinomo_runtime::network::DaemonRpcTimeouts::default()
                .with_connect_timeout(Some(timeout)),
        )
        .map_err(Into::into),
    }
}

fn send_join_endpoint_rpc(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, CliError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            let runtime = handle.clone();
            tokio::task::block_in_place(|| {
                runtime.block_on(send_join_endpoint_rpc_async(
                    state, endpoint, request, timeout,
                ))
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
        .block_on(send_join_endpoint_rpc_async(
            state, endpoint, request, timeout,
        ))
        .map_err(CliError::from);
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

async fn send_join_endpoint_rpc_async(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
) -> Result<network::ObjectRpcResponse, network::NetworkError> {
    let mut host = CliEndpointRpcHost::new(None, "cluster join endpoint rpc signing");
    let mut client = network::ClusterObjectRpcClient::endpoint_only(&mut host);
    let options = network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout);
    let result = client
        .call_endpoint(
            state,
            endpoint,
            network::ObjectRpcCall::value(request.clone()),
            options,
        )
        .await?;
    result.into_materialized_value_response(usize::MAX).await
}

pub(crate) fn parse_cluster_join_invite_argument(raw: &str) -> Result<ClusterJoinInvite, CliError> {
    decode_cluster_join_invite(&cluster_join_invite_argument_uri(raw)?).map_err(Into::into)
}

/// Expands a join invite argument (URI, join command, `@file`, or file path)
/// to the raw invite URI carried by a daemon-mediated join request.
pub(crate) fn cluster_join_invite_argument_uri(raw: &str) -> Result<String, CliError> {
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
    Ok(extract_invite_from_join_command(invite.trim()).to_string())
}

/// `sinomo cluster status`: local state summary (KV-synced from the daemon
/// first), plus node health counts from the daemon's `/cluster/health` view
/// when a daemon is reachable.
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
            println!("kv snapshot: {}", paths.kv_paths().snapshot.display());
            println!("members: {}", state.members.len());
            if let Some(rows) = try_fetch_daemon_cluster_health_snapshot_rows()? {
                let count = |availability: ClusterNodeAvailability| {
                    rows.iter()
                        .filter(|row| row.availability == availability)
                        .count()
                };
                println!(
                    "health: {} online, {} offline, {} unknown",
                    count(ClusterNodeAvailability::Online),
                    count(ClusterNodeAvailability::Offline),
                    count(ClusterNodeAvailability::Unknown)
                );
            }
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
    if let Some(rows) = try_fetch_daemon_cluster_health_snapshot_rows()? {
        print_cluster_health_rows(rows);
        return Ok(());
    }
    let state = match read_cluster_state(&paths) {
        Ok(state) => state,
        Err(_) => {
            refresh_cluster_kv_from_local_daemon(&paths)?;
            read_cluster_state(&paths)?
        }
    };
    let rows = cluster_health_rows_from_local_state(&paths, &state)?;
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
    // Doctor is an observational command. Live daemon health and direct DFS
    // probes below provide its current evidence; forcing an authoritative KV
    // snapshot here would turn diagnosis into a quorum barrier and can prevent
    // doctor from reporting the very authority failure it is meant to expose.
    let mut state = read_cluster_doctor_state(&paths)?;
    let rows = cluster_health_rows_for_cli(&paths, &state)?;
    if !rows.is_empty() {
        // The daemon health response is already a bounded, live member
        // projection. Use it for role/quorum findings instead of opening the
        // local KV store merely to refresh the bootstrap topology.
        state.members = rows.iter().map(|row| row.member.clone()).collect();
        state
            .members
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        state
            .members
            .dedup_by(|left, right| left.node_id == right.node_id);
    }
    let graph = network::read_cluster_connectivity_graph(&paths).unwrap_or_default();
    let mut findings = cluster_doctor_findings(&state, &graph, &rows);
    if let Some(data_dir) = paths.root.parent() {
        findings.extend(data_dir_storage_contract_findings(data_dir)?);
        match inspect_cluster_dfs_status_with_data_dir(SinomoDataDir::new(data_dir)) {
            Ok(report) => findings.extend(dfs_reachability_findings(&rows, &report)),
            Err(error) => findings.push(ClusterDoctorFinding {
                severity: "warning".to_string(),
                code: "dfs-status-unavailable".to_string(),
                detail: format!(
                    "cluster health succeeded, but the DFS reachability gate could not run: {error}"
                ),
            }),
        }
    }
    println!(
        "cluster doctor: {}",
        cluster_doctor_summary_label(&findings)
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

fn read_cluster_doctor_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, CliError> {
    // Do not call `read_cluster_state`: its KV overlay is appropriate for
    // commands that need the latest materialized directory, but it may attempt
    // a local Raft checkout. Doctor starts with the bounded bootstrap record;
    // `cluster_health_rows_for_cli` supplies the live daemon projection.
    let mut state = cluster::read_cluster_state_without_kv(paths)?;
    prepare_local_cluster_state_for_cli(paths, &mut state)?;
    Ok(state)
}

fn dfs_reachability_findings(
    health: &[ClusterHealthRow],
    dfs: &DfsClusterStatusReport,
) -> Vec<ClusterDoctorFinding> {
    let online = health
        .iter()
        .filter(|row| row.availability == ClusterNodeAvailability::Online)
        .map(|row| row.member.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut findings = dfs
        .nodes
        .iter()
        .filter_map(|node| {
            let error = node.error.as_deref()?;
            online
                .contains(&node.node_id)
                .then(|| ClusterDoctorFinding {
                    severity: "warning".to_string(),
                    code: "dfs-route-unavailable".to_string(),
                    detail: format!(
                        "node {} is online in cluster health but unavailable to the DFS object-RPC probe: {error}",
                        node.node_id
                    ),
                })
        })
        .collect::<Vec<_>>();
    if dfs.nodes.is_empty() && !online.is_empty() {
        findings.push(ClusterDoctorFinding {
            severity: "warning".to_string(),
            code: "dfs-provider-unavailable".to_string(),
            detail: "no online cluster service instance advertises a DFS provider capability"
                .to_string(),
        });
    }
    findings
}

fn cluster_doctor_summary_label(findings: &[ClusterDoctorFinding]) -> &'static str {
    if findings.iter().any(ClusterDoctorFinding::is_warning) {
        "warnings"
    } else {
        "ok"
    }
}

pub(crate) fn cluster_connectivity() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let state = read_cluster_state(&paths)?;
    let local = network::read_local_cluster_connectivity_records(&paths)?;
    let active = network::read_cluster_connectivity_graph(&paths)?;
    let kv = cluster::read_cluster_connectivity_graph_from_kv(&paths).unwrap_or_default();
    println!("cluster: {}", state.cluster_id);
    println!("local node: {}", state.local_node_id);
    print_connectivity_records("local", local);
    print_connectivity_records("active", active);
    print_connectivity_records("kv", kv);
    Ok(())
}

fn print_connectivity_records(scope: &str, records: Vec<network::ClusterConnectivityRecord>) {
    println!("{scope}: {} records", records.len());
    if records.is_empty() {
        return;
    }
    println!(
        "scope\tsource\ttarget\tendpoint\ttransport\tstate\tfailures\tlatency_ms\taddress\treason\tobserved_at\texpires_at"
    );
    for record in records {
        let latency = record
            .latency_ms
            .map(|latency| latency.to_string())
            .unwrap_or_else(|| "-".to_string());
        let reason = record
            .failure_reason
            .as_ref()
            .map(|reason| format!("{reason:?}"))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{}\t{}\t{}\t{}\t{:?}\t{:?}\t{}\t{}\t{}\t{}\t{}\t{}",
            scope,
            record.source_node_id,
            record.target_node_id,
            record.endpoint_id,
            record.transport,
            record.state,
            record.consecutive_failures,
            latency,
            record.address,
            reason,
            record.observed_at,
            record.expires_at
        );
    }
}

fn cluster_health_rows_for_cli(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterHealthRow>, CliError> {
    if let Some(rows) = try_fetch_daemon_cluster_health_rows()? {
        return Ok(rows);
    }
    cluster_health_rows_from_local_state(paths, state)
}

/// Read cluster health for an explicitly selected Sinomo data directory.
///
/// Agent and app runtimes may operate on a non-default data directory, so
/// daemon discovery must be anchored to that directory instead of consulting
/// the process environment or the user's home directory.
pub(crate) fn cluster_health_rows_for_data_dir(
    data_dir: &SinomoDataDir,
) -> Result<(ClusterBootstrapState, Vec<ClusterHealthRow>), CliError> {
    let paths = ClusterPaths::new(data_dir);
    match probe_daemon_status(&DaemonPaths::new(data_dir), DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => {
            let bootstrap_state = cluster::read_cluster_state_without_kv(&paths)?;
            let Some(daemon_cluster) = status.cluster.as_ref() else {
                return Err(CliError::DaemonNotRunning {
                    detail: "the daemon owning this data directory did not advertise a cluster identity; refusing to use it as the cluster-health authority".to_string(),
                });
            };
            if daemon_cluster.cluster_id != bootstrap_state.cluster_id
                || daemon_cluster.node_id != bootstrap_state.local_node_id
                || daemon_cluster.process != DAEMON_MAIN_CLUSTER_ROLE
            {
                return Err(CliError::DaemonNotRunning {
                    detail: format!(
                        "the daemon owning this data directory advertised cluster/node/process {}/{}/{}, expected {}/{}/{}; refusing to use it as the cluster-health authority",
                        daemon_cluster.cluster_id,
                        daemon_cluster.node_id,
                        daemon_cluster.process,
                        bootstrap_state.cluster_id,
                        bootstrap_state.local_node_id,
                        DAEMON_MAIN_CLUSTER_ROLE,
                    ),
                });
            }
            let rows = DaemonApiClient::at_address(status.address)
                .with_timeout(CLUSTER_HEALTH_DAEMON_RPC_TIMEOUT)
                .cluster_health_with_peer_views()?;
            // The daemon owns local Raft. Pair its live health rows with the
            // materialized member projection instead of trying the process-local
            // resident registry from this CLI process. A live daemon RPC failure
            // remains an error rather than falling through across that boundary.
            return Ok((
                read_cluster_state_with_materialized_member_overlay(&paths)?,
                rows,
            ));
        }
        DaemonProbeStatus::Locked { .. } => {
            return Err(CliError::DaemonNotRunning {
                detail: "main daemon owns this data directory but its health RPC did not answer; refusing direct cluster-state access while Raft may be active".to_string(),
            });
        }
        DaemonProbeStatus::NotRunning => {}
    }

    let state = match read_cluster_state(&paths) {
        Ok(state) => state,
        Err(_) => {
            refresh_cluster_kv_from_local_daemon_for_data_dir(data_dir, &paths)?;
            read_cluster_state(&paths)?
        }
    };
    let rows = cluster_health_rows_from_local_state(&paths, &state)?;
    Ok((state, rows))
}

fn cluster_health_rows_from_local_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterHealthRow>, CliError> {
    let mut rows = cluster::cluster_health_rows_with_probe(
        paths,
        state,
        &CliClusterStatusProbe { paths },
        false,
        OffsetDateTime::now_utc(),
    )
    .map_err(CliError::from)?;
    let availability = authoritative_local_main_daemon_availability(paths, state);
    apply_authoritative_local_main_availability(state, &mut rows, availability);
    Ok(rows)
}

fn authoritative_local_main_daemon_availability(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> ClusterNodeAvailability {
    let Some(member) = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
    else {
        return ClusterNodeAvailability::Unknown;
    };
    let Some(data_dir) = paths.root.parent() else {
        return ClusterNodeAvailability::Unknown;
    };
    let probe = probe_daemon_status(
        &DaemonPaths::with_root(data_dir.join("daemon")),
        CLUSTER_HEALTH_PROBE_TIMEOUT,
    );
    local_main_daemon_availability_from_probe(state, member, &probe)
}

fn local_main_daemon_availability_from_probe(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    probe: &DaemonProbeStatus,
) -> ClusterNodeAvailability {
    match probe {
        DaemonProbeStatus::Running(status) => {
            let Some(cluster) = status.cluster.as_ref() else {
                return ClusterNodeAvailability::Unknown;
            };
            if cluster.cluster_id != state.cluster_id
                || cluster.node_id != member.node_id
                || cluster.process != DAEMON_MAIN_CLUSTER_ROLE
            {
                return ClusterNodeAvailability::Unknown;
            }
            ClusterNodeAvailability::Online
        }
        DaemonProbeStatus::Locked { .. } => ClusterNodeAvailability::Unknown,
        DaemonProbeStatus::NotRunning => ClusterNodeAvailability::Offline,
    }
}

fn apply_authoritative_local_main_availability(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    availability: ClusterNodeAvailability,
) {
    let Some(row) = rows
        .iter_mut()
        .find(|row| row.member.node_id == state.local_node_id)
    else {
        return;
    };
    row.live_probe_observed = availability == ClusterNodeAvailability::Online;
    row.availability = availability.clone();
    row.status.availability = availability;
}

/// Cluster health rows from the daemon's `/cluster/health` aggregation via
/// [`DaemonApiClient`]; `None` when no daemon is reachable or the call fails
/// (readers fall back to the local state).
fn try_fetch_daemon_cluster_health_rows() -> Result<Option<Vec<ClusterHealthRow>>, CliError> {
    let Some(client) = reachable_cluster_control_daemon()? else {
        return Ok(None);
    };
    Ok(client
        .with_timeout(CLUSTER_HEALTH_DAEMON_RPC_TIMEOUT)
        .cluster_health_with_peer_views()
        .ok())
}

/// Read the daemon's already-published KV health view without triggering a
/// foreground peer sweep. Ordinary status/health commands are read-model
/// consumers; `cluster doctor` owns the explicit live diagnostic pass.
fn try_fetch_daemon_cluster_health_snapshot_rows() -> Result<Option<Vec<ClusterHealthRow>>, CliError>
{
    let Some(client) = reachable_cluster_control_daemon()? else {
        return Ok(None);
    };
    Ok(client
        .with_timeout(CLUSTER_HEALTH_DAEMON_RPC_TIMEOUT)
        .cluster_health()
        .ok())
}

/// `sinomo cluster peers`: prefers the daemon's aggregated `/cluster/health`
/// member view when a daemon is reachable, falling back to the local cluster
/// state read.
pub(crate) fn cluster_peers() -> Result<(), CliError> {
    let members = match try_fetch_daemon_cluster_health_rows()? {
        Some(rows) => {
            let mut seen = BTreeSet::new();
            rows.into_iter()
                .map(|row| row.member)
                .filter(|member| seen.insert(member.node_id.clone()))
                .collect()
        }
        None => {
            let paths = cluster_paths_from_defaults()?;
            cluster_peer_members(&paths)?
        }
    };
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

/// `sinomo cluster rename`: daemon-required — the member rename commits inside
/// the running daemon through `/cluster/control/rename`. The direct rename
/// machinery stays reachable through `cluster rename-hostnames`.
pub(crate) fn cluster_rename(options: ClusterRenameOptions) -> Result<(), CliError> {
    let client = cluster_control_daemon_client("rename <node-id> <name>")?;
    let update = client.cluster_rename(cluster::ClusterControlRenameRequest {
        node_id: Some(options.node_id),
        display_name: options.name,
    })?;
    if !update.changed {
        println!(
            "cluster member {} already has name {}",
            update.node_id, update.name
        );
        return Ok(());
    }

    println!(
        "cluster member {} renamed: {} -> {}",
        update.node_id, update.previous_name, update.name
    );
    Ok(())
}

pub(crate) fn cluster_rename_hostnames() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    refresh_cluster_kv_from_local_daemon(&paths)?;
    let mut state = read_cluster_state(&paths)?;
    let local_hostname = local_hostname();
    let statuses = match cluster::read_cluster_node_statuses_from_kv(&paths) {
        Ok(statuses) => statuses,
        Err(error) => {
            eprintln!("warning: cluster node-status hostname lookup skipped: {error}");
            Vec::new()
        }
    };
    let targets = cluster_hostname_rename_targets(&state, local_hostname.as_deref(), &statuses);
    if targets.is_empty() {
        println!("no cluster member hostnames could be inferred");
        return Ok(());
    }

    let mut changed_count = 0usize;
    let mut unchanged_count = 0usize;
    for target in targets {
        let update = cluster::update_cluster_node_name_with_host(
            &paths,
            state,
            target.node_id,
            target.name,
            OffsetDateTime::now_utc(),
            &mut CliClusterMemberMutationHost,
        )?;
        if update.changed {
            changed_count = changed_count.saturating_add(1);
            println!(
                "cluster member {} renamed: {} -> {}",
                update.node_id, update.previous_name, update.name
            );
        } else {
            unchanged_count = unchanged_count.saturating_add(1);
            println!(
                "cluster member {} already has name {}",
                update.node_id, update.name
            );
        }
        state = cluster::read_cluster_state(&paths)?;
    }
    println!("hostname rename summary: {changed_count} changed, {unchanged_count} unchanged");
    if changed_count > 0 {
        nudge_local_daemon_cluster_activation();
    }
    Ok(())
}

pub(crate) struct CliClusterMemberMutationHost;

impl cluster::ClusterMemberMutationHost for CliClusterMemberMutationHost {
    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, network::NetworkError> {
        // `state` can already contain the proposed membership mutation. Raft
        // authority must be established with the currently persisted voter set,
        // never with the configuration that is still being validated.
        let mut consensus_state = cluster::read_cluster_state_without_kv(paths)?;
        let mut driver = cli_cluster_control_plane_driver(paths);
        let read = cluster::read_cluster_kv_authoritative_snapshot_with_host(
            paths,
            &mut consensus_state,
            cli_cluster_kv_txn_quorum_options(true),
            &mut driver,
        )?;
        state.term = state.term.max(consensus_state.term);
        state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
        Ok(read)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, network::NetworkError> {
        let mut consensus_state = cluster::read_cluster_state_without_kv(paths)?;
        let result = cli_commit_cluster_kv_request_with_quorum_response(
            paths,
            &mut consensus_state,
            request,
        )?;
        state.term = state.term.max(consensus_state.term);
        state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
        Ok(result)
    }
}

/// `sinomo cluster role`: daemon-required — the node-role change commits
/// inside the running daemon through `/cluster/control/role`.
pub(crate) fn cluster_role(options: ClusterRoleOptions) -> Result<(), CliError> {
    let client = cluster_control_daemon_client("role <node-id> <role>")?;
    let update = client.cluster_role(cluster::ClusterControlRoleRequest {
        node_id: options.node_id,
        role: options.role,
    })?;
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

pub(crate) fn cluster_state_authority_role(
    options: ClusterStateAuthorityOptions,
) -> Result<(), CliError> {
    let client = cluster_control_daemon_client("state-authority <node-id> <role>")?;
    let update = client.cluster_state_authority(cluster::ClusterControlStateAuthorityRequest {
        node_id: options.node_id,
        role: options.role,
    })?;
    if !update.changed {
        println!(
            "cluster member {} already has state authority role {}",
            update.node_id,
            format_member_role(&update.role)
        );
        return Ok(());
    }

    println!(
        "cluster member {} state authority role changed: {} {} -> {} {}",
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
    let explicit_endpoint = explicit_cluster_repair_endpoint(&options.from);
    let peer_status = if let Some(endpoint) = explicit_endpoint.as_ref() {
        daemon_fetch_daemon_status_from_endpoint(endpoint, DAEMON_JOIN_TIMEOUT)?
    } else {
        daemon_fetch_daemon_status_from_address(&options.from, DAEMON_JOIN_TIMEOUT)?
    };
    let peer_snapshot = if let Some(endpoint) = explicit_endpoint.as_ref() {
        daemon_fetch_cluster_kv_authoritative_snapshot_from_endpoint(endpoint, DAEMON_JOIN_TIMEOUT)?
            .value
    } else {
        daemon_fetch_cluster_kv_authoritative_snapshot(&options.from, DAEMON_JOIN_TIMEOUT)?.value
    };
    let mut imported = Vec::new();
    let mut endpoint_updates = Vec::new();
    for mut member in cluster::read_cluster_members_from_snapshot(&peer_snapshot)? {
        if member.cluster_id != state.cluster_id {
            continue;
        }
        if let Some(existing) = state
            .members
            .iter_mut()
            .find(|candidate| candidate.node_id == member.node_id)
        {
            let mut merged_endpoints = 0usize;
            for endpoint in member.endpoints {
                if merge_cluster_member_endpoint(existing, endpoint) {
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
        let observed_endpoint = observed_cluster_repair_endpoint(&cluster.node_id, &options.from);
        if let Some(existing) = state
            .members
            .iter_mut()
            .find(|candidate| candidate.node_id == observed_endpoint.node_id)
            && merge_cluster_member_endpoint(existing, observed_endpoint)
        {
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
    let maintenance_endpoint = explicit_endpoint.as_ref().map(|endpoint| {
        peer_status
            .cluster
            .as_ref()
            .filter(|cluster| cluster.cluster_id == state.cluster_id)
            .map(|cluster| observed_cluster_repair_endpoint(&cluster.node_id, &options.from))
            .unwrap_or_else(|| endpoint.clone())
    });
    if let Some(endpoint) = maintenance_endpoint.as_ref()
        && let Err(error) = crate::daemon_send_daemon_cluster_maintenance_refresh_to_endpoint(
            endpoint,
            DAEMON_JOIN_TIMEOUT,
        )
    {
        eprintln!(
            "warning: peer cluster maintenance refresh through {} failed: {error}",
            options.from
        );
    }

    if let Some(node_id) = options.promote {
        let state = cluster::read_cluster_state(&paths)?;
        let update = cluster::update_cluster_state_authority_with_host(
            &paths,
            state,
            node_id,
            ClusterMemberRole::Voter,
            OffsetDateTime::now_utc(),
            &mut CliClusterMemberMutationHost,
        )?;
        if update.changed {
            println!(
                "cluster member {} state authority role changed: {} {} -> {} {}",
                update.node_id,
                format_member_role(&update.previous_role),
                format_member_state(&update.previous_state),
                format_member_role(&update.role),
                format_member_state(&update.state)
            );
        } else {
            println!(
                "cluster member {} already has state authority role {}",
                update.node_id,
                format_member_role(&update.role)
            );
        }
    }
    Ok(())
}

fn explicit_cluster_repair_endpoint(address: &str) -> Option<ClusterEndpointRecord> {
    if !address.starts_with("iroh://") {
        return None;
    }
    Some(cluster::invite_introducer_endpoint(
        &NodeId::from_legacy_named("node_cluster_repair_explicit_peer")
            .expect("static cluster repair peer node id"),
        address.to_string(),
    ))
}

fn observed_cluster_repair_endpoint(node_id: &NodeId, address: &str) -> ClusterEndpointRecord {
    if address.starts_with("iroh://") {
        cluster::invite_introducer_endpoint(node_id, address.to_string())
    } else {
        network::daemon_lan_endpoint(node_id, address.to_string(), 0)
    }
}

fn merge_cluster_member_endpoint(
    member: &mut ClusterMemberRecord,
    endpoint: ClusterEndpointRecord,
) -> bool {
    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            false
        } else {
            *existing = endpoint;
            true
        }
    } else if let Some(existing) = member.endpoints.iter_mut().find(|candidate| {
        candidate.transport == endpoint.transport && candidate.address == endpoint.address
    }) {
        if *existing == endpoint {
            false
        } else {
            *existing = endpoint;
            true
        }
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

/// `sinomo cluster remove`: daemon-required — member removal commits inside
/// the running daemon through `/cluster/control/remove`. The direct removal
/// machinery stays reachable through `cluster cleanup-legacy-shadow-members`.
pub(crate) fn cluster_remove(options: ClusterRemoveOptions) -> Result<(), CliError> {
    let client = cluster_control_daemon_client("remove <node-id>")?;
    let removal = client.cluster_remove(cluster::ClusterControlRemoveRequest {
        node_ids: options.node_ids,
    })?;
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
    let state = cluster::read_cluster_state(&paths)?;
    let removal = cluster::cleanup_legacy_shadow_daemon_members_with_host(
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
            paths,
            &mut state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        )?;
        cluster::write_cluster_state(paths, &state)?;
    }
    Ok(state.members)
}

/// `sinomo cluster leave`: daemon-required — the daemon quarantines the old
/// cluster storage and re-initializes a fresh single-node cluster through
/// `/cluster/control/leave`.
pub(crate) fn cluster_leave() -> Result<(), CliError> {
    let client = cluster_control_daemon_client("leave")?;
    let response = client.cluster_leave()?;
    match &response.previous_cluster_id {
        Some(cluster_id) => println!("cluster left: {cluster_id}"),
        None => println!("cluster left: no previous local cluster state"),
    }
    println!("single-node cluster: {}", response.new_cluster_id);
    println!("local node: {}", response.node_id);
    println!("state: {}", cluster_paths_from_defaults()?.state.display());
    print_daemon_cluster_identity_activation_notice();
    Ok(())
}

fn print_daemon_cluster_identity_activation_notice() {
    println!(
        "daemon activation: the main daemon is stopping so its supervisor can restart with the new cluster identity"
    );
}

fn nudge_local_daemon_cluster_activation() {
    if let Ok(Some(status)) = default_main_daemon_status() {
        let _ = crate::daemon_send_daemon_cluster_maintenance_refresh(
            &status.address,
            DAEMON_PROBE_TIMEOUT,
        );
    }
}

pub(crate) fn cluster_leave_with_paths(
    paths: &ClusterPaths,
) -> Result<ClusterLeaveReport, CliError> {
    paths.ensure()?;
    let existing = match cluster::read_cluster_state_without_kv(paths) {
        Ok(state) => Some(state),
        Err(network::NetworkError::ClusterOrphan) => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(mut consensus_state) = existing.clone() {
        cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut CliClusterMemberMutationHost,
            paths,
            &mut consensus_state,
        )?;
    }
    let leave_id = Id::generate();
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
        .map(str::to_string)
        .unwrap_or_else(|| default_cluster_node_name("sinomo-local"));

    let quarantine = if existing.is_some() || cluster::cluster_storage_has_live_artifacts(paths) {
        Some(cluster::quarantine_cluster_storage_for_leave(
            paths, &leave_id,
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
                cluster::rollback_cluster_storage_quarantine(&quarantine)?;
            }
            Err(error)
        }
    }
}

pub(crate) fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, CliError> {
    let mut state = cluster::read_cluster_state(paths)?;
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
    // The daemon owns local Raft. Consume its current-term snapshot directly
    // and materialize that verified projection for CLI readers; once a live
    // daemon is discovered, an RPC failure stays fail-closed instead of
    // falling through to a second authority path.
    let snapshot =
        daemon_fetch_cluster_kv_authoritative_snapshot(&status.address, DAEMON_PROBE_TIMEOUT)?;
    install_daemon_authoritative_cluster_kv_snapshot(paths, snapshot)
}

/// Like [`refresh_cluster_kv_from_local_daemon`], but probes the daemon that
/// owns `data_dir` instead of the env/home default. A caller operating on a
/// foreign data directory must never import another cluster's KV snapshot
/// (and, transitively, its peer targets) into its own paths.
pub(crate) fn refresh_cluster_kv_from_local_daemon_for_data_dir(
    data_dir: &SinomoDataDir,
    paths: &ClusterPaths,
) -> Result<(), CliError> {
    let status = match probe_daemon_status(&DaemonPaths::new(data_dir), DAEMON_PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => status,
        DaemonProbeStatus::Locked { .. } => {
            return Err(CliError::DaemonNotRunning {
                detail: "main daemon owns this data directory but its RPC service did not answer; refusing direct cluster-KV access while Raft may be active".to_string(),
            });
        }
        DaemonProbeStatus::NotRunning => return refresh_cluster_kv_from_reachable_voters(paths),
    };
    let snapshot =
        daemon_fetch_cluster_kv_authoritative_snapshot(&status.address, DAEMON_PROBE_TIMEOUT)?;
    install_daemon_authoritative_cluster_kv_snapshot(paths, snapshot)
}

fn install_daemon_authoritative_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: KvAuthoritativeRead<KvSnapshot>,
) -> Result<(), CliError> {
    let wrote = sinomo_runtime::kv::write_cluster_kv_snapshot_if_changed(
        &paths.kv_paths(),
        &snapshot.value,
    )?;
    if wrote {
        cluster::invalidate_cluster_kv_observed_read_models(paths);
    }
    Ok(())
}

struct CliClusterKvSnapshotRefreshHost {
    paths: ClusterPaths,
}

impl cluster::ClusterKvSnapshotRefreshHost for CliClusterKvSnapshotRefreshHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, network::NetworkError> {
        cluster_kv_state_authority_id(paths, state).or(Ok(None))
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<network::ObjectRpcResponse, network::NetworkError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| network::NetworkError::Io {
                operation: "cluster kv snapshot refresh runtime".to_string(),
                detail: source.to_string(),
            })?;
        let mut host = CliEndpointRpcHost::new(
            Some(state.cluster_id.clone()),
            "cluster kv snapshot refresh signing",
        );
        let mut client = network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(
            &mut host,
            &self.paths,
            state,
        )?;
        let result = runtime.block_on(async {
            let result = client
                .call_target(
                    state,
                    target_node_id,
                    network::ObjectRpcCall::value(request.clone()),
                    cluster::main_daemon_cluster_rpc_options(
                        network::ClusterObjectRpcOptions::default().with_overall_timeout(timeout),
                    ),
                )
                .await?;
            result.into_materialized_value_response(usize::MAX).await
        });
        runtime.shutdown_timeout(Duration::from_millis(250));
        result
    }

    fn cache_cluster_kv_snapshot_if_newer_than_raft(
        &mut self,
        _paths: &ClusterPaths,
        _local_node_id: &NodeId,
        _snapshot: KvSnapshot,
    ) -> Result<bool, network::NetworkError> {
        // `refresh_current_cluster_kv_store_with_host` has already persisted
        // the imported materialized projection before invoking this hook. A
        // CLI process does not own the resident Raft node and must never open
        // it merely to arbitrate a read snapshot; the daemon will reconcile
        // its own Raft-backed projection on its next committed edge.
        Ok(false)
    }
}

fn refresh_cluster_kv_from_reachable_voters(paths: &ClusterPaths) -> Result<(), CliError> {
    let Ok(state) = read_cluster_state_with_materialized_member_overlay(paths) else {
        return Ok(());
    };
    match cluster::refresh_current_cluster_kv_store_with_host(
        paths,
        &state,
        cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_snapshot_refresh_options(),
        &mut CliClusterKvSnapshotRefreshHost {
            paths: paths.clone(),
        },
    ) {
        Ok(_) | Err(network::NetworkError::ClusterReadOnly { .. }) => Ok(()),
        Err(network::NetworkError::Io { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn read_cluster_state_with_materialized_member_overlay(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, CliError> {
    let mut state = cluster::read_cluster_state_without_kv(paths)?;
    let Some(snapshot) = paths
        .kv_paths()
        .local_cache()
        .cached_snapshot_arc_if_present()?
    else {
        return Ok(state);
    };
    let mut projected_members = cluster::read_cluster_members_from_snapshot(&snapshot)?;
    if projected_members.is_empty() {
        return Ok(state);
    }

    // KV is authoritative for member role/state, while the bootstrap file can
    // retain a reachable endpoint learned before the latest materialization.
    // Preserve those endpoints without consulting the daemon-owned Raft node.
    for local_member in &state.members {
        if let Some(projected) = projected_members
            .iter_mut()
            .find(|candidate| candidate.node_id == local_member.node_id)
        {
            for endpoint in &local_member.endpoints {
                merge_cluster_member_endpoint(projected, endpoint.clone());
            }
        } else {
            projected_members.push(local_member.clone());
        }
    }
    projected_members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    state.members = projected_members;
    network::cleanup_cluster_endpoint_duplicates(&mut state);
    Ok(state)
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_store(
    paths: &ClusterPaths,
) -> Result<sinomo_runtime::kv::InProcessKvStore, CliError> {
    let state = cluster::read_cluster_state_without_kv(paths)?;
    sinomo_runtime::kv::read_cluster_kv_store_for_node(&paths.kv_paths(), state.local_node_id)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_local_snapshot_for_test(
    paths: &ClusterPaths,
) -> Result<KvSnapshot, CliError> {
    match sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
        .map_err(sinomo_runtime::network::NetworkError::directory)
    {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => {
            eprintln!("warning: ignoring unreadable cluster kv snapshot: {error}");
            read_cluster_kv_store(paths)
                .and_then(|store| store.snapshot().map_err(Into::into))
                .or_else(|_| {
                    sinomo_runtime::kv::InProcessKvStore::new()
                        .snapshot()
                        .map_err(Into::into)
                })
        }
    }
}

pub(crate) fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), CliError> {
    sinomo_runtime::kv::write_cluster_kv_snapshot_if_changed(&paths.kv_paths(), snapshot)
        .map(|_| ())
        .map_err(sinomo_runtime::network::NetworkError::directory)
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
    cluster::sync_cluster_state_to_kv_with_host(paths, state, &mut CliClusterMemberMutationHost)
        .map_err(Into::into)
}

struct CliClusterInvitePublishHost;

impl cluster::ClusterInvitePublishHost for CliClusterInvitePublishHost {
    type Error = CliError;

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, Self::Error> {
        cluster::ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(
            &mut CliClusterMemberMutationHost,
            paths,
            state,
        )
        .map_err(Into::into)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvTxnCommitResult, Self::Error> {
        cluster::ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut CliClusterMemberMutationHost,
            paths,
            state,
            request,
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
        &paths,
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    )?;
    cluster::write_cluster_state(&paths, &state)?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| CliError::Parse {
        detail: "daemon cluster member could not be registered".to_string(),
    })
}

pub(crate) fn daemon_cluster_status_from_local_state(
    paths: &ClusterPaths,
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, CliError> {
    let mut state = cluster::read_cluster_state_without_kv(paths)?;
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
        ClusterMemberRole::Voter => "authority",
        ClusterMemberRole::Learner => "replica",
        ClusterMemberRole::NonVoting => "observer",
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
        ClusterMemberState::Learner => "replica",
        ClusterMemberState::Voter => "authority",
        ClusterMemberState::NonVoting => "observer",
        ClusterMemberState::Suspect => "suspect",
        ClusterMemberState::Offline => "offline",
        ClusterMemberState::Removed => "removed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_authoritative_snapshot_refresh_materializes_newer_connectivity() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-daemon-snapshot-refresh-connectivity");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cli_daemon_snapshot_refresh").expect("cluster");
        let source_node_id =
            NodeId::from_legacy_named("node_cli_daemon_snapshot_source").expect("source");
        let target_node_id =
            NodeId::from_legacy_named("node_cli_daemon_snapshot_target").expect("target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "CLI daemon snapshot refresh",
            source_node_id.clone(),
            "local",
            "public-key",
            now,
        );
        cluster::write_cluster_state(&paths, &state).expect("cluster state");
        let endpoint_id = sinomo_runtime::network::ClusterEndpointId::from_legacy_named(
            "endpoint_cli_daemon_snapshot_refresh",
        )
        .expect("endpoint");
        let stale = sinomo_runtime::network::ClusterConnectivityRecord {
            cluster_id,
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            endpoint_id: endpoint_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.40:21353".to_string(),
            state: sinomo_runtime::network::ClusterConnectivityState::Unreachable,
            latency_ms: None,
            consecutive_failures: 3,
            last_success_at: None,
            last_failure_at: Some("stale".to_string()),
            failure_reason: Some(
                sinomo_runtime::network::ClusterConnectivityFailureReason::IdentityMismatch,
            ),
            observed_at: now,
            expires_at: now + time::Duration::hours(2),
        };
        let key = cluster::cluster_connectivity_key(&source_node_id, &target_node_id, &endpoint_id);
        let mut store = sinomo_runtime::kv::InProcessKvStore::new();
        store
            .put_serde(key.clone(), &stale, None)
            .expect("stale connectivity");
        let stale_snapshot = store.try_snapshot().expect("stale snapshot");
        sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &stale_snapshot)
            .expect("write stale materialized snapshot");

        let healthy = sinomo_runtime::network::ClusterConnectivityRecord {
            state: sinomo_runtime::network::ClusterConnectivityState::Healthy,
            latency_ms: Some(36),
            consecutive_failures: 0,
            last_success_at: Some("fresh".to_string()),
            last_failure_at: None,
            failure_reason: None,
            observed_at: now + time::Duration::seconds(1),
            expires_at: now + time::Duration::minutes(15),
            ..stale
        };
        store
            .put_serde(key.clone(), &healthy, None)
            .expect("healthy connectivity");
        let authoritative_snapshot = store.try_snapshot().expect("authoritative snapshot");
        let authoritative = KvAuthoritativeRead {
            authority: sinomo_runtime::kv::KvAuthorityProof {
                term: 1,
                commit_index: 2,
                revision: authoritative_snapshot.revision,
                basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            value: authoritative_snapshot.clone(),
        };

        // A durable-state marker makes any accidental local Raft open inspect
        // this node. A directory at the lock path then fails deterministically;
        // the daemon snapshot installer must touch only the materialized cache.
        let wal_paths = paths.kv_paths().raft_wal_paths(&source_node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("Raft root");
        std::fs::write(&wal_paths.wal, b"").expect("durable-state marker");
        std::fs::create_dir(&wal_paths.lock).expect("inaccessible node lock sentinel");

        install_daemon_authoritative_cluster_kv_snapshot(&paths, authoritative)
            .expect("install daemon authoritative snapshot");

        let materialized = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .expect("materialized snapshot");
        assert_eq!(materialized.revision, authoritative_snapshot.revision);
        let record = materialized
            .get(&key)
            .expect("materialized connectivity")
            .decode::<sinomo_runtime::network::ClusterConnectivityRecord>()
            .expect("decode connectivity");
        assert_eq!(
            record.state,
            sinomo_runtime::network::ClusterConnectivityState::Healthy
        );
        assert_eq!(record.latency_ms, Some(36));
        assert_eq!(record.failure_reason, None);

        let stale_authoritative = KvAuthoritativeRead {
            authority: sinomo_runtime::kv::KvAuthorityProof {
                term: 1,
                commit_index: 1,
                revision: stale_snapshot.revision,
                basis: sinomo_runtime::kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks: 2,
                    required_quorum: 2,
                },
            },
            value: stale_snapshot,
        };
        install_daemon_authoritative_cluster_kv_snapshot(&paths, stale_authoritative)
            .expect("ignore older daemon authoritative snapshot");
        let retained = sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
            .expect("retained materialized snapshot");
        assert_eq!(retained.revision, authoritative_snapshot.revision);
        let retained_record = retained
            .get(&key)
            .expect("retained connectivity")
            .decode::<sinomo_runtime::network::ClusterConnectivityRecord>()
            .expect("decode retained connectivity");
        assert_eq!(
            retained_record.state,
            sinomo_runtime::network::ClusterConnectivityState::Healthy
        );
    }

    #[test]
    fn cli_snapshot_refresh_host_never_opens_local_raft_storage() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-snapshot-refresh-no-raft-open");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_cli_snapshot_no_raft_open").expect("cluster id"),
            "CLI snapshot no Raft open",
            NodeId::from_legacy_named("node_cli_snapshot_no_raft_open").expect("node id"),
            "local",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        cluster::write_cluster_state(&paths, &state).expect("cluster state");
        let snapshot = sinomo_runtime::kv::InProcessKvStore::new()
            .try_snapshot()
            .expect("KV snapshot");
        sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &snapshot)
            .expect("materialized snapshot");

        // A durable-state marker makes a latest-observed read try the Raft
        // path. Making node.lock a directory turns any such attempt into a
        // deterministic error without relying on timing or another process.
        let wal_paths = paths.kv_paths().raft_wal_paths(&state.local_node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("Raft root");
        std::fs::write(&wal_paths.wal, b"").expect("durable-state marker");
        std::fs::create_dir(&wal_paths.lock).expect("inaccessible node lock sentinel");

        let mut host = CliClusterKvSnapshotRefreshHost {
            paths: paths.clone(),
        };
        assert!(
            !cluster::ClusterKvSnapshotRefreshHost::cache_cluster_kv_snapshot_if_newer_than_raft(
                &mut host,
                &paths,
                &state.local_node_id,
                snapshot.clone(),
            )
            .expect("CLI snapshot cache hook"),
            "the generic refresh already persisted the imported projection"
        );
        assert_eq!(
            sinomo_runtime::kv::read_cluster_kv_cached_snapshot(&paths.kv_paths())
                .expect("materialized snapshot remains readable"),
            snapshot
        );
    }

    #[test]
    fn daemon_health_state_overlay_never_opens_local_raft_storage() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-health-state-no-raft-open");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_cli_health_state_no_raft_open")
                .expect("cluster id"),
            "CLI health state no Raft open",
            NodeId::from_legacy_named("node_cli_health_state_no_raft_open").expect("node id"),
            "local",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        cluster::write_cluster_state(&paths, &state).expect("cluster state");
        let snapshot = sinomo_runtime::kv::InProcessKvStore::new()
            .try_snapshot()
            .expect("KV snapshot");
        sinomo_runtime::kv::write_cluster_kv_snapshot(&paths.kv_paths(), &snapshot)
            .expect("materialized snapshot");

        let wal_paths = paths.kv_paths().raft_wal_paths(&state.local_node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("Raft root");
        std::fs::write(&wal_paths.wal, b"").expect("durable-state marker");
        std::fs::create_dir(&wal_paths.lock).expect("inaccessible node lock sentinel");

        let observed = read_cluster_state_with_materialized_member_overlay(&paths)
            .expect("materialized health state");
        assert_eq!(observed.cluster_id, state.cluster_id);
        assert_eq!(observed.local_node_id, state.local_node_id);
    }

    #[test]
    fn cluster_doctor_summary_is_ok_for_info_only_observations() {
        let info = ClusterDoctorFinding {
            severity: "info".to_string(),
            code: "node-offline".to_string(),
            detail: "offline member is tolerated by quorum".to_string(),
        };
        assert_eq!(
            cluster_doctor_summary_label(std::slice::from_ref(&info)),
            "ok"
        );

        let warning = ClusterDoctorFinding {
            severity: "warning".to_string(),
            ..info
        };
        assert_eq!(cluster_doctor_summary_label(&[warning]), "warnings");
    }

    #[test]
    fn cluster_doctor_state_read_does_not_open_local_raft_storage() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-doctor-no-raft-open");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_cli_doctor_no_raft_open").expect("cluster id"),
            "CLI doctor no Raft open",
            NodeId::from_legacy_named("node_cli_doctor_no_raft_open").expect("node id"),
            "local",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        cluster::write_cluster_state(&paths, &state).expect("cluster state");

        // A durable marker plus an invalid lock path makes opening the local
        // Raft node fail deterministically. Doctor must still be able to read
        // its bounded bootstrap observation state.
        let wal_paths = paths.kv_paths().raft_wal_paths(&state.local_node_id);
        std::fs::create_dir_all(&wal_paths.root).expect("Raft root");
        std::fs::write(&wal_paths.wal, b"").expect("durable-state marker");
        std::fs::create_dir(&wal_paths.lock).expect("inaccessible node lock sentinel");

        let observed = read_cluster_doctor_state(&paths).expect("observational doctor state");
        assert_eq!(observed.cluster_id, state.cluster_id);
        assert_eq!(observed.local_node_id, state.local_node_id);
    }

    #[test]
    fn cluster_doctor_warns_when_an_online_node_fails_the_dfs_probe() {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_cli_dfs_gate").expect("cluster id");
        let online_node =
            NodeId::from_legacy_named("node_cli_dfs_gate_online").expect("online node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "cli DFS gate",
            online_node.clone(),
            "online",
            "public-key",
            now,
        );
        let online_member = state.members[0].clone();
        let mut offline_member = online_member.clone();
        offline_member.node_id =
            NodeId::from_legacy_named("node_cli_dfs_gate_offline").expect("offline node id");
        state.members.push(offline_member.clone());
        let row = |member: ClusterMemberRecord, availability: ClusterNodeAvailability| {
            let mut status = cluster::unknown_cluster_node_status(&state, &member, now);
            status.availability = availability.clone();
            ClusterHealthRow {
                member,
                status,
                availability,
                live_probe_observed: true,
            }
        };
        let rows = vec![
            row(online_member, ClusterNodeAvailability::Online),
            row(offline_member.clone(), ClusterNodeAvailability::Offline),
        ];
        let report = DfsClusterStatusReport {
            nodes: vec![
                DfsClusterNodeStatus {
                    node_id: online_node,
                    local: true,
                    endpoint: None,
                    status: None,
                    error: Some("no healthy route".to_string()),
                },
                DfsClusterNodeStatus {
                    node_id: offline_member.node_id,
                    local: false,
                    endpoint: None,
                    status: None,
                    error: Some("offline".to_string()),
                },
            ],
        };

        let findings = dfs_reachability_findings(&rows, &report);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, "dfs-route-unavailable");
        assert!(findings[0].detail.contains("no healthy route"));
    }

    #[test]
    fn cluster_doctor_warns_when_no_dfs_provider_is_discovered() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cli_no_dfs_provider").expect("cluster id");
        let node_id = NodeId::from_legacy_named("node_cli_no_dfs_provider").expect("node id");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "cli no DFS provider",
            node_id,
            "online",
            "public-key",
            now,
        );
        let member = state.members[0].clone();
        let mut status = cluster::unknown_cluster_node_status(&state, &member, now);
        status.availability = ClusterNodeAvailability::Online;
        let health = vec![ClusterHealthRow {
            member,
            status,
            availability: ClusterNodeAvailability::Online,
            live_probe_observed: true,
        }];

        let findings =
            dfs_reachability_findings(&health, &DfsClusterStatusReport { nodes: Vec::new() });

        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, "dfs-provider-unavailable");
    }

    #[test]
    fn local_main_probe_distinguishes_down_from_unresponsive() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cli_local_health").expect("cluster id");
        let node_id = NodeId::from_legacy_named("node_cli_local_health").expect("node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "cli local health",
            node_id,
            "local",
            "public-key",
            now,
        );
        state.members[0].capabilities = vec![
            sinomo_runtime::core::CapabilityName::new(
                sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY,
            )
            .expect("main capability"),
        ];
        let member = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .expect("local main member");

        assert_eq!(
            local_main_daemon_availability_from_probe(
                &state,
                member,
                &DaemonProbeStatus::NotRunning,
            ),
            ClusterNodeAvailability::Offline
        );
        assert_eq!(
            local_main_daemon_availability_from_probe(
                &state,
                member,
                &DaemonProbeStatus::Locked { status: None },
            ),
            ClusterNodeAvailability::Unknown
        );
    }

    #[test]
    fn local_main_availability_override_preserves_remote_rows() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_cli_health_override").expect("cluster id");
        let node_id = NodeId::from_legacy_named("node_cli_health_override").expect("node id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "cli health override",
            node_id,
            "local",
            "public-key",
            now,
        );
        state.members[0].capabilities = vec![
            sinomo_runtime::core::CapabilityName::new(
                sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY,
            )
            .expect("main capability"),
        ];
        let local_member = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
            .expect("local main member")
            .clone();
        let mut remote_member = local_member.clone();
        remote_member.node_id =
            NodeId::from_legacy_named("node_cli_health_override_remote").expect("remote node");
        let online_row = |member: ClusterMemberRecord| {
            let mut status = cluster::unknown_cluster_node_status(&state, &member, now);
            status.availability = ClusterNodeAvailability::Online;
            ClusterHealthRow {
                member,
                status,
                availability: ClusterNodeAvailability::Online,
                live_probe_observed: false,
            }
        };
        let mut rows = vec![online_row(local_member), online_row(remote_member.clone())];

        apply_authoritative_local_main_availability(
            &state,
            &mut rows,
            ClusterNodeAvailability::Offline,
        );

        let local = rows
            .iter()
            .find(|row| row.member.node_id == state.local_node_id)
            .expect("local row");
        assert_eq!(local.availability, ClusterNodeAvailability::Offline);
        assert_eq!(local.status.availability, ClusterNodeAvailability::Offline);
        let remote = rows
            .iter()
            .find(|row| row.member.node_id == remote_member.node_id)
            .expect("remote row");
        assert_eq!(remote.availability, ClusterNodeAvailability::Online);
        assert_eq!(remote.status.availability, ClusterNodeAvailability::Online);
    }

    #[test]
    fn repair_member_endpoint_replaces_same_address_with_canonical_identity() {
        crate::tests::install_test_iroh_network_module();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_repair_same_address").expect("cluster id");
        let main_node =
            NodeId::from_legacy_named("node_repair_same_address_main").expect("main node");
        let shadow_node = shadow_daemon_node_id(&main_node);
        let now = OffsetDateTime::now_utc();
        let address = "iroh://repair_same_address?relay=https://relay.example./".to_string();
        let shadow_endpoint = cluster::invite_introducer_endpoint(&shadow_node, address.clone());
        let main_endpoint = cluster::invite_introducer_endpoint(&main_node, address);
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id,
            main_node,
            "remote main",
            "remote-key",
            now,
        );
        member.endpoints.push(shadow_endpoint);

        assert!(merge_cluster_member_endpoint(
            &mut member,
            main_endpoint.clone()
        ));
        assert_eq!(member.endpoints, vec![main_endpoint]);
        assert!(network::cluster_endpoint_can_carry_main_daemon_rpc(
            &member.endpoints[0]
        ));
    }
}
