use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ClusterCliCommand {
    Help,
    Init(ClusterInitOptions),
    Invite(ClusterInviteOptions),
    Join(ClusterJoinOptions),
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
    paths.ensure()?;
    if paths.state.exists() {
        return Err(CliError::Parse {
            detail: format!(
                "cluster is already initialized at {}; use `sinomo cluster status`",
                paths.state.display()
            ),
        });
    }

    let local_node_id = options.node_id.unwrap_or_else(NodeId::generate);
    let timestamp = OffsetDateTime::now_utc();
    let cluster_id = options.cluster_id.unwrap_or_else(|| {
        ClusterId::new(format!(
            "cluster_{}",
            local_node_id.as_str().trim_start_matches("node_")
        ))
        .expect("generated cluster id")
    });
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let mut state = ClusterBootstrapState::initialized_locally(
        cluster_id,
        options.name.clone(),
        local_node_id.clone(),
        options.name,
        main_identity.public_key.clone(),
        timestamp,
    );
    ensure_daemon_cluster_members(
        &mut state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        timestamp,
    );
    assign_local_node_public_keys(paths, &mut state)?;
    write_json(&paths.state, &state)?;
    network::set_cluster_raft_leader(paths, &state.local_node_id, 1)?;
    sync_cluster_state_to_kv(paths, &state)?;
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
    println!("saved: {}", rendered.persisted_path.display());
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
    pub(crate) persisted_path: PathBuf,
    pub(crate) output_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ClusterInvitePublishRequest {
    pub(crate) invite: ClusterJoinInvite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ClusterInvitePublishResponse {
    pub(crate) accepted: bool,
    pub(crate) detail: String,
    pub(crate) state: ClusterBootstrapState,
    pub(crate) kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ClusterMemberPublishRequest {
    pub(crate) members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ClusterMemberPublishResponse {
    pub(crate) accepted: bool,
    pub(crate) detail: String,
    pub(crate) state: ClusterBootstrapState,
    pub(crate) kv_snapshot: Option<KvSnapshot>,
}

pub(crate) fn cluster_invite_with_paths(
    options: ClusterInviteOptions,
    paths: &ClusterPaths,
) -> Result<RenderedClusterInvite, CliError> {
    paths.ensure()?;
    fs::create_dir_all(&paths.invites).map_err(|source| CliError::Io {
        path: paths.invites.display().to_string(),
        detail: source.to_string(),
    })?;
    let state = read_cluster_state_without_kv(paths)?;
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
    let persisted_path = paths.invite_path(&invite.invite_id);
    write_json(&persisted_path, &invite)?;
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
        invite,
        uri,
        join_command,
        persisted_path,
        output_path: options.output,
    })
}

pub(crate) fn publish_cluster_invitation(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
) -> Result<(), CliError> {
    match publish_cluster_invitation_to_quorum(paths, state, invite) {
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
    _paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut seen_nodes = std::collections::BTreeSet::new();
    let mut targets = Vec::new();
    for endpoint in cluster_sync_targets(state) {
        if !cluster_raft_node_is_voter(state, &endpoint.node_id) {
            continue;
        }
        if !seen_nodes.insert(endpoint.node_id.clone()) {
            continue;
        }
        targets.push(endpoint);
    }
    targets
}

pub(crate) fn publish_local_members_via_voter(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), CliError> {
    let members: Vec<_> = state
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(state, member))
        .cloned()
        .collect();
    if members.is_empty() {
        return Ok(());
    }
    let request = ClusterMemberPublishRequest { members };
    let mut failures = Vec::new();
    for endpoint in cluster_sync_targets_with_connectivity(paths, state)
        .into_iter()
        .filter(|endpoint| cluster_raft_node_is_voter(state, &endpoint.node_id))
    {
        match send_cluster_member_publish(&endpoint, &request) {
            Ok(response) if response.accepted => {
                if let Some(snapshot) = response.kv_snapshot {
                    write_cluster_kv_snapshot(paths, &snapshot)?;
                }
                return Ok(());
            }
            Ok(response) => failures.push(format!(
                "{} ({:?}) rejected member publish: {}",
                endpoint.address, endpoint.transport, response.detail
            )),
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                endpoint.address, endpoint.transport, error
            )),
        }
    }
    Err(CliError::ClusterReadOnly {
        detail: format!(
            "local node is not a voter and no reachable voter accepted member publication{}",
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        ),
    })
}

pub(crate) fn publish_cluster_invitation_to_quorum(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
) -> Result<(), CliError> {
    let invitation_state = network::ClusterInvitationStateRecord {
        cluster_id: invite.cluster_id.clone(),
        invite_id: invite.invite_id.clone(),
        state: "active".to_string(),
        issuer_node_id: invite.issuer_node_id.clone(),
        expires_at: invite.expires_at,
        updated_at: OffsetDateTime::now_utc(),
    };
    let mut commit_state = state.clone();
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut commit_state,
        KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::put_serde(
                    network::cluster_invitation_key(&invite.invite_id),
                    invite,
                    None,
                )?,
                KvOp::put_serde(
                    network::cluster_invitation_state_key(&invite.invite_id),
                    &invitation_state,
                    None,
                )?,
            ],
            failure: Vec::new(),
        },
    )?;
    Ok(())
}

pub(crate) fn cluster_member_publish_txn(
    members: &[ClusterMemberRecord],
) -> Result<KvTxn, CliError> {
    let mut success = Vec::new();
    for member in members {
        let mut member = member.clone();
        network::cleanup_member_iroh_endpoints(&mut member);
        success.push(KvOp::put_serde(
            cluster_member_key(&member.node_id),
            &member,
            None,
        )?);
        success.push(KvOp::put_serde(
            network::cluster_capability_policy_key(&member.node_id),
            &network::ClusterCapabilityPolicyRecord::from(&member),
            None,
        )?);
        success.push(KvOp::DeletePrefix {
            prefix: network::cluster_endpoint_prefix(&member.node_id),
        });
        for endpoint in &member.endpoints {
            success.push(KvOp::put_serde(
                network::cluster_endpoint_key(&member.node_id, endpoint),
                endpoint,
                None,
            )?);
        }
    }
    Ok(KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    })
}

pub(crate) fn cluster_join_command(uri: &str) -> String {
    format!("sinomo cluster join \"{uri}\"")
}

pub(crate) fn sign_cluster_join_invite(
    invite: &ClusterJoinInvite,
    identity: &NodeIdentity,
) -> Result<String, CliError> {
    let key_pair = identity.key_pair()?;
    let payload = cluster_invite_signing_payload(invite)?;
    Ok(URL_SAFE_NO_PAD.encode(key_pair.sign(payload.as_bytes()).as_ref()))
}

pub(crate) fn verify_cluster_join_invite_signature(
    invite: &ClusterJoinInvite,
) -> Result<(), CliError> {
    if invite.signature.trim().is_empty()
        || invite.issuer_public_key.trim().is_empty()
        || invite.issuer_node_id.as_str().trim().is_empty()
    {
        return Err(CliError::AuthDenied);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(invite.issuer_public_key.as_bytes())
        .map_err(|_| CliError::AuthDenied)?;
    let signature = URL_SAFE_NO_PAD
        .decode(invite.signature.as_bytes())
        .map_err(|_| CliError::AuthDenied)?;
    let payload = cluster_invite_signing_payload(invite)?;
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .map_err(|_| CliError::AuthDenied)
}

pub(crate) fn cluster_invite_signing_payload(
    invite: &ClusterJoinInvite,
) -> Result<String, CliError> {
    let expires_at = invite
        .expires_at
        .format(&Rfc3339)
        .map_err(|source| CliError::Parse {
            detail: format!("invalid invite expiry timestamp: {source}"),
        })?;
    let introducers =
        serde_json::to_string(&invite.introducers).map_err(|source| CliError::Json {
            path: "cluster invite introducers".to_string(),
            detail: source.to_string(),
        })?;
    let channels =
        serde_json::to_string(&invite.communication_channels).map_err(|source| CliError::Json {
            path: "cluster invite communication channels".to_string(),
            detail: source.to_string(),
        })?;
    let allowed_roles =
        serde_json::to_string(&invite.allowed_member_roles).map_err(|source| CliError::Json {
            path: "cluster invite allowed roles".to_string(),
            detail: source.to_string(),
        })?;
    Ok(format!(
        "{CLUSTER_INVITE_AUTH_PROTOCOL}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        invite.invite_id,
        invite.cluster_id,
        invite.cluster_name.clone().unwrap_or_default(),
        invite.cluster_public_key,
        invite.invitation_secret,
        channels,
        introducers,
        invite.token_id,
        expires_at,
        invite.trust_domain,
        allowed_roles,
        invite.issuer_node_id,
        invite.issuer_public_key
    ))
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
    let invite_id = sinomo_runtime::core::ClusterInviteId::generate();
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let issuer_identity = load_or_create_main_node_identity(data_dir)?;
    let invitation_secret = random_url_secret(32)?;
    let communication_channels = invite_communication_channels(state, &introducers);
    let mut invite = ClusterJoinInvite {
        invite_id: invite_id.clone(),
        cluster_id: state.cluster_id.clone(),
        cluster_name: Some(state.cluster_name.clone()),
        cluster_public_key: issuer_identity.public_key.clone(),
        invitation_secret: invitation_secret.clone(),
        communication_channels,
        introducers,
        token_id: format!("token_{}", invite_id.as_str().trim_start_matches("invite_")),
        expires_at: now + time::Duration::seconds(options.ttl_seconds),
        trust_domain: TrustDomain::new("trust.local").expect("static trust domain"),
        allowed_member_roles: vec![ClusterMemberRole::Learner, ClusterMemberRole::NonVoting],
        issuer_node_id: state.local_node_id.clone(),
        issuer_public_key: issuer_identity.public_key.clone(),
        signature: String::new(),
    };
    invite.signature = sign_cluster_join_invite(&invite, &issuer_identity)?;
    Ok(invite)
}

pub(crate) fn random_url_secret(byte_len: usize) -> Result<String, CliError> {
    let rng = SystemRandom::new();
    let mut bytes = vec![0_u8; byte_len];
    rng.fill(&mut bytes).map_err(|_| CliError::Parse {
        detail: "failed to generate secure random invitation secret".to_string(),
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(crate) fn invite_communication_channels(
    state: &ClusterBootstrapState,
    endpoints: &[ClusterEndpointRecord],
) -> Vec<ClusterInvitationChannel> {
    endpoints
        .iter()
        .filter_map(|endpoint| {
            let member = state.members.iter().find(|member| {
                member.node_id == endpoint.node_id && member.state.is_authorized()
            })?;
            Some(ClusterInvitationChannel {
                endpoint: endpoint.clone(),
                node_public_key: member.node_public_key.clone(),
            })
        })
        .collect()
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
                push_unique_introducer(&mut introducers, endpoint);
            }
            if let Ok(record) = network::read_iroh_endpoint_record(paths, &state.local_node_id) {
                push_unique_introducer(&mut introducers, record.endpoint());
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
        push_unique_introducer(&mut introducers, endpoint);
    }
    sort_default_invite_introducers(&mut introducers);
    introducers
}

pub(crate) fn invite_introducers_from_daemon_status(
    node_id: &NodeId,
    status: &DaemonStatus,
) -> Vec<ClusterEndpointRecord> {
    if matches!(status.host.as_str(), "0.0.0.0" | "::") {
        let mut endpoints = Vec::new();
        for ip in discover_lan_ip_candidates() {
            endpoints.push(invite_introducer_endpoint(
                node_id,
                format_ip_port(ip, status.port),
            ));
        }
        endpoints.push(invite_introducer_endpoint(node_id, status.address.clone()));
        endpoints
    } else {
        vec![invite_introducer_endpoint(node_id, status.address.clone())]
    }
}

pub(crate) fn discover_lan_ip_candidates() -> Vec<IpAddr> {
    let mut candidates = Vec::new();
    for target in ["8.8.8.8:80", "1.1.1.1:80", "192.0.2.1:80"] {
        let Ok(socket) = UdpSocket::bind("0.0.0.0:0") else {
            continue;
        };
        if socket.connect(target).is_err() {
            continue;
        }
        let Ok(address) = socket.local_addr() else {
            continue;
        };
        let ip = address.ip();
        if is_invitable_ip(ip) && !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

pub(crate) fn is_invitable_ip(ip: IpAddr) -> bool {
    !ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast()
}

pub(crate) fn format_ip_port(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(ip) => format!("{ip}:{port}"),
        IpAddr::V6(ip) => format!("[{ip}]:{port}"),
    }
}

pub(crate) fn push_unique_introducer(
    introducers: &mut Vec<ClusterEndpointRecord>,
    endpoint: ClusterEndpointRecord,
) {
    if !introducers
        .iter()
        .any(|candidate| candidate.address == endpoint.address)
    {
        introducers.push(endpoint);
    }
}

pub(crate) fn sort_default_invite_introducers(introducers: &mut [ClusterEndpointRecord]) {
    introducers.sort_by_key(|endpoint| {
        (
            !is_remote_usable_invite_endpoint(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
}

pub(crate) fn is_remote_usable_invite_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
        && !is_loopback_connect_address(&endpoint.address)
}

pub(crate) fn is_loopback_connect_address(address: &str) -> bool {
    address.starts_with("127.")
        || address.starts_with("localhost:")
        || address.starts_with("[::1]:")
        || address.starts_with("::1:")
}

pub(crate) fn invite_introducer_endpoint(
    node_id: &NodeId,
    address: String,
) -> ClusterEndpointRecord {
    let is_loopback = is_loopback_connect_address(&address);
    ClusterEndpointRecord {
        endpoint_id: ClusterEndpointId::generate(),
        node_id: node_id.clone(),
        transport: if is_loopback {
            ClusterTransportKind::Loopback
        } else {
            ClusterTransportKind::LanTcp
        },
        address,
        source: ClusterEndpointSource::Static,
        privacy: if is_loopback {
            ClusterEndpointPrivacy::LocalOnly
        } else {
            ClusterEndpointPrivacy::Lan
        },
        priority: if is_loopback { 100 } else { 20 },
        last_observed_latency_ms: None,
    }
}

pub(crate) fn invite_introducer_endpoint_from_state(
    state: &ClusterBootstrapState,
    address: String,
) -> ClusterEndpointRecord {
    if let Some(endpoint) = state
        .members
        .iter()
        .filter(|member| member.state.is_authorized())
        .flat_map(|member| member.endpoints.iter())
        .find(|endpoint| endpoint.address == address)
    {
        return endpoint.clone();
    }
    invite_introducer_endpoint(&state.local_node_id, address)
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
    verify_cluster_join_invite_signature(&invite)?;
    if invite.expires_at <= timestamp {
        return Err(CliError::Parse {
            detail: format!(
                "cluster invite expired at {}",
                format_rfc3339(invite.expires_at)
            ),
        });
    }
    if let Some(cluster_id) = &options.cluster_id
        && cluster_id != &invite.cluster_id
    {
        return Err(CliError::Parse {
            detail: format!(
                "cluster id override `{cluster_id}` does not match invite cluster `{}`",
                invite.cluster_id
            ),
        });
    }
    let cluster_id = invite.cluster_id.clone();
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let mut state = ClusterBootstrapState::pending_join(
        cluster_id,
        invite
            .cluster_name
            .clone()
            .unwrap_or_else(|| options.name.clone()),
        local_node_id.clone(),
        options.name,
        main_identity.public_key.clone(),
        timestamp,
    );
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, timestamp);
    assign_local_node_public_keys(paths, &mut state)?;
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    write_json(&paths.state, &state)?;
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
    verify_cluster_join_invite_signature(&invite)?;
    if invite.cluster_id != state.cluster_id {
        return Err(CliError::Parse {
            detail: format!(
                "join retry invite cluster `{}` does not match local cluster `{}`",
                invite.cluster_id, state.cluster_id
            ),
        });
    }
    let (main_options, shadow_options) = local_daemon_cluster_options()?;
    ensure_daemon_cluster_members(&mut state, &main_options, &shadow_options, timestamp);
    assign_local_node_public_keys(paths, &mut state)?;
    reconcile_cluster_join_with_introducers(&mut state, &invite, Some(paths))?;
    state.updated_at = timestamp;
    write_json(&paths.state, &state)?;
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
    if invite.introducers.is_empty() {
        return Ok(());
    }
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite: invite.clone(),
        members: state.members.clone(),
    };
    let mut failures = Vec::new();
    for channel in invite_channels_for_join_registration(&request.invite) {
        match send_cluster_join_registration_to_channel(&channel.endpoint, &request) {
            Ok(response) => {
                if response.accepted {
                    if let Some(snapshot) = response.kv_snapshot.clone()
                        && let Some(paths) = paths
                    {
                        write_cluster_kv_snapshot(paths, &snapshot)?;
                    }
                    merge_cluster_join_registration(state, response.state);
                    println!("introducer accepted join: {}", channel.endpoint.address);
                    return Ok(());
                }
                failures.push(format!(
                    "{} ({:?}) rejected join: {}",
                    channel.endpoint.address, channel.endpoint.transport, response.detail
                ));
            }
            Err(error) => failures.push(format!(
                "{} ({:?}) failed: {}",
                channel.endpoint.address, channel.endpoint.transport, error
            )),
        }
    }
    if !failures.is_empty() {
        return Err(CliError::Network {
            address: "cluster introducers".to_string(),
            detail: format!(
                "join registration failed for all introducers: {}. If the invite only contains 127.0.0.1 or localhost, create a new invite with `sinomo cluster invite --introducer <host-lan-ip>:21353`.",
                failures.join("; ")
            ),
        });
    }
    Ok(())
}

pub(crate) fn invite_channels_for_join_registration(
    invite: &ClusterJoinInvite,
) -> Vec<ClusterInvitationChannel> {
    let channels = invite_channels(invite);
    if channels
        .iter()
        .any(|channel| is_remote_join_registration_endpoint(&channel.endpoint))
    {
        channels
            .into_iter()
            .filter(|channel| is_remote_join_registration_endpoint(&channel.endpoint))
            .collect()
    } else {
        channels
    }
}

pub(crate) fn is_remote_join_registration_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(endpoint.transport, ClusterTransportKind::Iroh)
        || is_remote_usable_invite_endpoint(endpoint)
}

pub(crate) fn send_cluster_join_registration_to_channel(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    match endpoint.transport {
        ClusterTransportKind::Iroh => send_cluster_join_registration_over_iroh(endpoint, request),
        _ => send_cluster_join_registration(&endpoint.address, request),
    }
}

pub(crate) fn merge_cluster_join_registration(
    local: &mut ClusterBootstrapState,
    accepted: ClusterBootstrapState,
) {
    let local_node_id = local.local_node_id.clone();
    let local_node_display_name = local.local_node_display_name.clone();
    let initialized_at = local.initialized_at;
    *local = accepted;
    local.local_node_id = local_node_id;
    local.local_node_display_name = local_node_display_name;
    local.initialized_at = initialized_at;
}

pub(crate) fn send_cluster_join_registration(
    address: &str,
    request: &ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    let payload = network::encode_object_text_payload(request)?;
    let challenge = random_url_secret(32)?;
    let challenge_request = ClusterInviteChallengeRequest {
        protocol: "sinomo.cluster-invite-challenge.v1".to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_payload = network::encode_object_text_payload(&challenge_request)?;
    let signed = sign_daemon_command_for_local_node(&format!("CLUSTER_JOIN {payload}"))?;
    let (proof_line, response) = network::send_daemon_command_pair_raw(
        address,
        &format!("INVITE_HELLO {challenge_payload}"),
        &signed,
        DAEMON_JOIN_TIMEOUT,
    )?;
    let proof_payload = proof_line
        .trim()
        .strip_prefix("INVITE_PROOF ")
        .ok_or_else(|| CliError::AuthDenied)?;
    let proof: ClusterInviteChallengeProof = network::decode_object_text_payload(proof_payload)?;
    verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;
    network::decode_object_text_payload(response.trim()).map_err(Into::into)
}

pub(crate) fn send_cluster_join_registration_over_iroh(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
) -> Result<ClusterJoinRegistrationResponse, CliError> {
    let payload = network::encode_object_text_payload(request)?;
    let challenge = random_url_secret(32)?;
    let challenge_request = ClusterInviteChallengeRequest {
        protocol: "sinomo.cluster-invite-challenge.v1".to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_payload = network::encode_object_text_payload(&challenge_request)?;
    let proof_response = network::send_daemon_command_to_cluster_endpoint(
        endpoint,
        &format!("INVITE_HELLO {challenge_payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    let proof_payload = proof_response
        .trim()
        .strip_prefix("INVITE_PROOF ")
        .ok_or_else(|| CliError::AuthDenied)?;
    let proof: ClusterInviteChallengeProof = network::decode_object_text_payload(proof_payload)?;
    verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;
    let signed = sign_daemon_command_for_local_node(&format!("CLUSTER_JOIN {payload}"))?;
    let response =
        network::send_daemon_command_to_cluster_endpoint(endpoint, &signed, DAEMON_JOIN_TIMEOUT)?;
    network::decode_object_text_payload(response.trim()).map_err(Into::into)
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
    decode_cluster_join_invite(extract_invite_from_join_command(invite.trim()))
}

pub(crate) fn extract_invite_from_join_command(raw: &str) -> &str {
    let first_line = raw
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(raw);
    let trimmed = first_line.trim();
    let Some(rest) = trimmed.strip_prefix("sinomo cluster join ") else {
        return raw.trim();
    };
    let rest = rest.trim();
    if let Some(after_quote) = rest.strip_prefix('"')
        && let Some(end) = after_quote.find('"')
    {
        return &after_quote[..end];
    }
    rest.split_whitespace().next().unwrap_or(rest)
}

pub(crate) fn encode_cluster_join_invite(invite: &ClusterJoinInvite) -> Result<String, CliError> {
    let bytes = serde_json::to_vec(invite).map_err(|source| CliError::Json {
        path: "cluster invite".to_string(),
        detail: source.to_string(),
    })?;
    Ok(format!(
        "{SINOMO_JOIN_URI_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(bytes)
    ))
}

pub(crate) fn decode_cluster_join_invite(raw: &str) -> Result<ClusterJoinInvite, CliError> {
    if let Some(payload) = raw.strip_prefix(SINOMO_JOIN_URI_PREFIX) {
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|source| CliError::Parse {
                detail: format!("invalid cluster invite encoding: {source}"),
            })?;
        serde_json::from_slice(&bytes).map_err(|source| CliError::Json {
            path: "cluster invite".to_string(),
            detail: source.to_string(),
        })
    } else if raw.trim_start().starts_with('{') {
        serde_json::from_str(raw).map_err(|source| CliError::Json {
            path: "cluster invite".to_string(),
            detail: source.to_string(),
        })
    } else {
        Err(CliError::Parse {
            detail:
                "cluster join requires a sinomo-join://v1 invite, invite JSON, or invite file path"
                    .to_string(),
        })
    }
}

pub(crate) fn cluster_status() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    match read_cluster_state_without_kv(&paths) {
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
            "node\tprocess\tmember\thealth\tversion\tplatform\tpid\taddress\tlast-seen\trollout"
        );
        return Ok(());
    }
    println!("node\tprocess\tmember\thealth\tversion\tplatform\tpid\taddress\tlast-seen\trollout");
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
        println!(
            "{}\t{}\t{} {}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.member.node_id,
            row.status.process,
            format_member_role(&row.member.role),
            format_member_state(&row.member.state),
            format_node_availability(&row.availability),
            row.status.daemon_version,
            platform,
            pid,
            address,
            row.status.observed_at,
            rollout
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterHealthRow {
    pub(crate) member: ClusterMemberRecord,
    pub(crate) status: ClusterNodeStatusRecord,
    pub(crate) availability: ClusterNodeAvailability,
}

pub(crate) fn cluster_health_rows(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterHealthRow>, CliError> {
    let stored_statuses = read_cluster_node_statuses_from_kv(paths)?;
    let mut rows = Vec::new();
    for member in &state.members {
        if matches!(member.state, ClusterMemberState::Removed) {
            continue;
        }
        let live = probe_member_daemon_status(state, member);
        let stored = stored_statuses
            .iter()
            .find(|status| status.node_id == member.node_id)
            .cloned();
        let live_online = live.is_some();
        let mut status = if let Some(live) = live {
            let mut live = live;
            if live.rollout.is_none() {
                live.rollout = stored.as_ref().and_then(|status| status.rollout.clone());
            }
            live
        } else if let Some(stored) = stored {
            stored
        } else {
            unknown_cluster_node_status(state, member, OffsetDateTime::now_utc())
        };
        let availability = if live_online
            || (status.availability == ClusterNodeAvailability::Online
                && !cluster_node_status_is_stale(&status))
        {
            ClusterNodeAvailability::Online
        } else if live_probe_possible(state, member) {
            ClusterNodeAvailability::Offline
        } else {
            ClusterNodeAvailability::Unknown
        };
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
        });
    }
    rows.sort_by(|left, right| left.member.node_id.cmp(&right.member.node_id));
    Ok(rows)
}

pub(crate) fn probe_member_daemon_status(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Option<ClusterNodeStatusRecord> {
    let is_local_process = cluster_member_is_local_process(state, member);
    let mut endpoints = prefer_bounded_daemon_command_targets(member.endpoints.clone());
    endpoints.sort_by_key(|endpoint| {
        (
            cluster_daemon_command_transport_rank(endpoint),
            endpoint.privacy != ClusterEndpointPrivacy::Lan,
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    for endpoint in endpoints {
        if !is_local_process && matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly) {
            continue;
        }
        let Ok(response) =
            send_daemon_command_to_endpoint(&endpoint, "STATUS", CLUSTER_HEALTH_PROBE_TIMEOUT)
        else {
            continue;
        };
        let Ok(status) = serde_json::from_str::<DaemonStatus>(response.trim()) else {
            continue;
        };
        let Some(cluster) = status.cluster.as_ref() else {
            continue;
        };
        if cluster.cluster_id != state.cluster_id || cluster.node_id != member.node_id {
            continue;
        }
        return Some(cluster_node_status_from_daemon_status(
            &status,
            cluster,
            None,
            OffsetDateTime::now_utc(),
        ));
    }
    None
}

pub(crate) fn live_probe_possible(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    let is_local_process = cluster_member_is_local_process(state, member);
    member.endpoints.iter().any(|endpoint| {
        (is_local_process || !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly))
            && matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::Loopback
                    | ClusterTransportKind::Iroh
            )
    })
}

pub(crate) fn cluster_member_is_local_process(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    member.node_id == state.local_node_id
        || member.node_id == shadow_daemon_node_id(&state.local_node_id)
}

pub(crate) fn unknown_cluster_node_status(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    ClusterNodeStatusRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: member.node_id.clone(),
        process: member
            .capabilities
            .iter()
            .find_map(|capability| match capability.as_str() {
                "daemon.main.run" => Some(DAEMON_MAIN_CLUSTER_ROLE.to_string()),
                "daemon.shadow.supervise" => Some(DAEMON_SHADOW_CLUSTER_ROLE.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| "unknown".to_string()),
        availability: ClusterNodeAvailability::Unknown,
        daemon_version: "unknown".to_string(),
        target_os: None,
        target_arch: None,
        pid: None,
        address: member
            .endpoints
            .iter()
            .find(|endpoint| !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly))
            .or_else(|| member.endpoints.first())
            .map(|endpoint| endpoint.address.clone()),
        bind_address: None,
        executable: None,
        rollout: None,
        daemon_started_at: None,
        observed_at,
    }
}

pub(crate) fn cluster_node_status_is_stale(status: &ClusterNodeStatusRecord) -> bool {
    (OffsetDateTime::now_utc() - status.observed_at).whole_seconds()
        > CLUSTER_NODE_STATUS_STALE_AFTER_SECONDS
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
    let graph = network::read_cluster_connectivity_graph_from_kv(&paths).unwrap_or_default();
    let findings = cluster_doctor_findings(&state, &graph, &rows);
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterDoctorFinding {
    pub(crate) severity: String,
    pub(crate) code: String,
    pub(crate) detail: String,
}

impl ClusterDoctorFinding {
    fn warning(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: "warning".to_string(),
            code: code.into(),
            detail: detail.into(),
        }
    }
}

pub(crate) fn cluster_doctor_findings(
    state: &ClusterBootstrapState,
    connectivity: &[network::ClusterConnectivityRecord],
    health: &[ClusterHealthRow],
) -> Vec<ClusterDoctorFinding> {
    let mut findings = Vec::new();
    for row in health {
        if row.availability == ClusterNodeAvailability::Offline {
            findings.push(ClusterDoctorFinding::warning(
                "node-offline",
                format!(
                    "{} is offline from {} using advertised endpoints",
                    row.member.node_id, state.local_node_id
                ),
            ));
        } else if row.availability == ClusterNodeAvailability::Unknown {
            findings.push(ClusterDoctorFinding::warning(
                "node-unknown",
                format!("{} has no recent reachable status", row.member.node_id),
            ));
        }
    }

    for record in connectivity {
        if record.state == network::ClusterConnectivityState::Unreachable {
            findings.push(ClusterDoctorFinding::warning(
                "endpoint-unreachable",
                format!(
                    "{} cannot reach {} at {} via {:?} ({} consecutive failures)",
                    record.source_node_id,
                    record.target_node_id,
                    record.address,
                    record.transport,
                    record.consecutive_failures
                ),
            ));
        }
    }

    for member in &state.members {
        let mut by_public_key: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for endpoint in &member.endpoints {
            if let Some(public_key) = network::iroh_endpoint_public_key(endpoint) {
                *by_public_key.entry(public_key).or_default() += 1;
            }
        }
        for (public_key, count) in by_public_key {
            if count > 1 {
                findings.push(ClusterDoctorFinding::warning(
                    "duplicate-iroh-endpoint",
                    format!(
                        "{} has {count} Iroh endpoint records for public key {public_key}",
                        member.node_id
                    ),
                ));
            }
        }
    }

    let route_targets = advertised_peer_route_targets(state);
    if !route_targets.is_empty() {
        let route_ips = discover_peer_route_ip_candidates(state);
        if route_ips.is_empty() {
            findings.push(ClusterDoctorFinding::warning(
                "peer-route-unadvertised",
                format!(
                    "{} peer route target(s) exist, but no routable local source address could be discovered",
                    route_targets.len()
                ),
            ));
        }
    }

    findings.sort_by(|left, right| (&left.code, &left.detail).cmp(&(&right.code, &right.detail)));
    findings
}

pub(crate) fn cluster_peers() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    let members = cluster_peer_members(&paths)?;
    for member in members {
        println!(
            "{}\t{}\t{:?}\t{:?}",
            member.node_id, member.display_name, member.role, member.state
        );
    }
    Ok(())
}

pub(crate) fn cluster_peer_members(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, CliError> {
    refresh_cluster_kv_from_local_daemon(paths)?;
    let mut state = read_cluster_state(paths)?;
    if cluster_state_needs_join_registration(&state) {
        let (main_options, shadow_options) = local_daemon_cluster_options()?;
        ensure_daemon_cluster_members(
            &mut state,
            &main_options,
            &shadow_options,
            OffsetDateTime::now_utc(),
        );
        write_json(&paths.state, &state)?;
    }
    Ok(state.members)
}

pub(crate) fn cluster_leave() -> Result<(), CliError> {
    let paths = cluster_paths_from_defaults()?;
    if !paths.state.exists() {
        return Err(CliError::ClusterOrphan);
    }
    fs::remove_file(&paths.state).map_err(|source| CliError::Io {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })?;
    if paths.kv_snapshot.exists() {
        fs::remove_file(&paths.kv_snapshot).map_err(|source| CliError::Io {
            path: paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    println!("cluster state removed: {}", paths.state.display());
    Ok(())
}

pub(crate) fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, CliError> {
    let mut state = network::read_cluster_state(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

pub(crate) fn read_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, CliError> {
    let mut state = network::read_cluster_state_without_kv(paths)?;
    prepare_local_cluster_state(paths, &mut state)?;
    Ok(state)
}

fn prepare_local_cluster_state(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), CliError> {
    ensure_daemon_cluster_members(
        state,
        &DaemonRunOptions::default(),
        &DaemonRunOptions::shadow_default(),
        OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(paths, state)
}

pub(crate) fn refresh_cluster_kv_from_local_daemon(paths: &ClusterPaths) -> Result<(), CliError> {
    let Some(status) = default_main_daemon_status()? else {
        return Ok(());
    };
    let Ok(response) =
        send_daemon_command(&status.address, "CLUSTER_KV_SNAPSHOT", DAEMON_PROBE_TIMEOUT)
    else {
        return Ok(());
    };
    if response.trim().is_empty() {
        return Ok(());
    }
    let snapshot: KvSnapshot =
        serde_json::from_str(response.trim()).map_err(|source| CliError::Json {
            path: "cluster kv snapshot response".to_string(),
            detail: source.to_string(),
        })?;
    write_cluster_kv_snapshot(paths, &snapshot)
}

pub(crate) fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, CliError> {
    match network::read_cluster_kv_snapshot(paths) {
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
    if network::read_cluster_kv_snapshot(paths).is_ok_and(|existing| existing == *snapshot) {
        return Ok(());
    }
    network::write_cluster_kv_snapshot(paths, snapshot).map_err(Into::into)
}

pub(crate) fn read_cluster_kv_store(
    paths: &ClusterPaths,
) -> Result<sinomo_runtime::kv::InProcessKvStore, CliError> {
    network::read_cluster_kv_store(paths).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn read_cluster_members_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, CliError> {
    network::read_cluster_members_from_kv(paths).map_err(Into::into)
}

pub(crate) fn read_cluster_node_statuses_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNodeStatusRecord>, CliError> {
    network::read_cluster_node_statuses_from_kv(paths).map_err(Into::into)
}

pub(crate) fn sync_cluster_state_to_kv(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, CliError> {
    let mut keyed_state = state.clone();
    assign_local_node_public_keys(paths, &mut keyed_state)?;
    network::cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let mut commit_state = keyed_state.clone();
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut commit_state,
        cluster_state_sync_txn(paths, &keyed_state)?,
    )
}

pub(crate) fn sync_cluster_node_status_to_kv(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
) -> Result<Option<KvSnapshot>, CliError> {
    if stored_cluster_node_status_matches(paths, &status)? {
        return Ok(None);
    }
    let mut state = read_cluster_state_without_kv(paths)?;
    let mut success = vec![KvOp::put_serde(
        network::cluster_node_status_key(&status.node_id),
        &status,
        None,
    )?];
    if let Some(rollout) = status.rollout.clone() {
        success.push(KvOp::put_serde(
            network::cluster_rollout_node_key(&status.node_id),
            &rollout,
            None,
        )?);
    }
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        },
    )
    .map(Some)
}

pub(crate) fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: network::ClusterNetworkInfoRecord,
) -> Result<Option<KvSnapshot>, CliError> {
    if stored_cluster_network_info_matches(paths, &info)? {
        return Ok(None);
    }
    let mut state = read_cluster_state_without_kv(paths)?;
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(
                network::cluster_network_info_key(&info.node_id),
                &info,
                None,
            )?],
            failure: Vec::new(),
        },
    )
    .map(Some)
}

pub(crate) fn sync_cluster_connectivity_records_to_kv(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: Vec<network::ClusterConnectivityRecord>,
) -> Result<Option<KvSnapshot>, CliError> {
    if stored_cluster_connectivity_records_match(paths, source_node_id, &records)? {
        return Ok(None);
    }
    let mut state = read_cluster_state_without_kv(paths)?;
    let mut success = vec![KvOp::DeletePrefix {
        prefix: network::cluster_connectivity_source_prefix(source_node_id),
    }];
    for record in records {
        let key = network::cluster_connectivity_key(
            &record.source_node_id,
            &record.target_node_id,
            &record.endpoint_id,
        );
        success.push(KvOp::put_serde(key, &record, None)?);
    }
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        },
    )
    .map(Some)
}

pub(crate) fn expire_cluster_network_records_in_kv(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<Option<KvSnapshot>, CliError> {
    let store = network::read_cluster_kv_snapshot_store(paths)?;
    let mut success = Vec::new();
    for entry in store.range_prefix("/cluster/network/info/") {
        let Ok(record) = entry.decode::<network::ClusterNetworkInfoRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete { key: entry.key });
        }
    }
    for entry in store.range_prefix("/cluster/network/connectivity/") {
        let Ok(record) = entry.decode::<network::ClusterConnectivityRecord>() else {
            continue;
        };
        if record.expires_at <= now {
            success.push(KvOp::Delete { key: entry.key });
        }
    }
    if success.is_empty() {
        return Ok(None);
    }
    let mut state = read_cluster_state_without_kv(paths)?;
    commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success,
            failure: Vec::new(),
        },
    )
    .map(Some)
}

fn stored_cluster_node_status_matches(
    paths: &ClusterPaths,
    status: &ClusterNodeStatusRecord,
) -> Result<bool, CliError> {
    let store = network::read_cluster_kv_snapshot_store(paths)?;
    let Some(entry) = store.get(&network::cluster_node_status_key(&status.node_id)) else {
        return Ok(false);
    };
    let Ok(existing) = entry.decode::<ClusterNodeStatusRecord>() else {
        return Ok(false);
    };
    Ok(cluster_node_status_stable_eq(&existing, status))
}

fn cluster_node_status_stable_eq(
    left: &ClusterNodeStatusRecord,
    right: &ClusterNodeStatusRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.process == right.process
        && left.availability == right.availability
        && left.daemon_version == right.daemon_version
        && left.target_os == right.target_os
        && left.target_arch == right.target_arch
        && left.address == right.address
        && left.bind_address == right.bind_address
        && left.executable == right.executable
        && left.rollout == right.rollout
        && left.daemon_started_at == right.daemon_started_at
}

fn stored_cluster_network_info_matches(
    paths: &ClusterPaths,
    info: &network::ClusterNetworkInfoRecord,
) -> Result<bool, CliError> {
    let store = network::read_cluster_kv_snapshot_store(paths)?;
    let Some(entry) = store.get(&network::cluster_network_info_key(&info.node_id)) else {
        return Ok(false);
    };
    let Ok(existing) = entry.decode::<network::ClusterNetworkInfoRecord>() else {
        return Ok(false);
    };
    Ok(cluster_network_info_stable_eq(&existing, info))
}

fn cluster_network_info_stable_eq(
    left: &network::ClusterNetworkInfoRecord,
    right: &network::ClusterNetworkInfoRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.environment_id == right.environment_id
        && left.observed_addresses == right.observed_addresses
        && left.endpoints == right.endpoints
}

fn stored_cluster_connectivity_records_match(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[network::ClusterConnectivityRecord],
) -> Result<bool, CliError> {
    let store = read_cluster_kv_store(paths)?;
    let mut existing = store
        .range_prefix(&network::cluster_connectivity_source_prefix(source_node_id))
        .into_iter()
        .filter_map(|entry| entry.decode::<network::ClusterConnectivityRecord>().ok())
        .collect::<Vec<_>>();
    let mut incoming = records.to_vec();
    existing.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
    incoming.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
    Ok(existing.len() == incoming.len()
        && existing
            .iter()
            .zip(incoming.iter())
            .all(|(left, right)| cluster_connectivity_stable_eq(left, right)))
}

fn cluster_connectivity_stable_eq(
    left: &network::ClusterConnectivityRecord,
    right: &network::ClusterConnectivityRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.source_node_id == right.source_node_id
        && left.target_node_id == right.target_node_id
        && left.endpoint_id == right.endpoint_id
        && left.transport == right.transport
        && left.address == right.address
        && left.state == right.state
}

pub(crate) fn cluster_state_sync_txn(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvTxn, CliError> {
    let mut state = state.clone();
    network::cleanup_cluster_iroh_endpoints(&mut state);
    let new_config = network::ClusterRaftConfigRecord::from(&state);
    let current_config_entry = read_cluster_kv_store(paths)?
        .get(&network::cluster_raft_config_key())
        .cloned();
    let mut compares = Vec::new();
    if let Some(current_config_entry) = current_config_entry {
        let current_config: network::ClusterRaftConfigRecord = current_config_entry.decode()?;
        validate_raft_config_transition(&current_config, &new_config)?;
        compares.push(KvCompare::ValueEquals {
            key: network::cluster_raft_config_key(),
            value: current_config_entry.value,
        });
    } else {
        compares.push(KvCompare::Missing {
            key: network::cluster_raft_config_key(),
        });
    }
    let mut success = vec![
        KvOp::put_serde(
            network::cluster_identity_key(),
            &network::ClusterIdentityRecord::from(&state),
            None,
        )?,
        KvOp::put_serde(network::cluster_raft_config_key(), &new_config, None)?,
    ];
    for member in &state.members {
        success.push(KvOp::put_serde(
            cluster_member_key(&member.node_id),
            member,
            None,
        )?);
        success.push(KvOp::put_serde(
            network::cluster_capability_policy_key(&member.node_id),
            &network::ClusterCapabilityPolicyRecord::from(member),
            None,
        )?);
        success.push(KvOp::DeletePrefix {
            prefix: network::cluster_endpoint_prefix(&member.node_id),
        });
        for endpoint in &member.endpoints {
            success.push(KvOp::put_serde(
                network::cluster_endpoint_key(&member.node_id, endpoint),
                endpoint,
                None,
            )?);
        }
    }
    Ok(KvTxn {
        compares,
        success,
        failure: Vec::new(),
    })
}

pub(crate) fn validate_raft_config_transition(
    current: &network::ClusterRaftConfigRecord,
    next: &network::ClusterRaftConfigRecord,
) -> Result<(), CliError> {
    if current.cluster_id != next.cluster_id {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "cluster raft config cluster id changed from `{}` to `{}`",
                current.cluster_id, next.cluster_id
            ),
        });
    }
    let current_voters = current
        .voters
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let next_voters = next
        .voters
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let removed_voters = current_voters
        .difference(&next_voters)
        .cloned()
        .collect::<Vec<_>>();
    if !removed_voters.is_empty() {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "raft voter removal requires a safe reconfiguration protocol; refusing to remove {}",
                removed_voters
                    .iter()
                    .map(NodeId::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
    }
    let added_voters = next_voters
        .difference(&current_voters)
        .cloned()
        .collect::<Vec<_>>();
    if added_voters.len() > 1 {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "raft voter expansion is limited to one voter per committed config change; requested {} additions",
                added_voters.len()
            ),
        });
    }
    if let Some(added_voter) = added_voters.first() {
        let current_learners = current
            .learners
            .iter()
            .chain(current.non_voting.iter())
            .collect::<std::collections::BTreeSet<_>>();
        if !current_learners.contains(added_voter) {
            return Err(CliError::ClusterReadOnly {
                detail: format!(
                    "raft voter `{added_voter}` must already be a committed learner or non-voting member before promotion"
                ),
            });
        }
    }
    Ok(())
}

pub(crate) fn assign_local_node_public_keys(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<(), CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "cluster paths have no data directory parent".to_string(),
    })?;
    let main_identity = load_or_create_main_node_identity(data_dir)?;
    let shadow_identity = load_or_create_shadow_node_identity(data_dir)?;
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    for member in &mut state.members {
        if member.node_id == state.local_node_id {
            member.node_public_key = main_identity.public_key.clone();
        } else if member.node_id == shadow_node_id {
            member.node_public_key = shadow_identity.public_key.clone();
        }
    }
    Ok(())
}

fn cluster_bootstrap_state_stable_eq(
    left: &ClusterBootstrapState,
    right: &ClusterBootstrapState,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.cluster_name == right.cluster_name
        && left.local_node_id == right.local_node_id
        && left.local_node_display_name == right.local_node_display_name
        && left.term == right.term
        && left.initialized_at == right.initialized_at
        && left.members.len() == right.members.len()
        && left
            .members
            .iter()
            .zip(right.members.iter())
            .all(|(left, right)| cluster_member_stable_eq(left, right))
}

fn cluster_member_stable_eq(left: &ClusterMemberRecord, right: &ClusterMemberRecord) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.display_name == right.display_name
        && left.role == right.role
        && left.state == right.state
        && left.trust_domain == right.trust_domain
        && left.node_public_key == right.node_public_key
        && left.certificate_id == right.certificate_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
        && left.endpoints == right.endpoints
        && left.joined_at == right.joined_at
}

pub(crate) struct ClusterNetworkMaintenance {
    pub(crate) discovery: Option<MdnsDiscoverySocket>,
    pub(crate) discovery_cache: MdnsDiscoveryCache,
    pub(crate) connectivity: std::collections::BTreeMap<String, ClusterConnectivityProbeState>,
    pub(crate) environment_id: Option<String>,
    pub(crate) tick: u64,
    pub(crate) last_publish: Instant,
    pub(crate) last_discovery: Instant,
    pub(crate) last_probe: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ClusterConnectivityProbeState {
    pub(crate) record: network::ClusterConnectivityRecord,
}

impl ClusterNetworkMaintenance {
    pub(crate) fn new() -> Self {
        let now = Instant::now();
        Self {
            discovery: MdnsDiscoverySocket::bind_default().ok(),
            discovery_cache: MdnsDiscoveryCache::new(),
            connectivity: std::collections::BTreeMap::new(),
            environment_id: None,
            tick: 0,
            last_publish: now,
            last_discovery: now,
            last_probe: now,
        }
    }

    pub(crate) fn tick(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
    ) -> Result<(), CliError> {
        self.tick = self.tick.saturating_add(1);
        let now = Instant::now();
        let publish_due = now.duration_since(self.last_publish) >= DAEMON_CLUSTER_PUBLISH_INTERVAL;
        let probe_due =
            now.duration_since(self.last_probe) >= DAEMON_CLUSTER_NETWORK_PROBE_INTERVAL;
        let discovery_due =
            now.duration_since(self.last_discovery) >= DAEMON_CLUSTER_DISCOVERY_INTERVAL;
        if !(publish_due || probe_due || discovery_due) {
            return Ok(());
        }

        let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon paths have no data directory parent".to_string(),
        })?;
        let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        if !cluster_paths.state.exists() {
            return Ok(());
        }

        let mut state = read_cluster_state_without_kv(&cluster_paths)?;
        let state_before_maintenance = state.clone();
        let mut discovered = false;
        let mut snapshot = None;

        if discovery_due {
            self.last_discovery = now;
            discovered = self.process_discovery(&mut state);
            self.announce(&state);
        }

        if publish_due || probe_due {
            network::cleanup_cluster_iroh_endpoints(&mut state);
            let shadow_options = current_shadow_daemon_options(data_dir);
            ensure_daemon_cluster_members(
                &mut state,
                options,
                &shadow_options,
                OffsetDateTime::now_utc(),
            );
            assign_local_node_public_keys(&cluster_paths, &mut state)?;
        }

        if publish_due {
            self.last_publish = now;
            if cluster_has_local_quorum(&cluster_paths, &state) {
                if !cluster_bootstrap_state_stable_eq(&state_before_maintenance, &state) {
                    let _ = sync_cluster_state_to_kv(&cluster_paths, &state)?;
                }
                let local_network_info = build_local_cluster_network_info(
                    &state,
                    &state.local_node_id,
                    OffsetDateTime::now_utc(),
                );
                if self
                    .environment_id
                    .as_ref()
                    .is_some_and(|known| known != &local_network_info.environment_id)
                {
                    self.connectivity.clear();
                }
                self.environment_id = Some(local_network_info.environment_id.clone());
                if let Some(network_snapshot) =
                    sync_cluster_network_info_to_kv(&cluster_paths, local_network_info)?
                {
                    snapshot = Some(network_snapshot);
                }
                if let Some(status_snapshot) = refresh_daemon_node_status_in_kv(paths)? {
                    snapshot = Some(status_snapshot);
                }
                let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
                if matches!(
                    probe_daemon_status(&shadow_paths, DAEMON_PROBE_TIMEOUT),
                    DaemonProbeStatus::Running(_)
                ) {
                    if let Some(status_snapshot) = refresh_daemon_node_status_in_kv(&shadow_paths)?
                    {
                        snapshot = Some(status_snapshot);
                    }
                }
            } else if !cluster_raft_local_is_voter(&state) {
                if let Err(error) = publish_local_members_via_voter(&cluster_paths, &state) {
                    eprintln!("sinomo daemon cluster member publish via voter failed: {error}");
                }
            }
        }

        if probe_due {
            self.last_probe = now;
            if cluster_has_local_quorum(&cluster_paths, &state) {
                let records = self.probe_connectivity(&state);
                let _ = sync_cluster_connectivity_records_to_kv(
                    &cluster_paths,
                    &state.local_node_id,
                    records,
                )?;
                if let Some(expire_snapshot) =
                    expire_cluster_network_records_in_kv(&cluster_paths, OffsetDateTime::now_utc())?
                {
                    snapshot = Some(expire_snapshot);
                }
            }
        }

        if discovered || !cluster_bootstrap_state_stable_eq(&state_before_maintenance, &state) {
            network::write_cluster_state(&cluster_paths, &state)?;
        }
        if let Some(snapshot) = snapshot.as_ref() {
            write_cluster_kv_snapshot(&cluster_paths, snapshot)?;
        }
        Ok(())
    }

    fn process_discovery(&mut self, state: &mut ClusterBootstrapState) -> bool {
        let Some(discovery) = &self.discovery else {
            return false;
        };
        let Ok(advertisements) = discovery.drain() else {
            return false;
        };
        let mut changed = false;
        for advertisement in advertisements {
            if advertisement.cluster_id != state.cluster_id
                || advertisement.node_id == state.local_node_id
            {
                continue;
            }
            for record in advertisement.records() {
                self.discovery_cache.upsert(record.clone());
                if let Some(member) = state
                    .members
                    .iter_mut()
                    .find(|member| member.node_id == record.endpoint.node_id)
                {
                    network::upsert_member_endpoint(member, record.endpoint);
                    changed = true;
                }
            }
        }
        self.discovery_cache.expire_after_tick(self.tick);
        changed
    }

    fn announce(&self, state: &ClusterBootstrapState) {
        let Some(discovery) = &self.discovery else {
            return;
        };
        let Some(member) = state
            .members
            .iter()
            .find(|member| member.node_id == state.local_node_id)
        else {
            return;
        };
        let endpoints: Vec<_> = member
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.privacy == ClusterEndpointPrivacy::Lan
                    && matches!(
                        endpoint.transport,
                        ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic
                    )
            })
            .cloned()
            .collect();
        if endpoints.is_empty() {
            return;
        }
        let advertisement = MdnsDiscoveryAdvertisement::new(
            state.cluster_id.clone(),
            state.local_node_id.clone(),
            endpoints,
            self.tick,
        );
        let _ = discovery.announce(&advertisement);
    }

    fn probe_connectivity(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Vec<network::ClusterConnectivityRecord> {
        let now = OffsetDateTime::now_utc();
        let timestamp = format_rfc3339(now);
        let mut active_keys = std::collections::BTreeSet::new();
        for endpoint in cluster_network_probe_targets(state) {
            let key = cluster_connectivity_probe_key(&state.local_node_id, &endpoint);
            active_keys.insert(key.clone());
            let started = Instant::now();
            let result =
                send_daemon_command_to_endpoint(&endpoint, "STATUS", CLUSTER_HEALTH_PROBE_TIMEOUT);
            let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            let previous = self
                .connectivity
                .get(&key)
                .map(|state| state.record.clone());
            let record = match result {
                Ok(response) if cluster_status_response_matches(state, &endpoint, &response) => {
                    cluster_connectivity_record(
                        state,
                        &endpoint,
                        network::ClusterConnectivityState::Healthy,
                        Some(latency_ms),
                        0,
                        Some(timestamp.clone()),
                        previous.and_then(|record| record.last_failure_at),
                        now,
                    )
                }
                _ => {
                    let observed_failures = previous
                        .as_ref()
                        .map(|record| record.consecutive_failures.saturating_add(1))
                        .unwrap_or(1);
                    let retained_success = previous.as_ref().and_then(|record| {
                        if observed_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
                            record.last_success_at.clone()
                        } else {
                            None
                        }
                    });
                    let state_label = if observed_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
                    {
                        network::ClusterConnectivityState::Unreachable
                    } else {
                        network::ClusterConnectivityState::Degraded
                    };
                    let failures = observed_failures.min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD);
                    cluster_connectivity_record(
                        state,
                        &endpoint,
                        state_label,
                        previous.and_then(|record| record.latency_ms),
                        failures,
                        retained_success,
                        Some(timestamp.clone()),
                        now,
                    )
                }
            };
            self.connectivity
                .insert(key, ClusterConnectivityProbeState { record });
        }

        self.connectivity
            .retain(|key, state| active_keys.contains(key) || state.record.expires_at > now);
        self.connectivity
            .values()
            .map(|state| state.record.clone())
            .collect()
    }
}

pub(crate) fn build_local_cluster_network_info(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    now: OffsetDateTime,
) -> network::ClusterNetworkInfoRecord {
    let endpoints = state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.endpoints.clone())
        .unwrap_or_default();
    let mut observed_addresses = endpoints
        .iter()
        .filter(|endpoint| {
            matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::LanQuic
                    | ClusterTransportKind::Iroh
                    | ClusterTransportKind::WebRtc
                    | ClusterTransportKind::TurnRelay
                    | ClusterTransportKind::HttpsRelay
            )
        })
        .map(|endpoint| endpoint.address.clone())
        .collect::<Vec<_>>();
    observed_addresses.sort();
    observed_addresses.dedup();
    let environment_id = cluster_network_environment_id(&observed_addresses);
    network::ClusterNetworkInfoRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: node_id.clone(),
        environment_id,
        observed_addresses,
        endpoints,
        observed_at: now,
        expires_at: now + time::Duration::seconds(CLUSTER_NETWORK_INFO_TTL_SECONDS),
    }
}

pub(crate) fn cluster_network_environment_id(values: &[String]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for value in values {
        for byte in value.as_bytes().iter().chain(std::iter::once(&0)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("env_{hash:016x}")
}

pub(crate) fn cluster_network_probe_targets(
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = cluster_sync_targets(state)
        .into_iter()
        .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
        .collect::<Vec<_>>();
    endpoints.sort_by_key(|endpoint| {
        (
            endpoint.node_id.clone(),
            endpoint.priority,
            format!("{:?}", endpoint.transport),
            endpoint.address.clone(),
        )
    });
    endpoints
}

pub(crate) fn cluster_connectivity_probe_key(
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> String {
    format!(
        "{}:{}:{}",
        source_node_id, endpoint.node_id, endpoint.endpoint_id
    )
}

pub(crate) fn cluster_status_response_matches(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    response: &str,
) -> bool {
    let Ok(status) = serde_json::from_str::<DaemonStatus>(response.trim()) else {
        return false;
    };
    status.cluster.as_ref().is_some_and(|cluster| {
        cluster.cluster_id == state.cluster_id && cluster.node_id == endpoint.node_id
    })
}

pub(crate) fn cluster_connectivity_record(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: network::ClusterConnectivityState,
    latency_ms: Option<u64>,
    consecutive_failures: u16,
    last_success_at: Option<String>,
    last_failure_at: Option<String>,
    now: OffsetDateTime,
) -> network::ClusterConnectivityRecord {
    let expires_in = if matches!(
        connectivity_state,
        network::ClusterConnectivityState::Healthy
    ) {
        CLUSTER_CONNECTIVITY_TTL_SECONDS
    } else {
        CLUSTER_CONNECTIVITY_TTL_SECONDS / 2
    };
    network::ClusterConnectivityRecord {
        cluster_id: state.cluster_id.clone(),
        source_node_id: state.local_node_id.clone(),
        target_node_id: endpoint.node_id.clone(),
        endpoint_id: endpoint.endpoint_id.clone(),
        transport: endpoint.transport.clone(),
        address: endpoint.address.clone(),
        state: connectivity_state,
        latency_ms,
        consecutive_failures,
        last_success_at,
        last_failure_at,
        observed_at: now,
        expires_at: now + time::Duration::seconds(expires_in),
    }
}

pub(crate) fn current_shadow_daemon_options(data_dir: &Path) -> DaemonRunOptions {
    match probe_daemon_status(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        DAEMON_PROBE_TIMEOUT,
    ) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions::shadow_default()
        }
    }
}

pub(crate) fn cluster_has_local_quorum(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> bool {
    cluster_reachable_voter_count(paths, state) >= cluster_raft_quorum_size(state)
}

pub(crate) fn cluster_status_summary_label(state: &ClusterBootstrapState) -> &'static str {
    if cluster_state_needs_join_registration(state) {
        "pending-join"
    } else if cluster_raft_local_is_voter(state) {
        "voter"
    } else {
        "read-only"
    }
}

pub(crate) fn cluster_state_needs_join_registration(state: &ClusterBootstrapState) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .is_none_or(|member| {
            matches!(member.role, ClusterMemberRole::Learner) && !member.state.is_authorized()
        })
}

pub(crate) fn cluster_reachable_voter_count(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> usize {
    let quorum_size = cluster_raft_quorum_size(state);
    let mut voters = std::collections::BTreeSet::new();
    if cluster_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return voters.len();
        }
    }
    for endpoint in
        prefer_bounded_daemon_command_targets(cluster_sync_targets_with_connectivity(paths, state))
    {
        if !cluster_raft_node_is_voter(state, &endpoint.node_id) {
            continue;
        }
        if send_daemon_command_to_endpoint(&endpoint, "STATUS", DAEMON_JOIN_TIMEOUT).is_ok() {
            voters.insert(endpoint.node_id);
            if voters.len() >= quorum_size {
                break;
            }
        }
    }
    voters.len()
}

pub(crate) fn commit_cluster_raft_command_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), CliError> {
    let entry =
        network::append_cluster_raft_command_uncommitted(paths, &state.local_node_id, command)?;
    let mut voter_acks = std::collections::BTreeSet::new();
    if cluster_raft_local_is_voter(state) {
        voter_acks.insert(state.local_node_id.clone());
    }
    for endpoint in cluster_sync_targets_with_connectivity(paths, state) {
        let Ok(response) = replicate_cluster_raft_to_endpoint(paths, state, &endpoint) else {
            continue;
        };
        if !response.accepted {
            continue;
        }
        if cluster_raft_node_is_voter(state, &endpoint.node_id) {
            voter_acks.insert(endpoint.node_id.clone());
        }
        network::merge_cluster_state_from_peer(state, response.state);
    }
    if voter_acks.len() < cluster_raft_quorum_size(state) {
        let _ = network::clear_cluster_raft_leader(paths, &state.local_node_id);
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "raft command at index {} replicated to {} voters, {} required",
                entry.index,
                voter_acks.len(),
                cluster_raft_quorum_size(state)
            ),
        });
    }

    let (snapshot, txn_response) =
        network::commit_cluster_raft_index(paths, &state.local_node_id, entry.index)?;
    for endpoint in cluster_sync_targets_with_connectivity(paths, state) {
        if let Ok(response) = replicate_cluster_raft_to_endpoint(paths, state, &endpoint) {
            if response.accepted {
                let _ = send_cluster_kv_watch_events(
                    &endpoint,
                    &ClusterKvWatchEventsRequest {
                        state: state.clone(),
                        leader_id: state.local_node_id.clone(),
                        events: snapshot.events.clone(),
                    },
                );
                network::merge_cluster_state_from_peer(state, response.state);
            }
        }
    }
    Ok((snapshot, txn_response))
}

pub(crate) fn commit_cluster_kv_txn_with_quorum(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
) -> Result<KvSnapshot, CliError> {
    if !cluster_has_local_quorum(paths, state) {
        return Err(CliError::ClusterReadOnly {
            detail: format!(
                "quorum unavailable ({} reachable voters, {} required)",
                cluster_reachable_voter_count(paths, state),
                cluster_raft_quorum_size(state)
            ),
        });
    }
    if !cluster_raft_local_is_leader(paths, state)? && !elect_cluster_raft_leader(paths, state)? {
        return Err(CliError::ClusterReadOnly {
            detail: "local daemon is not the Raft leader and could not win quorum".to_string(),
        });
    }
    commit_cluster_raft_command_with_quorum(paths, state, KvCommand::Txn(txn))
        .map(|(snapshot, _)| snapshot)
}

pub(crate) fn cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, CliError> {
    let node = network::open_cluster_raft_kv_node(paths, &state.local_node_id)?;
    Ok(node
        .hard_state()
        .leader_id
        .clone()
        .filter(|leader_id| cluster_raft_node_is_voter(state, leader_id)))
}

pub(crate) fn cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, CliError> {
    Ok(
        cluster_raft_known_leader_id(paths, state)?.as_ref() == Some(&state.local_node_id)
            && cluster_raft_local_is_voter(state),
    )
}

pub(crate) fn cluster_raft_local_is_voter(state: &ClusterBootstrapState) -> bool {
    cluster_raft_node_is_voter(state, &state.local_node_id)
}

pub(crate) fn cluster_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && matches!(member.role, ClusterMemberRole::Voter)
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == "daemon.main.run")
    })
}

pub(crate) fn cluster_raft_quorum_size(state: &ClusterBootstrapState) -> usize {
    let voters = state
        .members
        .iter()
        .filter(|member| cluster_raft_node_is_voter(state, &member.node_id))
        .map(|member| member.node_id.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    voters / 2 + 1
}

pub(crate) fn elect_cluster_raft_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, CliError> {
    if !cluster_raft_local_is_voter(state) {
        return Ok(false);
    }
    let (election_term, last_log_index, last_log_term) = {
        let node = network::open_cluster_raft_kv_node(paths, &state.local_node_id)?;
        (
            node.hard_state().current_term.saturating_add(1),
            node.last_log_index(),
            node.last_log_term(),
        )
    };
    let vote_request = ClusterRaftVoteRequest {
        state: state.clone(),
        vote: RaftRequestVoteRequest {
            term: election_term,
            candidate_id: state.local_node_id.clone(),
            last_log_index,
            last_log_term,
        },
    };
    let local_vote =
        network::apply_cluster_raft_vote(paths, &state.local_node_id, vote_request.vote.clone())?;
    let mut granted = std::collections::BTreeSet::new();
    if local_vote.vote_granted {
        granted.insert(state.local_node_id.clone());
    }
    for endpoint in cluster_raft_vote_targets(paths, state) {
        let Ok(response) = send_cluster_raft_vote(&endpoint, &vote_request) else {
            continue;
        };
        if response.vote.vote_granted {
            granted.insert(endpoint.node_id.clone());
        }
    }
    if granted.len() < cluster_raft_quorum_size(state) {
        return Ok(false);
    }
    network::set_cluster_raft_leader(paths, &state.local_node_id, election_term)?;
    Ok(true)
}

pub(crate) fn cluster_raft_vote_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_sync_targets_with_connectivity(paths, state)
        .into_iter()
        .filter(|endpoint| cluster_raft_node_is_voter(state, &endpoint.node_id))
        .collect()
}

pub(crate) fn build_cluster_raft_append_request_from_next_index(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    next_index: u64,
) -> Result<ClusterRaftAppendRequest, CliError> {
    let node = network::open_cluster_raft_kv_node(paths, &state.local_node_id)?;
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    let include_snapshot = node.log().is_empty()
        || (node.snapshot_index() > 0 && prev_log_index <= node.snapshot_index());
    let (snapshot, prev_log_index, entries) = if include_snapshot {
        let snapshot = RaftSnapshot {
            last_included_index: node.applied_index(),
            last_included_term: node.term_at(node.applied_index()).unwrap_or(1),
            kv: node.store().snapshot(),
        };
        let entries = node
            .log()
            .iter()
            .filter(|entry| entry.index > snapshot.last_included_index)
            .cloned()
            .collect::<Vec<_>>();
        (Some(snapshot), node.applied_index(), entries)
    } else {
        let entries = node
            .log()
            .iter()
            .filter(|entry| entry.index >= next_index)
            .cloned()
            .collect::<Vec<_>>();
        (None, prev_log_index, entries)
    };
    let prev_log_term = node.term_at(prev_log_index).unwrap_or(0);
    Ok(ClusterRaftAppendRequest {
        state: state.clone(),
        snapshot,
        append: RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: state.local_node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: node.hard_state().commit_index,
        },
    })
}

const CLUSTER_RAFT_APPEND_MAX_ATTEMPTS: usize = 8;

pub(crate) fn replicate_cluster_raft_to_endpoint(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) -> Result<ClusterRaftAppendResponse, CliError> {
    let mut next_index = network::open_cluster_raft_kv_node(paths, &state.local_node_id)?
        .last_log_index()
        .saturating_add(1);
    let mut last_response = None;
    for _ in 0..CLUSTER_RAFT_APPEND_MAX_ATTEMPTS {
        let request = build_cluster_raft_append_request_from_next_index(paths, state, next_index)?;
        let response = send_cluster_raft_append(endpoint, &request)?;
        if response.accepted {
            return Ok(response);
        }
        let rejected_next_index = response.append.match_index.saturating_add(1);
        next_index = if rejected_next_index < next_index {
            rejected_next_index
        } else {
            next_index.saturating_sub(1).max(1)
        };
        last_response = Some(response);
    }
    Ok(last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
        accepted: false,
        detail: "raft append was not attempted".to_string(),
        state: state.clone(),
        append: sinomo_runtime::kv::RaftAppendEntriesResponse {
            term: 0,
            success: false,
            match_index: 0,
        },
    }))
}

pub(crate) fn cluster_sync_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    network::cluster_sync_targets(state)
}

pub(crate) fn cluster_sync_targets_with_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = cluster_sync_targets(state);
    let graph = network::read_cluster_connectivity_graph_from_kv(paths).unwrap_or_default();
    endpoints.sort_by_key(|endpoint| {
        let score = graph
            .iter()
            .find(|record| {
                record.source_node_id == state.local_node_id
                    && record.target_node_id == endpoint.node_id
                    && record.endpoint_id == endpoint.endpoint_id
            })
            .map(|record| match record.state {
                network::ClusterConnectivityState::Healthy => 0,
                network::ClusterConnectivityState::Degraded => 1,
                network::ClusterConnectivityState::Unreachable => 3,
            })
            .unwrap_or(2);
        (
            cluster_daemon_command_transport_rank(endpoint),
            score,
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
}

pub(crate) fn prefer_bounded_daemon_command_targets(
    endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints: Vec<_> = endpoints.into_iter().collect();
    let mut nodes_with_bounded_transport = std::collections::BTreeSet::new();
    for endpoint in &endpoints {
        if cluster_daemon_command_transport_rank(endpoint) == 0 {
            nodes_with_bounded_transport.insert(endpoint.node_id.clone());
        }
    }
    endpoints.retain(|endpoint| {
        cluster_daemon_command_transport_rank(endpoint) == 0
            || !nodes_with_bounded_transport.contains(&endpoint.node_id)
    });
    endpoints
}

pub(crate) fn cluster_daemon_command_transport_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => 0,
        ClusterTransportKind::Iroh => 1,
        _ => 2,
    }
}

pub(crate) fn send_daemon_command_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    command: &str,
    timeout: Duration,
) -> Result<String, CliError> {
    let signed = sign_daemon_command_for_local_node(command)?;
    network::send_daemon_command_to_cluster_endpoint(endpoint, &signed, timeout).map_err(Into::into)
}

pub(crate) fn send_cluster_raft_append(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftAppendRequest,
) -> Result<ClusterRaftAppendResponse, CliError> {
    let payload = network::encode_object_text_payload(request)?;
    let response = send_daemon_command_to_endpoint(
        endpoint,
        &format!("CLUSTER_RAFT_APPEND {payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    network::decode_object_text_payload(response.trim()).map_err(Into::into)
}

pub(crate) fn send_cluster_raft_vote(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterRaftVoteRequest,
) -> Result<ClusterRaftVoteResponse, CliError> {
    let payload = network::encode_object_text_payload(request)?;
    let response = send_daemon_command_to_endpoint(
        endpoint,
        &format!("CLUSTER_RAFT_VOTE {payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    network::decode_object_text_payload(response.trim()).map_err(Into::into)
}

pub(crate) fn send_cluster_member_publish(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterMemberPublishRequest,
) -> Result<ClusterMemberPublishResponse, CliError> {
    let payload = network::encode_object_text_payload(request)?;
    let response = send_daemon_command_to_endpoint(
        endpoint,
        &format!("CLUSTER_MEMBER_PUBLISH {payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    network::decode_object_text_payload(response.trim()).map_err(Into::into)
}

pub(crate) fn send_cluster_kv_watch_events(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, CliError> {
    if request.events.is_empty() {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: true,
            detail: "no watch events to fan out".to_string(),
            stored_events: 0,
        });
    }
    let payload = network::encode_object_text_payload(request)?;
    let response = send_daemon_command_to_endpoint(
        endpoint,
        &format!("CLUSTER_KV_WATCH_EVENTS {payload}"),
        DAEMON_JOIN_TIMEOUT,
    )?;
    network::decode_object_text_payload(response.trim()).map_err(Into::into)
}

pub(crate) fn handle_daemon_cluster_raft_append(
    paths: &DaemonPaths,
    payload: &str,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterRaftAppendResponse, CliError> {
    let request: ClusterRaftAppendRequest = network::decode_object_text_payload(payload)?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut state = read_cluster_state(&cluster_paths)?;
    if request.state.cluster_id != state.cluster_id {
        let node = network::open_cluster_raft_kv_node(&cluster_paths, &state.local_node_id)?;
        return Ok(ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            state,
            append: sinomo_runtime::kv::RaftAppendEntriesResponse {
                term: node.hard_state().current_term,
                success: false,
                match_index: node.last_log_index(),
            },
        });
    }
    let leader_authorized_by_local_state =
        cluster_raft_node_is_voter(&state, &request.append.leader_id)
            && authenticated_signer_matches_cluster_node(
                &state,
                authenticated,
                &request.append.leader_id,
            );
    let leader_authorized_for_pending_join = cluster_state_needs_join_registration(&state)
        && request.append.leader_id == request.state.local_node_id
        && cluster_raft_node_is_voter(&request.state, &request.append.leader_id)
        && authenticated_signer_matches_cluster_node(
            &request.state,
            authenticated,
            &request.append.leader_id,
        );
    if !leader_authorized_by_local_state && !leader_authorized_for_pending_join {
        let node = network::open_cluster_raft_kv_node(&cluster_paths, &state.local_node_id)?;
        return Ok(ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
            state,
            append: sinomo_runtime::kv::RaftAppendEntriesResponse {
                term: node.hard_state().current_term,
                success: false,
                match_index: node.last_log_index(),
            },
        });
    }
    network::merge_cluster_state_from_peer(&mut state, request.state);
    network::write_cluster_state(&cluster_paths, &state)?;
    let append = network::apply_cluster_raft_append(
        &cluster_paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    Ok(ClusterRaftAppendResponse {
        accepted: append.success,
        detail: if append.success {
            "raft append applied".to_string()
        } else {
            "raft append rejected by local log".to_string()
        },
        state,
        append,
    })
}

pub(crate) fn handle_daemon_cluster_raft_vote(
    paths: &DaemonPaths,
    payload: &str,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterRaftVoteResponse, CliError> {
    let request: ClusterRaftVoteRequest = network::decode_object_text_payload(payload)?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut state = read_cluster_state(&cluster_paths)?;
    let local_node_id = state.local_node_id.clone();
    if request.state.cluster_id != state.cluster_id {
        let node = network::open_cluster_raft_kv_node(&cluster_paths, &local_node_id)?;
        return Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            state,
            vote: RaftRequestVoteResponse {
                term: node.hard_state().current_term,
                vote_granted: false,
            },
        });
    }
    let candidate_is_authorized_voter = state.members.iter().any(|member| {
        member.node_id == request.vote.candidate_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && matches!(member.role, ClusterMemberRole::Voter)
    });
    if !candidate_is_authorized_voter
        || !authenticated_signer_matches_cluster_node(
            &state,
            authenticated,
            &request.vote.candidate_id,
        )
    {
        let node = network::open_cluster_raft_kv_node(&cluster_paths, &local_node_id)?;
        return Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail: format!(
                "raft vote rejected for unauthorized or mismatched candidate {}",
                request.vote.candidate_id
            ),
            state,
            vote: RaftRequestVoteResponse {
                term: node.hard_state().current_term,
                vote_granted: false,
            },
        });
    }
    network::merge_cluster_state_from_peer(&mut state, request.state);
    network::write_cluster_state(&cluster_paths, &state)?;
    let vote = network::apply_cluster_raft_vote(&cluster_paths, &local_node_id, request.vote)?;
    Ok(ClusterRaftVoteResponse {
        accepted: vote.vote_granted,
        detail: if vote.vote_granted {
            "raft vote granted".to_string()
        } else {
            "raft vote rejected by local hard state".to_string()
        },
        state,
        vote,
    })
}

pub(crate) fn authenticated_signer_matches_cluster_node(
    state: &ClusterBootstrapState,
    authenticated: &AuthenticatedDaemonCommand,
    node_id: &NodeId,
) -> bool {
    if !matches!(
        authenticated.signer,
        DaemonSignerAuthority::Local | DaemonSignerAuthority::Voter
    ) {
        return false;
    }
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_id.as_str() == authenticated.signer_node_id
            && member.node_public_key == authenticated.signer_public_key
    })
}

pub(crate) fn handle_daemon_cluster_kv_watch_events(
    paths: &DaemonPaths,
    payload: &str,
) -> Result<ClusterKvWatchEventsResponse, CliError> {
    let request: ClusterKvWatchEventsRequest = network::decode_object_text_payload(payload)?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = read_cluster_state(&cluster_paths)?;
    if request.state.cluster_id != state.cluster_id {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
            stored_events: 0,
        });
    }
    if !cluster_raft_node_is_voter(&state, &request.leader_id) {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "watch event fan-out rejected from unauthorized leader {}",
                request.leader_id
            ),
            stored_events: 0,
        });
    }
    let stored_events = network::append_cluster_kv_watch_events(&cluster_paths, &request.events)?;
    Ok(ClusterKvWatchEventsResponse {
        accepted: true,
        detail: "cluster kv watch events stored".to_string(),
        stored_events,
    })
}

pub(crate) fn handle_daemon_cluster_invite_publish(
    paths: &DaemonPaths,
    payload: &str,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterInvitePublishResponse, CliError> {
    let request: ClusterInvitePublishRequest = network::decode_object_text_payload(payload)?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = read_cluster_state(&cluster_paths)?;
    if request.invite.cluster_id != state.cluster_id {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match local cluster `{}`",
                request.invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request.invite.expires_at <= OffsetDateTime::now_utc() {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                format_rfc3339(request.invite.expires_at)
            ),
            state,
            kv_snapshot: None,
        });
    }
    if let Err(error) = verify_cluster_join_invite_signature(&request.invite) {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == request.invite.issuer_node_id
            && member.node_public_key == request.invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                request.invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if authenticated.signer_node_id != request.invite.issuer_node_id.as_str()
        || authenticated.signer_public_key != request.invite.issuer_public_key
    {
        return Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: "invite publisher does not match invite issuer".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    match publish_cluster_invitation_to_quorum(&cluster_paths, &state, &request.invite) {
        Ok(()) => Ok(ClusterInvitePublishResponse {
            accepted: true,
            detail: "invite published".to_string(),
            state: read_cluster_state(&cluster_paths).unwrap_or(state),
            kv_snapshot: read_cluster_kv_snapshot(&cluster_paths).ok(),
        }),
        Err(error) => Ok(ClusterInvitePublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

pub(crate) fn handle_daemon_cluster_member_publish(
    paths: &DaemonPaths,
    payload: &str,
    authenticated: &AuthenticatedDaemonCommand,
) -> Result<ClusterMemberPublishResponse, CliError> {
    let request: ClusterMemberPublishRequest = network::decode_object_text_payload(payload)?;
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let state = read_cluster_state(&cluster_paths)?;
    if request.members.is_empty() {
        return Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: "member publish request was empty".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    for member in &request.members {
        if member.cluster_id != state.cluster_id {
            return Ok(ClusterMemberPublishResponse {
                accepted: false,
                detail: format!(
                    "member {} cluster `{}` does not match local cluster `{}`",
                    member.node_id, member.cluster_id, state.cluster_id
                ),
                state,
                kv_snapshot: None,
            });
        }
        if member.node_id.as_str() != authenticated.signer_node_id
            && !member
                .node_id
                .as_str()
                .starts_with(&format!("{}-", authenticated.signer_node_id))
        {
            return Ok(ClusterMemberPublishResponse {
                accepted: false,
                detail: format!(
                    "member publisher {} cannot publish member {}",
                    authenticated.signer_node_id, member.node_id
                ),
                state,
                kv_snapshot: None,
            });
        }
        if !state.members.iter().any(|existing| {
            existing.node_id == member.node_id
                && existing.node_public_key == member.node_public_key
                && existing.state.is_authorized()
        }) {
            return Ok(ClusterMemberPublishResponse {
                accepted: false,
                detail: format!(
                    "member {} is not an authorized cluster member with the published public key",
                    member.node_id
                ),
                state,
                kv_snapshot: None,
            });
        }
    }
    if cluster_member_publish_matches_kv(&cluster_paths, &request.members)? {
        return Ok(ClusterMemberPublishResponse {
            accepted: true,
            detail: "member endpoints already current".to_string(),
            state,
            kv_snapshot: Some(read_cluster_kv_snapshot(&cluster_paths)?),
        });
    }
    let mut commit_state = state.clone();
    match commit_cluster_kv_txn_with_quorum(
        &cluster_paths,
        &mut commit_state,
        cluster_member_publish_txn(&request.members)?,
    ) {
        Ok(snapshot) => Ok(ClusterMemberPublishResponse {
            accepted: true,
            detail: "member endpoints published".to_string(),
            state: read_cluster_state(&cluster_paths).unwrap_or(state),
            kv_snapshot: Some(snapshot),
        }),
        Err(error) => Ok(ClusterMemberPublishResponse {
            accepted: false,
            detail: error.to_string(),
            state,
            kv_snapshot: None,
        }),
    }
}

fn cluster_member_publish_matches_kv(
    paths: &ClusterPaths,
    members: &[ClusterMemberRecord],
) -> Result<bool, CliError> {
    let store = network::read_cluster_kv_snapshot_store(paths)?;
    for member in members {
        let mut member = member.clone();
        network::cleanup_member_iroh_endpoints(&mut member);
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));

        let Some(entry) = store.get(&cluster_member_key(&member.node_id)) else {
            return Ok(false);
        };
        let Ok(mut existing) = entry.decode::<ClusterMemberRecord>() else {
            return Ok(false);
        };
        network::cleanup_member_iroh_endpoints(&mut existing);
        existing
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        if !cluster_member_stable_eq(&existing, &member) {
            return Ok(false);
        }

        let mut existing_endpoints = store
            .range_prefix(&network::cluster_endpoint_prefix(&member.node_id))
            .into_iter()
            .filter_map(|entry| entry.decode::<ClusterEndpointRecord>().ok())
            .collect::<Vec<_>>();
        existing_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        if existing_endpoints != member.endpoints {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonClusterProcess {
    Main,
    Shadow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DaemonClusterRegistration {
    pub(crate) process: DaemonClusterProcess,
    pub(crate) main_options: DaemonRunOptions,
    pub(crate) shadow_options: DaemonRunOptions,
}

pub(crate) fn ensure_daemon_cluster_members(
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> bool {
    let main_changed = ensure_main_daemon_cluster_member(state, main_options, timestamp);
    let shadow_changed = ensure_shadow_daemon_cluster_member(state, shadow_options, timestamp);
    let changed = main_changed || shadow_changed;
    if changed {
        state.updated_at = timestamp;
    }
    changed
}

pub(crate) fn local_daemon_cluster_options()
-> Result<(DaemonRunOptions, DaemonRunOptions), CliError> {
    let main = match default_main_daemon_status()? {
        Some(status) => daemon_options_from_status(&status),
        None => DaemonRunOptions::default(),
    };
    let shadow =
        match probe_daemon_status(&shadow_daemon_paths_from_defaults()?, DAEMON_PROBE_TIMEOUT) {
            DaemonProbeStatus::Running(status)
            | DaemonProbeStatus::Locked {
                status: Some(status),
            } => daemon_options_from_status(&status),
            DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
                DaemonRunOptions::shadow_default()
            }
        };
    Ok((main, shadow))
}

pub(crate) fn daemon_options_from_status(status: &DaemonStatus) -> DaemonRunOptions {
    DaemonRunOptions {
        host: status.host.clone(),
        port: status.port,
    }
}

pub(crate) fn ensure_main_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> bool {
    let local_node_id = state.local_node_id.clone();
    let existing = state
        .members
        .iter()
        .find(|member| member.node_id == local_node_id);
    let role = existing
        .map(|member| member.role.clone())
        .unwrap_or(ClusterMemberRole::Learner);
    let member_state = existing
        .map(|member| member.state.clone())
        .unwrap_or(ClusterMemberState::Pending);
    let node_public_key = existing_member_public_key(state, &local_node_id).unwrap_or_default();
    let mut member = ClusterMemberRecord::local_voter(
        state.cluster_id.clone(),
        local_node_id.clone(),
        state.local_node_display_name.clone(),
        node_public_key,
        timestamp,
    );
    let capabilities = main_daemon_capabilities(&role);
    member.role = role;
    member.state = member_state;
    upsert_daemon_member(
        state,
        member,
        capabilities,
        daemon_advertised_endpoints_for_state(
            state,
            &local_node_id,
            options,
            0,
            &discover_lan_ip_candidates(),
            &discover_peer_route_ip_candidates(state),
        ),
        timestamp,
    )
}

pub(crate) fn ensure_shadow_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> bool {
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let existing_shadow_state = state
        .members
        .iter()
        .find(|member| member.node_id == shadow_node_id)
        .map(|member| member.state.clone())
        .filter(|state| *state != ClusterMemberState::Pending);
    let node_public_key = existing_member_public_key(state, &shadow_node_id).unwrap_or_default();
    let main_authorized = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .is_some_and(|member| member.state.is_authorized());
    let mut member = if main_authorized {
        ClusterMemberRecord::local_non_voting(
            state.cluster_id.clone(),
            shadow_node_id.clone(),
            format!("{} shadow", state.local_node_display_name),
            node_public_key,
            timestamp,
        )
    } else {
        ClusterMemberRecord::pending_non_voting(
            state.cluster_id.clone(),
            shadow_node_id.clone(),
            format!("{} shadow", state.local_node_display_name),
            node_public_key,
            timestamp,
        )
    };
    if let Some(existing_state) = existing_shadow_state {
        member.state = existing_state;
    }
    upsert_daemon_member(
        state,
        member,
        shadow_daemon_capabilities(),
        daemon_advertised_endpoints_for_state(
            state,
            &shadow_node_id,
            options,
            10,
            &discover_lan_ip_candidates(),
            &discover_peer_route_ip_candidates(state),
        ),
        timestamp,
    )
}

pub(crate) fn existing_member_public_key(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Option<String> {
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.node_public_key.clone())
        .filter(|key| !key.trim().is_empty())
}

pub(crate) fn upsert_daemon_member(
    state: &mut ClusterBootstrapState,
    replacement: ClusterMemberRecord,
    capabilities: Vec<CapabilityName>,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == replacement.node_id)
    {
        let mut next_endpoints = member.endpoints.clone();
        for endpoint in endpoints {
            next_endpoints.retain(|candidate| candidate.endpoint_id != endpoint.endpoint_id);
            next_endpoints.push(endpoint);
        }
        next_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        let changed = member.display_name != replacement.display_name
            || member.role != replacement.role
            || member.state != replacement.state
            || member.node_public_key != replacement.node_public_key
            || member.capabilities != capabilities
            || member.allowed_capabilities != capabilities
            || member.endpoints != next_endpoints;
        if changed {
            member.display_name = replacement.display_name;
            member.role = replacement.role;
            member.state = replacement.state;
            member.node_public_key = replacement.node_public_key;
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = capabilities;
            member.endpoints = next_endpoints;
            member.updated_at = timestamp;
        }
        changed
    } else {
        let mut member = replacement;
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        member.endpoints = endpoints;
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        state.members.push(member);
        true
    }
}

pub(crate) fn daemon_advertised_endpoints_for_state(
    _state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    daemon_advertised_endpoints_from_ips(node_id, options, priority, lan_ips, route_ips)
}

pub(crate) fn daemon_advertised_endpoints_from_ips(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = vec![daemon_loopback_endpoint(node_id, options, priority)];
    let mut lan_addresses = Vec::new();
    if matches!(options.host.as_str(), "0.0.0.0" | "::") {
        for ip in lan_ips.iter().chain(route_ips.iter()).copied() {
            if is_invitable_ip(ip) {
                push_unique_address(&mut lan_addresses, format_ip_port(ip, options.port));
            }
        }
    } else if options.host.parse::<IpAddr>().is_ok_and(is_invitable_ip)
        || !is_loopback_connect_address(&daemon_connect_address(&options.host, options.port))
    {
        push_unique_address(
            &mut lan_addresses,
            format!("{}:{}", options.host, options.port),
        );
    }

    for address in lan_addresses {
        endpoints.push(daemon_lan_endpoint(
            node_id,
            address,
            priority.saturating_add(20),
        ));
    }
    endpoints
}

pub(crate) fn push_unique_address(addresses: &mut Vec<String>, address: String) {
    if !addresses.iter().any(|candidate| candidate == &address) {
        addresses.push(address);
    }
}

pub(crate) fn advertised_peer_route_targets(state: &ClusterBootstrapState) -> Vec<SocketAddr> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        for endpoint in &member.endpoints {
            if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                continue;
            }
            if !matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
            ) {
                continue;
            }
            let Ok(address) = endpoint.address.parse::<SocketAddr>() else {
                continue;
            };
            if is_invitable_ip(address.ip()) && !targets.contains(&address) {
                targets.push(address);
            }
        }
    }
    targets.sort();
    targets
}

pub(crate) fn discover_peer_route_ip_candidates(state: &ClusterBootstrapState) -> Vec<IpAddr> {
    let mut candidates = Vec::new();
    for target in advertised_peer_route_targets(state) {
        let bind = if target.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let Ok(socket) = UdpSocket::bind(bind) else {
            continue;
        };
        if socket.connect(target).is_err() {
            continue;
        }
        let Ok(address) = socket.local_addr() else {
            continue;
        };
        let ip = address.ip();
        if is_invitable_ip(ip) && !candidates.contains(&ip) {
            candidates.push(ip);
        }
    }
    candidates
}

pub(crate) fn daemon_loopback_endpoint(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: daemon_loopback_endpoint_id(node_id),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::Loopback,
        address: daemon_connect_address(&options.host, options.port),
        source: ClusterEndpointSource::Static,
        privacy: ClusterEndpointPrivacy::LocalOnly,
        priority,
        last_observed_latency_ms: None,
    }
}

pub(crate) fn daemon_lan_endpoint(
    node_id: &NodeId,
    address: String,
    priority: u16,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: daemon_lan_endpoint_id(node_id, &address),
        node_id: node_id.clone(),
        transport: ClusterTransportKind::LanTcp,
        address,
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority,
        last_observed_latency_ms: None,
    }
}

pub(crate) fn daemon_cluster_status(
    state: &ClusterBootstrapState,
    process: DaemonClusterProcess,
) -> Option<DaemonClusterStatus> {
    let node_id = match process {
        DaemonClusterProcess::Main => state.local_node_id.clone(),
        DaemonClusterProcess::Shadow => shadow_daemon_node_id(&state.local_node_id),
    };
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == node_id)?;
    Some(DaemonClusterStatus {
        cluster_id: state.cluster_id.clone(),
        node_id,
        process: match process {
            DaemonClusterProcess::Main => DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            DaemonClusterProcess::Shadow => DAEMON_SHADOW_CLUSTER_ROLE.to_string(),
        },
        member_role: member.role.clone(),
        member_state: member.state.clone(),
    })
}

pub(crate) fn register_daemon_cluster_node(
    registration: &DaemonClusterRegistration,
) -> Result<DaemonClusterStatus, CliError> {
    let paths = cluster_paths_from_defaults()?;
    let mut state = read_cluster_state(&paths)?;
    ensure_daemon_cluster_members(
        &mut state,
        &registration.main_options,
        &registration.shadow_options,
        OffsetDateTime::now_utc(),
    );
    write_json(&paths.state, &state)?;
    daemon_cluster_status(&state, registration.process).ok_or_else(|| CliError::Parse {
        detail: "daemon cluster member could not be registered".to_string(),
    })
}

pub(crate) fn advertise_daemon_iroh_endpoint(
    paths: &DaemonPaths,
    record: network::IrohEndpointRecord,
) -> Result<(), CliError> {
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let mut state = read_cluster_state(&cluster_paths)?;
    let endpoint = record.endpoint();
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)
    {
        network::upsert_member_endpoint(member, endpoint);
        network::cleanup_member_iroh_endpoints(member);
        state.updated_at = OffsetDateTime::now_utc();
        network::write_cluster_state(&cluster_paths, &state)?;
        sync_cluster_state_to_kv(&cluster_paths, &state)?;
    }
    Ok(())
}

pub(crate) fn daemon_cluster_registration_if_initialized(
    registration: DaemonClusterRegistration,
) -> Result<Option<DaemonClusterRegistration>, CliError> {
    let paths = cluster_paths_from_defaults()?;
    if paths.state.exists() {
        Ok(Some(registration))
    } else {
        Ok(None)
    }
}

pub(crate) fn refresh_daemon_node_status_in_kv(
    paths: &DaemonPaths,
) -> Result<Option<KvSnapshot>, CliError> {
    let mut status = read_daemon_status(paths)?;
    ensure_daemon_status_cluster(paths, &mut status)?;
    let Some(cluster) = status.cluster.as_ref() else {
        return Ok(None);
    };
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    let state = read_cluster_state_without_kv(&cluster_paths)?;
    if !cluster_has_local_quorum(&cluster_paths, &state) {
        return Ok(None);
    }
    let _ = sync_cluster_network_info_to_kv(
        &cluster_paths,
        build_local_cluster_network_info(&state, &cluster.node_id, OffsetDateTime::now_utc()),
    );
    let record = cluster_node_status_from_daemon_status(
        &status,
        cluster,
        read_cluster_rollout_status(&rollout_paths)?,
        OffsetDateTime::now_utc(),
    );
    sync_cluster_node_status_to_kv(&cluster_paths, record)
}

pub(crate) fn ensure_daemon_status_cluster(
    paths: &DaemonPaths,
    status: &mut DaemonStatus,
) -> Result<(), CliError> {
    if status.cluster.is_some() {
        return Ok(());
    }
    let data_dir = paths.root.parent().ok_or_else(|| CliError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon paths have no data directory parent".to_string(),
    })?;
    let cluster_paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let mut state = read_cluster_state(&cluster_paths)?;
    let process = daemon_cluster_process_from_paths(data_dir, paths);
    let (main_options, shadow_options) =
        daemon_cluster_options_from_status(data_dir, paths, status, process);
    ensure_daemon_cluster_members(
        &mut state,
        &main_options,
        &shadow_options,
        OffsetDateTime::now_utc(),
    );
    assign_local_node_public_keys(&cluster_paths, &mut state)?;
    network::write_cluster_state(&cluster_paths, &state)?;
    if let Some(cluster) = daemon_cluster_status(&state, process) {
        status.cluster = Some(cluster);
        write_daemon_status(paths, status)?;
    }
    Ok(())
}

pub(crate) fn daemon_cluster_process_from_paths(
    data_dir: &Path,
    paths: &DaemonPaths,
) -> DaemonClusterProcess {
    if paths.root == data_dir.join("shadow-daemon") {
        DaemonClusterProcess::Shadow
    } else {
        DaemonClusterProcess::Main
    }
}

pub(crate) fn daemon_cluster_options_from_status(
    data_dir: &Path,
    paths: &DaemonPaths,
    status: &DaemonStatus,
    process: DaemonClusterProcess,
) -> (DaemonRunOptions, DaemonRunOptions) {
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let main_options = if process == DaemonClusterProcess::Main {
        daemon_options_from_status(status)
    } else {
        read_daemon_status(&main_paths)
            .map(|status| daemon_options_from_status(&status))
            .unwrap_or_else(|_| DaemonRunOptions::default())
    };
    let shadow_options = if paths.root == shadow_paths.root {
        daemon_options_from_status(status)
    } else {
        read_daemon_status(&shadow_paths)
            .map(|status| daemon_options_from_status(&status))
            .unwrap_or_else(|_| DaemonRunOptions::shadow_default())
    };
    (main_options, shadow_options)
}

pub(crate) fn cluster_node_status_from_daemon_status(
    status: &DaemonStatus,
    cluster: &DaemonClusterStatus,
    rollout: Option<ClusterNodeRolloutStatus>,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    ClusterNodeStatusRecord {
        cluster_id: cluster.cluster_id.clone(),
        node_id: cluster.node_id.clone(),
        process: cluster.process.clone(),
        availability: ClusterNodeAvailability::Online,
        daemon_version: status.version.clone(),
        target_os: status.target_os.clone(),
        target_arch: status.target_arch.clone(),
        pid: Some(status.pid),
        address: Some(status.address.clone()),
        bind_address: Some(daemon_bind_address(status)),
        executable: status
            .executable
            .as_ref()
            .map(|path| path.display().to_string()),
        rollout: rollout.or_else(|| status.rollout.clone()),
        daemon_started_at: Some(status.started_at.clone()),
        observed_at,
    }
}

pub(crate) fn read_cluster_rollout_status(
    paths: &SelfRolloutPaths,
) -> Result<Option<ClusterNodeRolloutStatus>, CliError> {
    if !paths.state.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&paths.state).map_err(|source| CliError::Io {
        path: paths.state.display().to_string(),
        detail: source.to_string(),
    })?;
    let value: serde_json::Value =
        serde_json::from_str(&content).map_err(|source| CliError::Json {
            path: paths.state.display().to_string(),
            detail: source.to_string(),
        })?;
    let Some(status) = json_string_field(&value, "status") else {
        return Ok(None);
    };
    Ok(Some(ClusterNodeRolloutStatus {
        status,
        mode: json_string_field(&value, "mode"),
        detail: json_string_field(&value, "detail"),
        source: json_string_field(&value, "source")
            .or_else(|| json_string_field(&value, "source_repo")),
        requested_at: json_string_field(&value, "requested_at"),
        staged_at: json_string_field(&value, "staged_at"),
        updated_at: json_string_field(&value, "updated_at"),
        failed_at: json_string_field(&value, "failed_at"),
        diagnostics: json_string_field(&value, "diagnostics"),
        stdout_log: json_string_field(&value, "stdout_log"),
        stderr_log: json_string_field(&value, "stderr_log"),
        stdout_tail: json_string_field(&value, "stdout_tail"),
        stderr_tail: json_string_field(&value, "stderr_tail"),
    }))
}

pub(crate) fn json_string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(|field| match field {
        serde_json::Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    })
}

pub(crate) fn daemon_cluster_registration(
    process: DaemonClusterProcess,
    main_options: DaemonRunOptions,
    shadow_options: DaemonRunOptions,
) -> DaemonClusterRegistration {
    DaemonClusterRegistration {
        process,
        main_options,
        shadow_options,
    }
}

pub(crate) fn format_member_role(role: &ClusterMemberRole) -> &'static str {
    match role {
        ClusterMemberRole::Voter => "voter",
        ClusterMemberRole::Learner => "learner",
        ClusterMemberRole::NonVoting => "non-voting",
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

pub(crate) fn invite_channels(invite: &ClusterJoinInvite) -> Vec<ClusterInvitationChannel> {
    if !invite.communication_channels.is_empty() {
        return invite.communication_channels.clone();
    }
    invite
        .introducers
        .iter()
        .cloned()
        .map(|endpoint| ClusterInvitationChannel {
            endpoint,
            node_public_key: invite.issuer_public_key.clone(),
        })
        .collect()
}
