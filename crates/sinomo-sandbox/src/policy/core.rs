use crate::internal::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxNodeAvailability {
    Online,
    Offline,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxNodeCandidate {
    pub node_id: NodeId,
    pub availability: SandboxNodeAvailability,
}

impl SandboxNodeCandidate {
    pub fn online(node_id: NodeId) -> Self {
        Self {
            node_id,
            availability: SandboxNodeAvailability::Online,
        }
    }

    pub fn offline(node_id: NodeId) -> Self {
        Self {
            node_id,
            availability: SandboxNodeAvailability::Offline,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxPlacementReason {
    AnyEligibleNode,
    LocalOwnedMountOwner,
    RemoteLocalOwnedMountAllowed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxExecutionPlacement {
    pub node_id: NodeId,
    pub reason: SandboxPlacementReason,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local_owned_mount_nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxSchedulingError {
    pub code: String,
    pub detail: String,
}

impl SandboxSchedulingError {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl fmt::Display for SandboxSchedulingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for SandboxSchedulingError {}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxPolicy {
    #[serde(default)]
    pub allowed_mount_prefixes: Vec<String>,
    #[serde(default)]
    pub allow_remote_local_owned_mounts: bool,
    #[serde(default)]
    pub allowed_command_capabilities: BTreeSet<String>,
    #[serde(default)]
    pub allowed_toolchain_ids: BTreeSet<SandboxToolchainId>,
    #[serde(default)]
    pub allowed_toolchain_provenance_prefixes: BTreeSet<String>,
    #[serde(default = "default_sandbox_package_cache_prefixes")]
    pub allowed_package_cache_prefixes: Vec<String>,
    #[serde(default)]
    pub allow_unsigned_toolchains: bool,
    #[serde(default)]
    pub allowed_http_hosts: BTreeSet<String>,
    #[serde(default = "default_sandbox_http_request_bytes")]
    pub max_http_request_bytes: u64,
    #[serde(default = "default_sandbox_http_response_bytes")]
    pub max_http_response_bytes: u64,
    #[serde(default = "default_sandbox_http_timeout_ms")]
    pub max_http_timeout_ms: u64,
    #[serde(default = "default_sandbox_max_concurrent_commands_per_sandbox")]
    pub max_concurrent_commands_per_sandbox: u64,
    #[serde(default = "default_sandbox_max_concurrent_sandboxes_per_node")]
    pub max_concurrent_sandboxes_per_node: u64,
    pub max_limits: SandboxResourceLimits,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            allowed_mount_prefixes: vec![SIFS_SANDBOX_ROOT.to_string()],
            allow_remote_local_owned_mounts: false,
            allowed_command_capabilities: BTreeSet::new(),
            allowed_toolchain_ids: BTreeSet::new(),
            allowed_toolchain_provenance_prefixes: BTreeSet::new(),
            allowed_package_cache_prefixes: default_sandbox_package_cache_prefixes(),
            allow_unsigned_toolchains: false,
            allowed_http_hosts: BTreeSet::new(),
            max_http_request_bytes: default_sandbox_http_request_bytes(),
            max_http_response_bytes: default_sandbox_http_response_bytes(),
            max_http_timeout_ms: default_sandbox_http_timeout_ms(),
            max_concurrent_commands_per_sandbox:
                default_sandbox_max_concurrent_commands_per_sandbox(),
            max_concurrent_sandboxes_per_node: default_sandbox_max_concurrent_sandboxes_per_node(),
            max_limits: SandboxResourceLimits::default(),
        }
    }
}

pub fn default_sandbox_http_request_bytes() -> u64 {
    1024 * 1024
}

pub fn default_sandbox_http_response_bytes() -> u64 {
    16 * 1024 * 1024
}

pub fn default_sandbox_http_timeout_ms() -> u64 {
    10_000
}

pub fn default_sandbox_max_concurrent_commands_per_sandbox() -> u64 {
    1
}

pub fn default_sandbox_max_concurrent_sandboxes_per_node() -> u64 {
    8
}

pub fn default_sandbox_package_cache_prefixes() -> Vec<String> {
    vec![format!("{SIFS_SANDBOX_ROOT}/toolchains")]
}

pub fn select_sandbox_execution_node(
    manifest: &SandboxInstanceManifest,
    candidates: &[SandboxNodeCandidate],
    policy: &SandboxPolicy,
) -> Result<SandboxExecutionPlacement, SandboxSchedulingError> {
    let mut local_owned_nodes = local_owned_mount_nodes(&manifest.mounts)?;
    local_owned_nodes.sort();
    local_owned_nodes.dedup();

    if local_owned_nodes.len() == 1 {
        let owner = &local_owned_nodes[0];
        if let Some(candidate) = candidates
            .iter()
            .find(|candidate| &candidate.node_id == owner)
        {
            if candidate.availability == SandboxNodeAvailability::Online {
                policy
                    .validate_manifest(manifest, owner)
                    .map_err(|denial| {
                        SandboxSchedulingError::new("policy-blocked", denial.to_string())
                    })?;
                return Ok(SandboxExecutionPlacement {
                    node_id: owner.clone(),
                    reason: SandboxPlacementReason::LocalOwnedMountOwner,
                    local_owned_mount_nodes: local_owned_nodes.clone(),
                });
            }
            if !policy.allow_remote_local_owned_mounts {
                return Err(SandboxSchedulingError::new(
                    "unavailable-offline",
                    format!("local-owned mount owner {owner} is offline"),
                ));
            }
        } else if !policy.allow_remote_local_owned_mounts {
            return Err(SandboxSchedulingError::new(
                "unavailable-offline",
                format!("local-owned mount owner {owner} is not a scheduling candidate"),
            ));
        }
    } else if local_owned_nodes.len() > 1 && !policy.allow_remote_local_owned_mounts {
        return Err(SandboxSchedulingError::new(
            "policy-blocked",
            format!(
                "sandbox mounts local-owned subtrees from multiple owner nodes: {}",
                local_owned_nodes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }

    let mut first_policy_denial = None;
    for candidate in candidates {
        if candidate.availability != SandboxNodeAvailability::Online {
            continue;
        }
        match policy.validate_manifest(manifest, &candidate.node_id) {
            Ok(()) => {
                let reason = if local_owned_nodes.is_empty() {
                    SandboxPlacementReason::AnyEligibleNode
                } else {
                    SandboxPlacementReason::RemoteLocalOwnedMountAllowed
                };
                return Ok(SandboxExecutionPlacement {
                    node_id: candidate.node_id.clone(),
                    reason,
                    local_owned_mount_nodes: local_owned_nodes.clone(),
                });
            }
            Err(denial) => {
                if first_policy_denial.is_none() {
                    first_policy_denial = Some(denial);
                }
            }
        }
    }

    if let Some(denial) = first_policy_denial {
        return Err(SandboxSchedulingError::new(
            "policy-blocked",
            denial.to_string(),
        ));
    }
    Err(SandboxSchedulingError::new(
        "unavailable-offline",
        "no online sandbox execution candidate is available",
    ))
}

pub fn sandbox_execution_placement_for_node(
    manifest: &SandboxInstanceManifest,
    node_id: &NodeId,
) -> Result<SandboxExecutionPlacement, SandboxSchedulingError> {
    let mut local_owned_nodes = local_owned_mount_nodes(&manifest.mounts)?;
    local_owned_nodes.sort();
    local_owned_nodes.dedup();
    let reason = if local_owned_nodes.is_empty() {
        SandboxPlacementReason::AnyEligibleNode
    } else if local_owned_nodes.len() == 1 && local_owned_nodes[0] == *node_id {
        SandboxPlacementReason::LocalOwnedMountOwner
    } else {
        SandboxPlacementReason::RemoteLocalOwnedMountAllowed
    };
    Ok(SandboxExecutionPlacement {
        node_id: node_id.clone(),
        reason,
        local_owned_mount_nodes: local_owned_nodes,
    })
}

impl SandboxPolicy {
    pub fn validate_invocation(
        &self,
        instance: &SandboxInstanceManifest,
        toolchain: &SandboxToolchainManifest,
        command: &SandboxToolchainCommand,
        execution_node_id: &NodeId,
    ) -> Result<(), SandboxPolicyDenial> {
        self.validate_manifest(instance, execution_node_id)?;
        self.validate_toolchain_invocation_authority(toolchain)?;
        self.validate_command(command, &command.effective_target_abi(toolchain))?;
        self.validate_resource_limits(&effective_sandbox_command_limits(
            &instance.limits,
            &command.default_limits,
        ))
    }

    pub fn validate_manifest(
        &self,
        manifest: &SandboxInstanceManifest,
        execution_node_id: &NodeId,
    ) -> Result<(), SandboxPolicyDenial> {
        self.validate_resource_limits(&manifest.limits)?;
        self.validate_mounts(&manifest.mounts, execution_node_id)
    }

    pub fn validate_mounts(
        &self,
        mounts: &[SandboxMount],
        execution_node_id: &NodeId,
    ) -> Result<(), SandboxPolicyDenial> {
        let mut mount_paths = BTreeSet::new();
        for mount in mounts {
            if mount.mount_path.as_str() == "/" {
                return Err(SandboxPolicyDenial::new(
                    "mount-root-denied",
                    "mounts cannot replace the sandbox root",
                ));
            }
            if !mount_paths.insert(mount.mount_path.clone()) {
                return Err(SandboxPolicyDenial::new(
                    "mount-duplicate",
                    format!("duplicate mount path {}", mount.mount_path),
                ));
            }
            let source = normalize_sandbox_sifs_root(&mount.source_sifs_path);
            if !self
                .allowed_mount_prefixes
                .iter()
                .any(|prefix| sifs_path_has_prefix(&source, prefix))
            {
                return Err(SandboxPolicyDenial::new(
                    "mount-source-denied",
                    format!("mount source {source} is not allowed by policy"),
                ));
            }
            if let Some(owner_node_id) = local_owned_mount_node_id(&source)
                && !self.allow_remote_local_owned_mounts
                && owner_node_id != execution_node_id.as_str()
            {
                return Err(SandboxPolicyDenial::new(
                    "mount-local-owned-remote-denied",
                    format!(
                        "mount source {source} is owned by node {owner_node_id}, not execution node {execution_node_id}"
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_command(
        &self,
        command: &SandboxToolchainCommand,
        target_abi: &SandboxWasiAbi,
    ) -> Result<(), SandboxPolicyDenial> {
        if sandbox_command_requires_child_process_substrate(&command.name)
            && !sandbox_runtime_supports_child_processes_for_abi(target_abi)
        {
            return Err(SandboxPolicyDenial::new(
                "command-capability-unsupported",
                format!(
                    "command {} requires a real child-process execution substrate, but the {target_abi:?} sandbox runtime does not support it",
                    command.name
                ),
            ));
        }
        for capability in &command.required_capabilities {
            if !sandbox_runtime_supports_command_capability_for_abi(capability, target_abi) {
                return Err(SandboxPolicyDenial::new(
                    "command-capability-unsupported",
                    format!(
                        "command {} requires capability {capability}, but the {target_abi:?} sandbox runtime does not support it",
                        command.name
                    ),
                ));
            }
            if !self.allowed_command_capabilities.contains(capability) {
                return Err(SandboxPolicyDenial::new(
                    "command-capability-denied",
                    format!(
                        "command {} requires capability {capability}, which is not allowed",
                        command.name
                    ),
                ));
            }
            if capability == SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY
                && matches!(target_abi, SandboxWasiAbi::Wasix)
                && !command.required_capabilities.iter().any(|capability| {
                    capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                        || capability == SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY
                })
            {
                return Err(SandboxPolicyDenial::new(
                    "command-capability-missing",
                    format!(
                        "command {} requires capability {capability}, and WASIX child exec must choose a policy mode; the command manifest must explicitly declare either {} or {}",
                        command.name,
                        SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY,
                        SANDBOX_PINNED_CHILD_EXEC_CONTAINMENT_CAPABILITY
                    ),
                ));
            }
            if capability == SANDBOX_CHILD_PROCESS_EXEC_CAPABILITY
                && matches!(target_abi, SandboxWasiAbi::Wasix)
                && command
                    .required_capabilities
                    .iter()
                    .any(|capability| capability == SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
                && !self
                    .allowed_command_capabilities
                    .contains(SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY)
            {
                return Err(SandboxPolicyDenial::new(
                    "command-capability-denied",
                    format!(
                        "command {} requires capability {capability}, but this command can load absolute child executables from sandbox mounts; explicitly allow {} to enable sandbox-local child exec",
                        command.name, SANDBOX_WASIX_UNPINNED_CHILD_EXEC_CAPABILITY
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_toolchain(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<(), SandboxPolicyDenial> {
        self.validate_toolchain_invocation_authority(manifest)?;
        for command in manifest.commands.values() {
            self.validate_resource_limits(&command.default_limits)?;
        }
        Ok(())
    }

    fn validate_toolchain_invocation_authority(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<(), SandboxPolicyDenial> {
        self.validate_toolchain_update_authority(manifest)?;
        if !self.allow_unsigned_toolchains && manifest.signature.is_none() {
            return Err(SandboxPolicyDenial::new(
                "toolchain-unsigned-denied",
                format!("toolchain {} is not signed", manifest.toolchain_id),
            ));
        }
        Ok(())
    }

    pub fn validate_toolchain_update(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<(), SandboxPolicyDenial> {
        self.validate_toolchain_update_authority(manifest)?;
        for command in manifest.commands.values() {
            self.validate_resource_limits(&command.default_limits)?;
        }
        Ok(())
    }

    fn validate_toolchain_update_authority(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<(), SandboxPolicyDenial> {
        if !self.allowed_toolchain_ids.is_empty()
            && !self.allowed_toolchain_ids.contains(&manifest.toolchain_id)
        {
            return Err(SandboxPolicyDenial::new(
                "toolchain-denied",
                format!(
                    "toolchain {} is not allowed by policy",
                    manifest.toolchain_id
                ),
            ));
        }
        if !self.allowed_toolchain_provenance_prefixes.is_empty()
            && !self
                .allowed_toolchain_provenance_prefixes
                .iter()
                .any(|prefix| manifest.provenance.starts_with(prefix))
        {
            return Err(SandboxPolicyDenial::new(
                "toolchain-provenance-denied",
                format!(
                    "toolchain {} provenance `{}` is not allowed by policy",
                    manifest.toolchain_id, manifest.provenance
                ),
            ));
        }
        self.validate_toolchain_package_cache_paths(manifest)?;
        Ok(())
    }

    fn validate_toolchain_package_cache_paths(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<(), SandboxPolicyDenial> {
        for command in manifest.commands.values() {
            self.validate_package_cache_path(&command.module_path)
                .map_err(|denial| {
                    SandboxPolicyDenial::new(
                        denial.code,
                        format!(
                            "toolchain {} command {} module path denied: {}",
                            manifest.toolchain_id, command.name, denial.detail
                        ),
                    )
                })?;
        }
        for artifact in manifest.artifacts.values() {
            self.validate_package_cache_path(&artifact.sifs_path)
                .map_err(|denial| {
                    SandboxPolicyDenial::new(
                        denial.code,
                        format!(
                            "toolchain {} artifact {} path denied: {}",
                            manifest.toolchain_id, artifact.relative_path, denial.detail
                        ),
                    )
                })?;
        }
        Ok(())
    }

    pub fn validate_package_cache_path(&self, path: &str) -> Result<(), SandboxPolicyDenial> {
        let path = normalize_sandbox_sifs_root(path);
        if self
            .allowed_package_cache_prefixes
            .iter()
            .any(|prefix| sifs_path_has_prefix(&path, prefix))
        {
            return Ok(());
        }
        Err(SandboxPolicyDenial::new(
            "package-cache-path-denied",
            format!("package cache path {path} is not allowed by policy"),
        ))
    }

    pub fn validate_resource_limits(
        &self,
        limits: &SandboxResourceLimits,
    ) -> Result<(), SandboxPolicyDenial> {
        for (name, actual, limit) in [
            ("memory-mb", limits.memory_mb, self.max_limits.memory_mb),
            (
                "wall-time-ms",
                limits.wall_time_ms,
                self.max_limits.wall_time_ms,
            ),
            (
                "max-input-bytes",
                limits.max_input_bytes,
                self.max_limits.max_input_bytes,
            ),
            (
                "max-output-bytes",
                limits.max_output_bytes,
                self.max_limits.max_output_bytes,
            ),
            (
                "max-workspace-bytes",
                limits.max_workspace_bytes,
                self.max_limits.max_workspace_bytes,
            ),
            (
                "max-written-bytes-per-command",
                limits.max_written_bytes_per_command,
                self.max_limits.max_written_bytes_per_command,
            ),
            (
                "max-file-count",
                limits.max_file_count,
                self.max_limits.max_file_count,
            ),
        ] {
            if actual > limit {
                return Err(SandboxPolicyDenial::new(
                    "resource-limit-denied",
                    format!("{name}={actual} exceeds policy limit {limit}"),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_active_execution_counts(
        &self,
        sandbox_id: &SandboxId,
        active_commands_for_sandbox_after_admit: u64,
        active_sandboxes_for_node_after_admit: u64,
    ) -> Result<(), SandboxPolicyDenial> {
        if active_commands_for_sandbox_after_admit > self.max_concurrent_commands_per_sandbox {
            return Err(SandboxPolicyDenial::new(
                "concurrent-commands-denied",
                format!(
                    "sandbox {sandbox_id} would have {active_commands_for_sandbox_after_admit} active commands; policy limit is {}",
                    self.max_concurrent_commands_per_sandbox
                ),
            ));
        }
        if active_sandboxes_for_node_after_admit > self.max_concurrent_sandboxes_per_node {
            return Err(SandboxPolicyDenial::new(
                "concurrent-sandboxes-denied",
                format!(
                    "node would have {active_sandboxes_for_node_after_admit} active sandboxes; policy limit is {}",
                    self.max_concurrent_sandboxes_per_node
                ),
            ));
        }
        Ok(())
    }

    pub fn validate_http_request(
        &self,
        request: &SandboxHttpRequest,
    ) -> Result<SandboxHttpUrlParts, SandboxPolicyDenial> {
        if !self
            .allowed_command_capabilities
            .contains(SANDBOX_NETWORK_HTTP_CAPABILITY)
        {
            return Err(SandboxPolicyDenial::new(
                "network-capability-denied",
                format!("{SANDBOX_NETWORK_HTTP_CAPABILITY} is not allowed by sandbox policy"),
            ));
        }
        let parts = SandboxHttpUrlParts::parse(&request.url)?;
        if !matches!(parts.scheme.as_str(), "http" | "https") {
            return Err(SandboxPolicyDenial::new(
                "network-scheme-denied",
                format!("HTTP request scheme {} is not allowed", parts.scheme),
            ));
        }
        if !self.allowed_http_hosts.contains(&parts.host) {
            return Err(SandboxPolicyDenial::new(
                "network-host-denied",
                format!("HTTP host {} is not allowlisted", parts.host),
            ));
        }
        if request.body.len() as u64 > self.max_http_request_bytes {
            return Err(SandboxPolicyDenial::new(
                "network-request-bytes-denied",
                format!(
                    "HTTP request body is {} bytes; policy limit is {}",
                    request.body.len(),
                    self.max_http_request_bytes
                ),
            ));
        }
        if request.max_response_bytes > self.max_http_response_bytes {
            return Err(SandboxPolicyDenial::new(
                "network-response-budget-denied",
                format!(
                    "HTTP response budget {} exceeds policy limit {}",
                    request.max_response_bytes, self.max_http_response_bytes
                ),
            ));
        }
        if request.timeout_ms > self.max_http_timeout_ms {
            return Err(SandboxPolicyDenial::new(
                "network-timeout-denied",
                format!(
                    "HTTP timeout {}ms exceeds policy limit {}ms",
                    request.timeout_ms, self.max_http_timeout_ms
                ),
            ));
        }
        Ok(parts)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxClusterPolicyConfig {
    pub policy: SandboxPolicy,
}

impl Default for SandboxClusterPolicyConfig {
    fn default() -> Self {
        Self {
            policy: SandboxPolicy::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum SandboxPolicyConfigError {
    #[error("sandbox policy config store error at {path}: {detail}")]
    Store { path: String, detail: String },
    #[error("invalid sandbox policy config at {path}: {detail}")]
    Invalid { path: String, detail: String },
}

pub fn sandbox_policy_config_definition()
-> Result<sinomo_config::ConfigDefinition, SandboxPolicyConfigError> {
    let definition = sinomo_config::ConfigDefinition::new(
        SANDBOX_POLICY_CONFIG_KEY,
        SANDBOX_STORAGE_MODULE,
        sinomo_config::ConfigValueType::Object {
            schema_name: "SandboxClusterPolicyConfig".to_string(),
        },
        "Sandbox execution policy, including mount, toolchain, network, and resource limits.",
        sinomo_config::ConfigClusterMode::ClusterOverrideLocal,
    )
    .map_err(map_sandbox_config_error)?
    .with_default(&SandboxClusterPolicyConfig::default())
    .map_err(map_sandbox_config_error)?
    .with_local_legacy_path("config/sandbox-policy.json");
    debug_assert_eq!(definition.cluster_key(), SANDBOX_POLICY_CONFIG_KV_KEY);
    Ok(definition)
}

pub fn register_config_definitions(
    builder: sinomo_config::ConfigRegistryBuilder,
) -> Result<sinomo_config::ConfigRegistryBuilder, SandboxPolicyConfigError> {
    builder
        .register(sandbox_policy_config_definition()?)
        .map_err(map_sandbox_config_error)
}

pub fn sandbox_policy_config_from_cluster_kv_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Option<SandboxClusterPolicyConfig>, SandboxPolicyConfigError> {
    sandbox_policy_config_from_cluster_kv_entries(&snapshot.entries)
}

pub fn sandbox_policy_config_from_cluster_kv_entries(
    entries: &[KvEntry],
) -> Result<Option<SandboxClusterPolicyConfig>, SandboxPolicyConfigError> {
    let definition = sandbox_policy_config_definition()?;
    let cluster_key = definition.cluster_key();
    entries
        .iter()
        .find(|entry| entry.key == cluster_key)
        .map(|entry| decode_sandbox_policy_config_entry(entry, &definition))
        .transpose()
}

fn decode_sandbox_policy_config_entry(
    entry: &KvEntry,
    definition: &sinomo_config::ConfigDefinition,
) -> Result<SandboxClusterPolicyConfig, SandboxPolicyConfigError> {
    let config = match sinomo_config::decode_cluster_config_entry(entry, definition) {
        Ok(value) => {
            serde_json::from_value(value).map_err(|source| SandboxPolicyConfigError::Store {
                path: entry.key.clone(),
                detail: source.to_string(),
            })?
        }
        Err(config_error) => {
            decode_legacy_sandbox_policy_config_entry(entry).map_err(|source| {
                SandboxPolicyConfigError::Store {
                    path: entry.key.clone(),
                    detail: format!("not a sinomo-config document ({config_error}); {source}"),
                }
            })?
        }
    };
    validate_sandbox_policy_config(&config)?;
    Ok(config)
}

fn map_sandbox_config_error(source: sinomo_config::ConfigError) -> SandboxPolicyConfigError {
    SandboxPolicyConfigError::Store {
        path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
        detail: source.to_string(),
    }
}

pub fn sandbox_policy_config_cluster_kv_txn(
    config: &SandboxClusterPolicyConfig,
) -> Result<KvTxn, SandboxPolicyConfigError> {
    validate_sandbox_policy_config(config)?;
    let definition = sandbox_policy_config_definition()?;
    let value = serde_json::to_value(config).map_err(|source| SandboxPolicyConfigError::Store {
        path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
        detail: source.to_string(),
    })?;
    sinomo_config::cluster_config_put_txn(&definition, value).map_err(map_sandbox_config_error)
}

fn decode_legacy_sandbox_policy_config_entry(
    entry: &KvEntry,
) -> Result<SandboxClusterPolicyConfig, SandboxPolicyConfigError> {
    let config: SandboxClusterPolicyConfig =
        decode_kv_value(&entry.value).map_err(|source| SandboxPolicyConfigError::Store {
            path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
            detail: source.to_string(),
        })?;
    validate_sandbox_policy_config(&config)?;
    Ok(config)
}

pub fn validate_sandbox_policy_config(
    config: &SandboxClusterPolicyConfig,
) -> Result<(), SandboxPolicyConfigError> {
    for prefix in &config.policy.allowed_mount_prefixes {
        validate_sandbox_policy_sifs_prefix(prefix, "allowed mount prefix")?;
    }
    for prefix in &config.policy.allowed_package_cache_prefixes {
        validate_sandbox_policy_sifs_prefix(prefix, "allowed package cache prefix")?;
    }
    for prefix in &config.policy.allowed_toolchain_provenance_prefixes {
        validate_sandbox_policy_non_empty_nul_free(prefix, "allowed toolchain provenance prefix")?;
    }
    for host in &config.policy.allowed_http_hosts {
        validate_sandbox_policy_http_host(host)?;
    }
    for capability in &config.policy.allowed_command_capabilities {
        if capability.trim().is_empty() {
            return Err(SandboxPolicyConfigError::Invalid {
                path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
                detail: "allowed command capability must be non-empty".to_string(),
            });
        }
        if capability.contains('\0') {
            return Err(SandboxPolicyConfigError::Invalid {
                path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
                detail: format!(
                    "allowed command capability `{capability}` contains an embedded NUL byte"
                ),
            });
        }
    }
    Ok(())
}

fn validate_sandbox_policy_sifs_prefix(
    value: &str,
    field: &str,
) -> Result<(), SandboxPolicyConfigError> {
    validate_sandbox_policy_non_empty_nul_free(value, field)?;
    let trimmed = value.trim();
    if !trimmed.starts_with('/')
        || trimmed.contains("//")
        || trimmed.contains("/../")
        || trimmed.ends_with("/..")
    {
        return Err(SandboxPolicyConfigError::Invalid {
            path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
            detail: format!("{field} `{value}` is not a valid SIFS path prefix"),
        });
    }
    Ok(())
}

fn validate_sandbox_policy_http_host(host: &str) -> Result<(), SandboxPolicyConfigError> {
    validate_sandbox_policy_non_empty_nul_free(host, "allowed HTTP host")?;
    if host.contains('/')
        || host.contains(':')
        || host.contains('@')
        || host.chars().any(char::is_whitespace)
    {
        return Err(SandboxPolicyConfigError::Invalid {
            path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
            detail: format!("allowed HTTP host `{host}` is invalid"),
        });
    }
    Ok(())
}

fn validate_sandbox_policy_non_empty_nul_free(
    value: &str,
    field: &str,
) -> Result<(), SandboxPolicyConfigError> {
    if value.trim().is_empty() {
        return Err(SandboxPolicyConfigError::Invalid {
            path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
            detail: format!("{field} must be non-empty"),
        });
    }
    if value.contains('\0') {
        return Err(SandboxPolicyConfigError::Invalid {
            path: SANDBOX_POLICY_CONFIG_KV_KEY.to_string(),
            detail: format!("{field} `{value}` contains an embedded NUL byte"),
        });
    }
    Ok(())
}

pub fn effective_sandbox_command_limits(
    sandbox_limits: &SandboxResourceLimits,
    command_limits: &SandboxResourceLimits,
) -> SandboxResourceLimits {
    SandboxResourceLimits {
        memory_mb: sandbox_limits.memory_mb.min(command_limits.memory_mb),
        wall_time_ms: sandbox_limits.wall_time_ms.min(command_limits.wall_time_ms),
        max_output_bytes: sandbox_limits
            .max_output_bytes
            .min(command_limits.max_output_bytes),
        max_input_bytes: sandbox_limits
            .max_input_bytes
            .min(command_limits.max_input_bytes),
        max_workspace_bytes: sandbox_limits
            .max_workspace_bytes
            .min(command_limits.max_workspace_bytes),
        max_written_bytes_per_command: sandbox_limits
            .max_written_bytes_per_command
            .min(command_limits.max_written_bytes_per_command),
        max_file_count: sandbox_limits
            .max_file_count
            .min(command_limits.max_file_count),
    }
}

pub fn effective_sandbox_timeout_ms(requested_timeout_ms: u64, limit_timeout_ms: u64) -> u64 {
    if requested_timeout_ms == 0 {
        limit_timeout_ms
    } else if limit_timeout_ms == SANDBOX_UNLIMITED_WALL_TIME_MS {
        requested_timeout_ms
    } else {
        requested_timeout_ms.min(limit_timeout_ms)
    }
}

fn sifs_path_has_prefix(path: &str, prefix: &str) -> bool {
    let path = normalize_sandbox_sifs_root(path);
    let prefix = normalize_sandbox_sifs_root(prefix);
    path == prefix || path.starts_with(&format!("{}/", prefix.trim_end_matches('/')))
}

fn local_owned_mount_node_id(source: &str) -> Option<&str> {
    let mut parts = source.trim_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("sinomo"), Some("nodes"), Some(node_id), Some("files")) => Some(node_id),
        _ => None,
    }
}

fn local_owned_mount_nodes(mounts: &[SandboxMount]) -> Result<Vec<NodeId>, SandboxSchedulingError> {
    let mut nodes = Vec::new();
    for mount in mounts {
        let source = normalize_sandbox_sifs_root(&mount.source_sifs_path);
        if let Some(node_id) = local_owned_mount_node_id(&source) {
            nodes.push(NodeId::new(node_id.to_string()).map_err(|source| {
                SandboxSchedulingError::new(
                    "policy-blocked",
                    format!("invalid local-owned mount owner node id: {source}"),
                )
            })?);
        }
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{KvOp, KvRevision, KvVersion};

    fn kv_entry_from_put(op: KvOp) -> KvEntry {
        let KvOp::Put { key, value, .. } = op else {
            panic!("expected put op")
        };
        KvEntry {
            key,
            value,
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        }
    }

    #[test]
    fn sandbox_policy_config_txn_writes_config_document() {
        let mut config = SandboxClusterPolicyConfig::default();
        config.policy.allow_unsigned_toolchains = true;

        let txn = sandbox_policy_config_cluster_kv_txn(&config).expect("policy config txn");
        assert_eq!(txn.success.len(), 1);
        let entry = kv_entry_from_put(txn.success.into_iter().next().expect("put op"));
        assert_eq!(entry.key, SANDBOX_POLICY_CONFIG_KV_KEY);

        let document: sinomo_config::ConfigDocument = entry.decode().expect("config document");
        assert_eq!(document.key.as_str(), SANDBOX_POLICY_CONFIG_KEY);
        let decoded: SandboxClusterPolicyConfig =
            serde_json::from_value(document.value).expect("policy config value");
        assert_eq!(decoded, config);

        let from_entries = sandbox_policy_config_from_cluster_kv_entries(&[entry])
            .expect("decode entries")
            .expect("policy config");
        assert_eq!(from_entries, config);
    }

    #[test]
    fn sandbox_policy_config_reads_legacy_raw_kv_value() {
        let mut config = SandboxClusterPolicyConfig::default();
        config.policy.allow_remote_local_owned_mounts = true;
        let op =
            KvOp::put_serde(SANDBOX_POLICY_CONFIG_KV_KEY, &config, None).expect("legacy policy op");
        let entry = kv_entry_from_put(op);

        let decoded = sandbox_policy_config_from_cluster_kv_entries(&[entry])
            .expect("decode entries")
            .expect("policy config");
        assert_eq!(decoded, config);
    }
}
