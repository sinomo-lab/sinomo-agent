//! Fixed-command runtime verification of the live SIFS-backed agent HOME.

use sinomo_agent::{
    AgentToolCall, AgentToolRegistry, AgentToolStatus, InteractionSession, InteractionSurface,
    InteractionSurfaceKind, LocalWorkspaceAgentConfig, WorkspaceSifsVfs,
};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::node::LocalNodePermission;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};

pub fn register(builder: SinomoTestRegistryBuilder) -> SinomoTestRegistryBuilder {
    builder
        .register(agent_home_command())
        .register(agent_home_prepare())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_home_command(ctx) {
        id: "cluster.sifs.agent-home-command",
        summary: "Run a fixed command with the real live SIFS agent HOME and sandbox mount",
        tags: ["cluster-sifs-consumers", "agent-home", "kernel-filesystem"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_agent_home_command(ctx) }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn agent_home_prepare(ctx) {
        id: "cluster.sifs.agent-home-prepare",
        summary: "Call the normal strict HOME metadata initializer without a mount or sandbox",
        tags: ["cluster-sifs-consumers", "agent-home", "metadata-only"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: { run_agent_home_prepare(ctx) }
    }
}

#[cfg(unix)]
fn run_agent_home_prepare(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let agent = ctx.required_parameter("agent")?.to_string();
    if !isolated_agent(&agent) {
        return Err(invalid(
            "agent",
            "must be an isolated lowercase cluster-test-* identity",
        ));
    }
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let expected = sinomo_agent::agent_home_sifs_path_for_id(&agent);
    // These accessors only observe this caller's effective native credentials.
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    ctx.record("agent", &agent);
    ctx.record("home", &expected);
    ctx.record("expected_uid", uid);
    ctx.record("expected_gid", gid);
    ctx.record("expected_mode", "0700");
    ctx.record("initializer", "prepare_simple_sandbox_agent_home");
    ctx.record("fuse_mount_requested", false);
    ctx.record("sandbox_provisioned", false);
    ctx.record("fixture_retained", true);
    // Use the existing product initializer exactly once. Its ordinary native
    // owner, create-with-attributes and existing-owner/mode checks remain intact.
    // Any failure or uncertain effect is retained without a second attempt.
    let actual = crate::prepare_simple_sandbox_agent_home(&data_dir, &agent)
        .map_err(SinomoTestError::failed)?;
    if actual != expected {
        return Ok(SinomoTestOutcome::failed(
            "strict initializer returned another home path",
        ));
    }
    Ok(SinomoTestOutcome::passed(
        "normal strict HOME initializer accepted caller ownership and mode0700; no FUSE mount or sandbox command requested",
    ))
}

#[cfg(not(unix))]
fn run_agent_home_prepare(_ctx: &mut SinomoTestContext) -> SinomoTestResult {
    Ok(SinomoTestOutcome::failed(
        "strict POSIX agent-home initialization is unavailable on this platform",
    ))
}

fn invalid(name: &str, detail: &str) -> SinomoTestError {
    SinomoTestError::InvalidParameter {
        name: name.into(),
        detail: detail.into(),
    }
}

fn isolated_agent(value: &str) -> bool {
    value.starts_with("cluster-test-")
        && value.len() > 13
        && value.len() <= 120
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[cfg(all(unix, feature = "dfs-fuse"))]
fn run_agent_home_command(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let agent = ctx.required_parameter("agent")?.to_string();
    if !isolated_agent(&agent) {
        return Err(invalid(
            "agent",
            "must be an isolated lowercase cluster-test-* identity",
        ));
    }
    let payload = ctx.required_parameter("payload")?.to_string();
    if payload.is_empty()
        || payload.len() > 1024
        || !payload
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(invalid(
            "payload",
            "must contain 1..1024 ASCII letters, digits or hyphens",
        ));
    }
    let name = ctx.required_parameter("name")?.to_string();
    if name.is_empty()
        || name.len() > 80
        || name.starts_with('.')
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
    {
        return Err(invalid("name", "must be a safe isolated filename"));
    }
    let action = ctx.required_parameter("action")?.to_string();
    if !matches!(action.as_str(), "create" | "read") {
        return Err(invalid("action", "must be create or read"));
    }
    let fail = |error: String| SinomoTestError::failed(error);
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let state = sinomo_runtime::cluster::read_cluster_state_without_kv(
        &sinomo_runtime::network::ClusterPaths::new(&data_dir),
    )
    .map_err(|error| fail(error.to_string()))?;
    let mount = crate::ensure_simple_sandbox_sifs_mount(&data_dir)
        .map_err(|error| fail(error.to_string()))?
        .ok_or_else(|| fail("the product live SIFS mount is unavailable".into()))?;
    let home = crate::prepare_simple_sandbox_agent_home(&data_dir, &agent).map_err(fail)?;
    let workspace = format!("{home}/workspace");
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    crate::sifs_file_manager::SifsFileManagerBackend::open(&data_dir)
        .map_err(fail)?
        .ensure_posix_directory_with_attrs(&workspace, uid, gid, 0o700)
        .map_err(fail)?;
    let workspace_host = mount.mount_root().join(workspace.trim_start_matches('/'));
    let session_id = SessionId::generate();
    let config = LocalWorkspaceAgentConfig::new(&workspace_host)
        .with_data_dir_evidence(&data_dir, &session_id)
        .with_sandbox_sifs_mount_root(mount.mount_root())
        .with_workspace_sifs_vfs(WorkspaceSifsVfs::mounted(&data_dir))
        .with_agent_home_sifs_path(&home)
        .allowing_shell_commands(true)
        .allowing_file_writes(true);
    use sinomo_runtime::sandbox::{SandboxMount, SandboxMountMode, SandboxPath};
    let mounts = vec![
        SandboxMount {
            source_sifs_path: workspace.clone(),
            mount_path: SandboxPath::new("/workspace").map_err(|error| fail(error.to_string()))?,
            mode: SandboxMountMode::ReadWrite,
        },
        SandboxMount {
            source_sifs_path: home.clone(),
            mount_path: SandboxPath::new("/home/agent").map_err(|error| fail(error.to_string()))?,
            mode: SandboxMountMode::ReadWrite,
        },
    ];
    let sandbox = sinomo_agent::local_workspace::provision_default_agent_session_sandbox(
        &crate::sandbox_lifecycle_client(&data_dir),
        &config,
        mounts,
    )
    .map_err(fail)?;
    // This runtime fixture authorizes only its fixed, validated command, as the
    // existing active-cancellation fixture does. Cluster filesystem effects
    // retain ordinary daemon, namespace and native owner authorization.
    let registry = AgentToolRegistry::local_workspace_with_config(config)
        .with_local_permission_checker(|permission| {
            Ok(permission == LocalNodePermission::RemoteManagement)
        });
    mount.retain_for_registry(&registry);
    let session = InteractionSession::new(
        session_id,
        state.local_node_id,
        InteractionSurface::new(
            "cluster.sifs.agent-home",
            InteractionSurfaceKind::CommandLine,
        ),
    );
    let script = if action == "create" {
        format!(
            "set -eu\nset -C\nprintf '%s' '{payload}' > \"$HOME/{name}\"\nprintf '%s\\n' \"$HOME\"\ncat \"$HOME/{name}\""
        )
    } else {
        format!("set -eu\nprintf '%s\\n' \"$HOME\"\ncat \"$HOME/{name}\"")
    };
    let result = registry.execute(
        &session,
        0,
        0,
        &AgentToolCall {
            call_id: None,
            tool: "run_command".into(),
            input: serde_json::json!({"command":script,"timeout_ms":30_000}),
            r#async: false,
            summary: "verify isolated live SIFS agent home".into(),
        },
    );
    ctx.record("agent", &agent);
    ctx.record("home", &home);
    ctx.record("workspace", &workspace);
    ctx.record("sandbox_id", sandbox.to_string());
    ctx.record("tool_status", &result.status);
    ctx.record("tool_output", &result.output);
    if result.status != AgentToolStatus::Succeeded {
        return Ok(SinomoTestOutcome::failed(
            "the fixed agent HOME command failed",
        ));
    }
    let output = result
        .output
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| fail("command omitted stdout".into()))?;
    let (observed_home, contents) = output
        .split_once('\n')
        .ok_or_else(|| fail("command omitted its actual HOME".into()))?;
    let host_home = mount.mount_root().join(home.trim_start_matches('/'));
    if !matches!(observed_home, "/home/agent") && observed_home != host_home.to_string_lossy() {
        return Ok(SinomoTestOutcome::failed(format!(
            "command substituted another HOME: {observed_home}"
        )));
    }
    if contents != payload {
        return Ok(SinomoTestOutcome::failed(
            "command read different HOME bytes",
        ));
    }
    let client = sinomo_runtime::api::DaemonApiClient::connect_for_paths(
        &sinomo_runtime::api::DaemonPaths::new(&data_dir),
    )
    .map_err(|error| fail(error.to_string()))?;
    let actual =
        sinomo_runtime::vfs::discovered_daemon_sifs(data_dir.clone(), client.address().to_string())
            .map_err(|error| fail(error.to_string()))?
            .read(format!("{home}/{name}"))
            .map_err(|error| fail(error.to_string()))?;
    if actual != payload.as_bytes() {
        return Ok(SinomoTestOutcome::failed(
            "command HOME write is absent from canonical SIFS",
        ));
    }
    ctx.record("actual_home", observed_home);
    ctx.record("path", format!("{home}/{name}"));
    Ok(SinomoTestOutcome::passed(
        "real agent HOME command and canonical SIFS bytes agree",
    ))
}

#[cfg(not(all(unix, feature = "dfs-fuse")))]
fn run_agent_home_command(_ctx: &mut SinomoTestContext) -> SinomoTestResult {
    Ok(SinomoTestOutcome::failed(
        "this runtime lacks the product Unix FUSE agent-home integration",
    ))
}
