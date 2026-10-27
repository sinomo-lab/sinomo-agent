//! Separate native daemon processes and real authenticated TCP/routed streams.
//! Driver replies are observation DATA, never private prefix/serving authority.
use super::*;
use sinomo_core::network::{ClusterPaths, ObjectRpcRequest};

mod host;
const CHILD: &str =
    "runtime_host::tests::incomplete_genesis::owned_process::per_node::native_daemon_process_child";
const INPUT_ENV: &str = "SINOMO_DFS_PER_NODE_NATIVE_INPUT";
const DRIVER_ROUTE: &str = "/_dfs-test/native-peer";
const DRIVER_CATEGORY: &str = "dfs.test.native.peer";
const DRIVER_OPERATION: &str = "dfs.test.native.peer.query";
const OP_CAP: Duration = sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
const MAX_REPLY: usize = 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeInput {
    owned_root: PathBuf,
    data_root: PathBuf,
    node_id: NodeId,
    invocation: String,
    retained: Option<RetainedNodeStart>,
}

/// Post-reap observation DATA only. This cannot grant runtime authority or
/// reconstruct a signer, private transport return or current storage owner.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedNodeStart {
    old_pid: u32,
    old_start: u64,
    data_identity: [u64; 2],
    data_owner_identity: [u64; 2],
    identity_identity: [u64; 2],
    identity_blake3: String,
    public_key: String,
    cluster_id: sinomo_core::node::ClusterId,
    address: String,
    wal_root_identity: [u64; 2],
    wal_lock_identity: [u64; 2],
    wal_identity: [u64; 2],
    wal_blake3: String,
    hard_state_identity: [u64; 2],
    hard_state_blake3: String,
    wal_snapshot_identity: Option<[u64; 2]>,
    wal_snapshot_blake3: Option<String>,
    raft_term: u64,
    raft_commit_index: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeHello {
    pid: u32,
    invocation: String,
    node_id: NodeId,
    address: String,
    public_key: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeConfiguration {
    state: sinomo_core::ClusterBootstrapState,
    bootstrap_initial_voter: bool,
    #[serde(default)]
    dfs_learners: bool,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum ProbeKind {
    Ping,
    DfsStatus,
    EndpointPing,
    EndpointDfsStatus,
    EndpointWrongPeer,
    RegisteredEndpointPing,
    FormationEndpointPing,
    RegisteredEndpointWrongPeer,
    RegisteredEndpointWrongId,
    PrefixRound,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ProbePath {
    NodeRouting,
    AuthenticatedEndpoint,
    RegisteredNativeEndpoint,
    NativePrefixOwner,
}
impl ProbeKind {
    fn path(self) -> ProbePath {
        match self {
            Self::Ping | Self::DfsStatus => ProbePath::NodeRouting,
            Self::RegisteredEndpointPing
            | Self::FormationEndpointPing
            | Self::RegisteredEndpointWrongPeer
            | Self::RegisteredEndpointWrongId => ProbePath::RegisteredNativeEndpoint,
            Self::PrefixRound => ProbePath::NativePrefixOwner,
            _ => ProbePath::AuthenticatedEndpoint,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverRequest {
    target: NodeId,
    nonce: String,
    kind: ProbeKind,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeReply {
    responder_node_id: Option<String>,
    route: String,
    body: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeFailure {
    error: String,
    delivery_state: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverResponse {
    source_node_id: NodeId,
    source_pid: u32,
    target: NodeId,
    nonce: String,
    path: ProbePath,
    contact: Option<sinomo_core::ClusterEndpointRecord>,
    result: Result<ProbeReply, ProbeFailure>,
}

#[test]
#[ignore = "owned native daemon process; invoked only by exact parent"]
fn native_daemon_process_child() -> TestResult<()> {
    let input_path = PathBuf::from(
        std::env::var_os(INPUT_ENV).ok_or("explicit per-node parent input required")?,
    );
    let input: NodeInput = read_data(&input_path)?;
    let suffix = if input.retained.is_some() {
        ":restart"
    } else {
        ""
    };
    let filename = if input.retained.is_some() {
        "restart-input.sobj"
    } else {
        "input.sobj"
    };
    if input_path != input.owned_root.join(filename)
        || input.data_root != input.owned_root.join("data")
        || std::env::var_os("SINOMO_DATA_DIR").map(PathBuf::from) != Some(input.data_root.clone())
        || input.invocation
            != format!(
                "{}:{}{suffix}",
                input
                    .owned_root
                    .file_name()
                    .ok_or("owned root name missing")?
                    .to_string_lossy(),
                input.node_id
            )
    {
        return Err("per-node owned input/path binding differs".into());
    }
    DirectoryId::read(&input.owned_root)?;
    let trace = crate::default_cluster::bootstrap_observation::CaptureGuard::start(
        input.data_root.clone(),
        input.node_id.clone(),
    );
    let trace_path = input.owned_root.join(if input.retained.is_some() {
        "restart-bootstrap-owner-observation.sobj"
    } else {
        "bootstrap-owner-observation.sobj"
    });
    let result = host::run_node(input);
    let saved = write_data(&trace_path, &trace.snapshot());
    result?;
    saved
}

struct NativeNode {
    owned: OwnedChild,
    input: NodeInput,
    hello: Option<NodeHello>,
    released: bool,
    contacts: std::collections::BTreeMap<NodeId, String>,
}
impl NativeNode {
    fn alive_identity(&mut self) -> TestResult<ProcessIdentity> {
        let expected = self
            .owned
            .identity
            .as_ref()
            .ok_or("native node identity absent")?
            .clone();
        let child = self
            .owned
            .child
            .as_mut()
            .ok_or("native node already reaped")?;
        if let Some(status) = child.try_wait()? {
            self.owned.child = None;
            return Err(format!("native node exited: {status}").into());
        }
        assert_eq!(ProcessIdentity::read(expected.pid)?, expected);
        Ok(expected)
    }
}
fn launch_native_node(index: usize) -> TestResult<NativeNode> {
    let fixture = sinomo_testdir::TestDir::new(&format!("dfs-real-native-node-{index}"));
    let root = fixture.path().to_path_buf();
    let node_id = NodeId::generate();
    let input = NodeInput {
        owned_root: root.clone(),
        data_root: root.join("data"),
        node_id: node_id.clone(),
        invocation: format!("{}:{node_id}", root.file_name().unwrap().to_string_lossy()),
        retained: None,
    };
    let input_path = root.join("input.sobj");
    write_data(&input_path, &input)?;
    let temp = root.join("child-tmp");
    fs::create_dir(&temp)?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .process_group(0)
        .env(INPUT_ENV, &input_path)
        .env("SINOMO_DATA_DIR", &input.data_root)
        .env("TMPDIR", &temp)
        .env("SINOMO_TEST_KEEP_TMP", "1")
        .stdin(Stdio::piped())
        .stdout(File::create(root.join("child.stdout.log"))?)
        .stderr(File::create(root.join("child.stderr.log"))?);
    Ok(NativeNode {
        owned: OwnedChild::spawn(fixture, &mut command)?,
        input,
        hello: None,
        released: false,
        contacts: std::collections::BTreeMap::new(),
    })
}
fn read_hello(node: &mut NativeNode, deadline: Instant) -> TestResult<NodeHello> {
    loop {
        node.alive_identity()?;
        if Instant::now() >= deadline {
            return Err("native listener/key Ready exceeded original60s".into());
        }
        let path = node
            .input
            .owned_root
            .join(if node.input.retained.is_some() {
                "restart-hello.sobj"
            } else {
                "hello.sobj"
            });
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let hello: NodeHello = read_data(&path)?;
                assert_eq!(hello.pid, node.owned.identity.as_ref().unwrap().pid);
                assert_eq!(hello.node_id, node.input.node_id);
                assert_eq!(hello.invocation, node.input.invocation);
                let address: std::net::SocketAddr = hello.address.parse()?;
                assert!(address.ip().is_loopback() && address.port() != 0);
                assert!(
                    !hello.public_key.is_empty()
                        && hello.public_key.len() <= 128
                        && !hello.public_key.starts_with("pk-")
                );
                DirectoryId::read(&node.input.data_root)?;
                if Instant::now() >= deadline {
                    return Err("native Hello validation exceeded original60s".into());
                }
                return Ok(hello);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        std::thread::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}
fn retained_expectation(node: &NativeNode, raw: &RawTree) -> TestResult<RetainedNodeStart> {
    use sinomo_core::cluster::ClusterKvPathsExt;
    if node.owned.child.is_some() || raw.root != node.input.data_root {
        return Err("retained expectation requires the exact reaped DATA root".into());
    }
    let old = node
        .owned
        .identity
        .as_ref()
        .ok_or("old process identity absent")?;
    ensure_group_empty(old)?;
    let hello = node.hello.as_ref().ok_or("old native Hello absent")?;
    let data = SinomoDataDir::new(&node.input.data_root);
    let paths = ClusterPaths::new(&data);
    let wal = paths.kv_paths().raft_wal_paths(&node.input.node_id);
    let file = |path: &Path| -> TestResult<&RawFile> {
        let relative = path.strip_prefix(&raw.root)?;
        raw.files
            .iter()
            .find(|f| f.relative == relative)
            .ok_or_else(|| "required retained file missing from raw capture".into())
    };
    let dir = |path: &Path| -> TestResult<[u64; 2]> {
        let relative = path.strip_prefix(&raw.root)?;
        let value = raw
            .directories
            .iter()
            .find(|d| d.relative == relative)
            .ok_or("required retained directory missing")?;
        Ok([value.physical.device, value.physical.inode])
    };
    let fid = |f: &RawFile| [f.physical.device, f.physical.inode];
    let key = file(&sinomo_core::node::NodeIdentityPaths::new(&node.input.data_root).main)?;
    let hard_file = file(&wal.hard_state)?;
    let hard: sinomo_core::kv::RaftHardState =
        sinomo_core::object::serde_ext::from_bytes(&hard_file.bytes)?;
    let snapshot = raw
        .files
        .iter()
        .find(|f| f.relative == wal.snapshot.strip_prefix(&raw.root).unwrap());
    let initial: NodeConfiguration = read_data(&node.input.owned_root.join("configuration.sobj"))?;
    // Decode only actual captured native bytes. No WAL opener, mutation, reset,
    // key creation, current authority or success inference occurs here.
    Ok(RetainedNodeStart {
        old_pid: old.pid,
        old_start: old.start,
        data_identity: dir(&node.input.data_root)?,
        data_owner_identity: fid(file(&node.input.data_root.join("daemon/data-owner.lock"))?),
        identity_identity: fid(key),
        identity_blake3: key.blake3.clone(),
        public_key: hello.public_key.clone(),
        cluster_id: initial.state.cluster_id,
        address: hello.address.clone(),
        wal_root_identity: dir(&wal.root)?,
        wal_lock_identity: fid(file(&wal.lock)?),
        wal_identity: fid(file(&wal.wal)?),
        wal_blake3: file(&wal.wal)?.blake3.clone(),
        hard_state_identity: fid(hard_file),
        hard_state_blake3: hard_file.blake3.clone(),
        wal_snapshot_identity: snapshot.map(fid),
        wal_snapshot_blake3: snapshot.map(|f| f.blake3.clone()),
        raft_term: hard.current_term,
        raft_commit_index: hard.commit_index,
    })
}

fn restart_retained_node(node: &mut NativeNode, expected: RetainedNodeStart) -> TestResult<()> {
    if node.owned.child.is_some() {
        return Err("cannot restart an unreaped native owner".into());
    }
    ensure_group_empty(
        node.owned
            .identity
            .as_ref()
            .ok_or("reaped identity absent")?,
    )?;
    let mut input = node.input.clone();
    input.invocation.push_str(":restart");
    input.retained = Some(expected);
    let root = &input.owned_root;
    let input_path = root.join("restart-input.sobj");
    if input_path.try_exists()? {
        return Err("retained invocation already exists".into());
    }
    write_data(&input_path, &input)?;
    let temp = root.join("restart-tmp");
    fs::create_dir(&temp)?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .process_group(0)
        .env(INPUT_ENV, &input_path)
        .env("SINOMO_DATA_DIR", &input.data_root)
        .env("TMPDIR", &temp)
        .env("SINOMO_TEST_KEEP_TMP", "1")
        .stdin(Stdio::null())
        .stdout(File::create(root.join("restart.stdout.log"))?)
        .stderr(File::create(root.join("restart.stderr.log"))?);
    let fixture = node
        .owned
        .fixture
        .take()
        .ok_or("reaped native fixture ownership lost")?;
    // Move the SAME fixture guard only after proven reap. On spawn failure the
    // existing OwnedChild owner retains it; no unowned child is created.
    node.owned = OwnedChild::spawn(fixture, &mut command)?;
    node.input = input;
    node.hello = None;
    node.released = false;
    Ok(())
}

fn wait_for_retained_refusal(node: &mut NativeNode, deadline: Instant) -> TestResult<ExitStatus> {
    loop {
        if let Some(status) = node
            .owned
            .child
            .as_mut()
            .ok_or("refusal child missing")?
            .try_wait()?
        {
            node.owned.child = None;
            ensure_group_empty(node.owned.identity.as_ref().unwrap())?;
            if Instant::now() > deadline {
                return Err("retained refusal exceeded original startup60s".into());
            }
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err("retained refusal did not terminate within60s".into());
        }
        std::thread::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn configure_nodes(nodes: &mut [NativeNode], dfs_learners: bool) -> TestResult<()> {
    let contacts = nodes
        .iter()
        .map(|node| {
            let hello = node.hello.as_ref().unwrap();
            (hello.node_id.clone(), hello.address.clone())
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let now = time::OffsetDateTime::now_utc();
    let cluster = sinomo_core::Id::generate();
    let a = nodes[0].hello.as_ref().unwrap();
    let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
        cluster.clone(),
        "owned native DFS process fixture",
        a.node_id.clone(),
        a.node_id.to_string(),
        a.public_key.clone(),
        now,
    );
    for node in &nodes[1..] {
        let hello = node.hello.as_ref().unwrap();
        state
            .members
            .push(sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster.clone(),
                hello.node_id.clone(),
                hello.node_id.to_string(),
                hello.public_key.clone(),
                now,
            ));
    }
    if dfs_learners {
        // Fresh fixture input only, before any daemon receives configuration.
        // Native Learner semantics permit DFS service eligibility but not a
        // control vote/admin capability. No retained membership is rewritten.
        for member in &mut state.members {
            if member.node_id != a.node_id {
                sinomo_core::node::apply_cluster_raft_role(
                    member,
                    sinomo_core::ClusterMemberRole::Learner,
                    now,
                );
            }
        }
    }
    for member in &mut state.members {
        let hello = nodes
            .iter()
            .find_map(|node| node.hello.as_ref().filter(|h| h.node_id == member.node_id))
            .unwrap();
        member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
        member.allowed_capabilities = member.capabilities.clone();
        member.endpoints = vec![sinomo_core::network::daemon_lan_endpoint(
            &member.node_id,
            hello.address.clone(),
            0,
        )];
    }
    for (index, node) in nodes.iter_mut().enumerate() {
        node.contacts = contacts.clone();
        let mut local = state.clone();
        local.local_node_id = node.input.node_id.clone();
        local.local_node_display_name = node.input.node_id.to_string();
        write_data(
            &node.input.owned_root.join("configuration.sobj"),
            &NodeConfiguration {
                state: local,
                bootstrap_initial_voter: index == 0,
                dfs_learners,
            },
        )?;
    }
    for node in nodes {
        let child = node
            .owned
            .child
            .as_mut()
            .ok_or("native child missing before release")?;
        let stdin = child
            .stdin
            .as_mut()
            .ok_or("owned native stdin unavailable")?;
        stdin.write_all(&[1])?;
        stdin.flush()?;
        node.released = true;
    }
    Ok(())
}
fn local_rpc(
    node: &NativeNode,
    request: ObjectRpcRequest,
    budget: Duration,
) -> TestResult<sinomo_core::network::ObjectRpcResponse> {
    use sinomo_core::network::{
        ClusterObjectRpcClient, ClusterObjectRpcOptions, DaemonRpcTimeouts, ObjectRpcCall,
    };
    let hello = node
        .hello
        .as_ref()
        .ok_or("native listener hello unavailable")?;
    let data = SinomoDataDir::new(&node.input.data_root);
    let paths = sinomo_daemon::DaemonPaths::new(&data);
    let runtime = crate::test_runtime::executor();
    let mut host =
        sinomo_daemon::DaemonObjectRpcAsyncHost::new(runtime.handle(), false).with_paths(&paths);
    let key = sinomo_core::network::cached_daemon_signing_public_key(&data)?;
    let endpoint = NodeId::derived("sinomo.network.local-object-rpc-call", &key);
    let pool_scope = format!("local-object-rpc:{}:{endpoint}", hello.address);
    let deadline = Instant::now() + budget;
    let result = runtime.block_on(async {
        let response = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            ClusterObjectRpcClient::endpoint_only(&mut host)
                .call_loopback(
                    &data,
                    &hello.address,
                    ObjectRpcCall::value(request),
                    ClusterObjectRpcOptions::new(DaemonRpcTimeouts::connect_and_idle(budget))
                        .with_overall_timeout(budget),
                )
                .await?
                .into_materialized_value_response(MAX_REPLY)
                .await
        })
        .await;
        let retired = tokio::time::timeout(
            OP_CAP,
            sinomo_core::network::retire_pooled_routed_streams(&pool_scope),
        )
        .await;
        (response, retired)
    });
    result
        .1
        .map_err(|_| "parent native client carrier did not drain within5s")?;
    Ok(result
        .0
        .map_err(|_| "parent native RPC reached its original deadline")??)
}
fn probe(
    node: &mut NativeNode,
    target: &NodeId,
    kind: ProbeKind,
    nonce: String,
    budget: Duration,
) -> TestResult<DriverResponse> {
    let identity = node.alive_identity()?;
    let request = DriverRequest {
        target: target.clone(),
        nonce: nonce.clone(),
        kind,
    };
    let response = local_rpc(
        node,
        ObjectRpcRequest::encode_body(DRIVER_ROUTE, DRIVER_CATEGORY, &request)?,
        budget,
    )?;
    response.error_detail()?;
    let result: DriverResponse = response.decode_body()?;
    assert_eq!(result.source_node_id, node.input.node_id);
    assert_eq!(result.source_pid, identity.pid);
    assert_eq!(result.target, *target);
    assert_eq!(result.nonce, nonce);
    assert_eq!(result.path, kind.path());
    if let Some(contact) = &result.contact {
        assert_eq!(&contact.node_id, target);
        assert!(matches!(
            contact.transport,
            sinomo_core::ClusterTransportKind::Loopback | sinomo_core::ClusterTransportKind::LanTcp
        ));
        let selected = if matches!(
            kind,
            ProbeKind::EndpointWrongPeer | ProbeKind::RegisteredEndpointWrongPeer
        ) {
            node.contacts
                .keys()
                .find(|id| *id != &node.input.node_id && *id != target)
                .ok_or("wrong-contact control has no other owned peer")?
        } else {
            target
        };
        assert_eq!(
            Some(&contact.address),
            node.contacts.get(selected),
            "diagnostic contact may name only its actual owned listener"
        );
    }
    if matches!(
        kind.path(),
        ProbePath::AuthenticatedEndpoint | ProbePath::RegisteredNativeEndpoint
    ) && result.result.is_ok()
    {
        assert!(
            result.contact.is_some(),
            "successful endpoint result lacks its selected owned contact"
        );
    }
    node.alive_identity()?;
    if let Ok(reply) = &result.result {
        assert!(reply.body.len() <= MAX_REPLY);
        match kind {
            ProbeKind::Ping
            | ProbeKind::EndpointPing
            | ProbeKind::EndpointWrongPeer
            | ProbeKind::RegisteredEndpointPing
            | ProbeKind::FormationEndpointPing
            | ProbeKind::RegisteredEndpointWrongPeer
            | ProbeKind::RegisteredEndpointWrongId => {
                assert_eq!(
                    reply.route,
                    sinomo_core::network::CLUSTER_NETWORK_PING_ROUTE
                );
                let ping: sinomo_core::cluster::ClusterNetworkPingResponse =
                    sinomo_core::object::serde_ext::from_bytes(&reply.body)?;
                assert_eq!(ping.nonce, nonce);
                assert_eq!(&ping.responder_node_id, target);
                assert_eq!(reply.responder_node_id.as_deref(), Some(target.as_str()));
            }
            ProbeKind::PrefixRound => {
                assert_eq!(reply.route, DRIVER_ROUTE);
                assert_eq!(target, &node.input.node_id);
                let data: serde_json::Value =
                    sinomo_core::object::serde_ext::from_bytes(&reply.body)?;
                assert_eq!(data["classification"], "historical-data-only");
                assert_eq!(data["family"], "native-prefix-full-assignment");
                // This is DATA describing checks inside the actual owner. No
                // private round/capability can be reconstructed in the parent.
            }
            ProbeKind::DfsStatus | ProbeKind::EndpointDfsStatus => {
                assert_eq!(reply.route, crate::DFS_STATUS_ROUTE);
                let _: crate::DfsOperatorStatus =
                    sinomo_core::object::serde_ext::from_bytes(&reply.body)?;
                if let Some(actual) = reply.responder_node_id.as_deref() {
                    assert_eq!(actual, target.as_str());
                }
            }
        }
    }
    Ok(result)
}
fn record_probe(
    root: &Path,
    report: &mut serde_json::Value,
    phase: &str,
    result: &TestResult<DriverResponse>,
) -> TestResult<()> {
    let index = report["probes"].as_array().unwrap().len();
    if index >= 128 {
        return Err("bounded native probe evidence exceeded128 records".into());
    }
    let summary = match result {
        Ok(value) => {
            let file = root.join(format!("probe-{index:03}.sobj"));
            write_data(&file, value)?;
            let bytes = fs::read(&file)?;
            serde_json::json!({"phase":phase,"path":value.path,"selected_contact":value.contact,"source":value.source_node_id,"source_pid":value.source_pid,"target":value.target,"nonce":value.nonce,
                "actual_result":match &value.result {Ok(reply)=>serde_json::json!({"route":reply.route,"responder":reply.responder_node_id,"body_bytes":reply.body.len()}),Err(error)=>serde_json::json!({"error":error})},
                "evidence":file,"blake3":blake3::hash(&bytes).to_hex().to_string()})
        }
        Err(error) => {
            serde_json::json!({"phase":phase,"caller_error":error.to_string(),"delivery":"unavailable/unknown; no synthetic DefinitelyNotSent"})
        }
    };
    report["probes"].as_array_mut().unwrap().push(summary);
    Ok(())
}
fn wait_for_native_ping(
    nodes: &mut [NativeNode],
    source: usize,
    target: usize,
    kind: ProbeKind,
    deadline: Instant,
    root: &Path,
    report: &mut serde_json::Value,
) -> TestResult<()> {
    loop {
        if Instant::now() >= deadline {
            return Err(
                "native baseline did not complete within original60s startup envelope".into(),
            );
        }
        let target_id = nodes[target].input.node_id.clone();
        let nonce = format!("baseline-{source}-{target}-{}", sinomo_core::Id::generate());
        let result = probe(
            &mut nodes[source],
            &target_id,
            kind,
            nonce,
            OP_CAP.min(deadline.saturating_duration_since(Instant::now())),
        );
        record_probe(root, report, "baseline-native-ping", &result)?;
        if matches!(&result,Ok(value) if value.result.is_ok()) {
            if Instant::now() >= deadline {
                return Err("native baseline completed after original startup deadline".into());
            }
            return Ok(());
        }
        nodes[source].alive_identity()?;
        nodes[target].alive_identity()?;
        std::thread::sleep(
            Duration::from_secs(1).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}
fn ensure_group_empty(identity: &ProcessIdentity) -> TestResult<()> {
    let mut scanned = 0;
    for item in fs::read_dir("/proc")? {
        scanned += 1;
        if scanned > 65_536 {
            return Err("owned group scan exceeds bound".into());
        }
        let item = item?;
        let Some(pid) = item
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let state = match ProcessIdentity::read(pid) {
            Ok(p) => p,
            Err(_) if !item.path().exists() => continue,
            Err(e) => return Err(e),
        };
        if state.group == identity.group {
            return Err(format!("owned native group still has process {}", state.pid).into());
        }
    }
    Ok(())
}
fn normal_drain(nodes: &mut [NativeNode], report: &mut serde_json::Value) -> TestResult<()> {
    // Distinct native lifecycle allowance: the maintained daemon has a30s
    // network-context join plus component/runtime drains. This never extends
    // an RPC or the existing5s SIGKILL/reap owner deadline.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut failures = Vec::new();
    for node in nodes.iter_mut() {
        if node.owned.child.is_none() {
            continue;
        }
        if !node.released {
            node.owned.child.as_mut().unwrap().stdin.take();
            continue;
        }
        let request = ObjectRpcRequest::encode_body(
            sinomo_daemon::DAEMON_SHUTDOWN_ROUTE,
            sinomo_daemon::DAEMON_RPC_CATEGORY,
            &(),
        )?;
        let result = local_rpc(
            node,
            request,
            OP_CAP.min(deadline.saturating_duration_since(Instant::now())),
        );
        if let Err(e) = result {
            failures.push(format!("{} shutdown request: {e}", node.input.node_id));
        }
    }
    let mut exits = Vec::new();
    for node in nodes.iter_mut() {
        if node.owned.child.is_none() {
            continue;
        }
        loop {
            if let Some(status) = node.owned.child.as_mut().unwrap().try_wait()? {
                node.owned.child = None;
                let id = node.owned.identity.as_ref().unwrap();
                ensure_group_empty(id)?;
                exits.push(serde_json::json!({"node":node.input.node_id,"identity":id,"exit":status.code(),"signal":status.signal()}));
                if node.released && !status.success() {
                    failures.push(format!(
                        "{} normal native exit {status}",
                        node.input.node_id
                    ));
                }
                break;
            }
            if Instant::now() >= deadline {
                failures.push(format!(
                    "{} native normal drain exceeded original60s",
                    node.input.node_id
                ));
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    if Instant::now() > deadline {
        failures.push("native lifecycle drain exceeded its original60s envelope".into());
    }
    report["normal_drain"] = serde_json::json!({"exits":exits,"errors":failures});
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

fn preserve_native_failure_diagnostics(nodes: &mut [NativeNode], root: &Path) -> serde_json::Value {
    let mut observations = Vec::new();
    for (index, node) in nodes.iter_mut().enumerate() {
        if !node.released || node.alive_identity().is_err() {
            continue;
        }
        for (label, route, category) in [
            (
                "status",
                sinomo_daemon::DAEMON_STATUS_ROUTE,
                sinomo_daemon::DAEMON_RPC_CATEGORY,
            ),
            (
                "connectivity",
                sinomo_core::network::CLUSTER_NETWORK_CONNECTIVITY_ROUTE,
                sinomo_core::network::CLUSTER_NETWORK_RPC_CATEGORY,
            ),
        ] {
            let result = (|| -> TestResult<serde_json::Value> {
                let request = ObjectRpcRequest::encode_body(route, category, &())?;
                let response = local_rpc(node, request, OP_CAP)?;
                response.error_detail()?;
                let path = root.join(format!("failure-node-{index}-{label}.sobj"));
                write_data(&path, &response)?;
                let value = if label == "status" {
                    serde_json::to_value(response.decode_body::<sinomo_daemon::DaemonStatus>()?)?
                } else {
                    serde_json::to_value(
                        response
                            .decode_body::<Vec<sinomo_core::network::ClusterConnectivityRecord>>(
                            )?,
                    )?
                };
                Ok(serde_json::json!({"actual_response":value,"raw":path}))
            })();
            observations.push(serde_json::json!({"node":node.input.node_id,"kind":label,
                "result":result.as_ref().map(|value|value).map_err(|error|error.to_string()),
                "qualification":"actual read-only native diagnostics, not a routing/authority certificate"}));
        }
    }
    serde_json::json!(observations)
}

fn observe_native_prefix(
    node: &mut NativeNode,
    root: &Path,
    report: &mut serde_json::Value,
    phase: &str,
    budget: Duration,
) -> TestResult<Option<serde_json::Value>> {
    let local = node.input.node_id.clone();
    let result = probe(
        node,
        &local,
        ProbeKind::PrefixRound,
        format!("owned-prefix-{}", sinomo_core::Id::generate()),
        budget,
    );
    record_probe(root, report, phase, &result)?;
    let Ok(response) = result else {
        return Ok(None);
    };
    let Ok(reply) = response.result else {
        return Ok(None);
    };
    let data: serde_json::Value = sinomo_core::object::serde_ext::from_bytes(&reply.body)?;
    let target: crate::DfsMetadataQuorumSyncTarget =
        serde_json::from_value(data["target"].clone())?;
    let ids = std::iter::once(target.primary_node_id.clone())
        .chain(target.replica_node_ids.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(ids, node.contacts.keys().cloned().collect::<BTreeSet<_>>());
    assert_eq!(target.replica_node_ids.len(), 2);
    assert_eq!(target.primary_node_id, node.input.node_id);
    if data["complete"] == true {
        assert_eq!(data["round_complete"], true);
        assert_eq!(data["deadline_expired"], false);
        let peers = data["peers"]
            .as_array()
            .ok_or("native prefix peer observations absent")?;
        assert_eq!(peers.len(), 2);
        assert!(peers.iter().all(|p| p["attempted"] == true
            && p["native_present"] == true
            && p["binding_valid"] == true));
    }
    Ok(Some(data))
}

fn run_native_process_case(
    endpoint_mode: bool,
    retained_wrong_owner: Option<bool>,
    registered_endpoint: bool,
    prefix_round: bool,
) {
    let fixture = sinomo_testdir::TestDir::new("dfs-per-node-native-process-control");
    let root = fixture.path().to_path_buf();
    let mut nodes = Vec::new();
    let ping_kind = if registered_endpoint {
        ProbeKind::RegisteredEndpointPing
    } else if endpoint_mode {
        ProbeKind::EndpointPing
    } else {
        ProbeKind::Ping
    };
    let status_kind = if endpoint_mode {
        ProbeKind::EndpointDfsStatus
    } else {
        ProbeKind::DfsStatus
    };
    let mut report = serde_json::json!({"scope":"independent real daemon processes and native authenticated TCP/routed stream calls; no Fabric","explicit_endpoint_diagnostic":endpoint_mode,"formation_native_observation_requested":prefix_round,
        "limits":"initial control voter A, nonvoters B/C; no control-quorum f1/metadata serving/quietness acceptance or private provenance reconstructed",
        "caps":{"startup_seconds":60,"rpc_seconds":5,"owned_reap_seconds":5,"normal_drain_seconds":60},"probes":[]});
    let tested = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> TestResult<()> {
        let deadline = Instant::now() + READY_CAP;
        for i in 0..3 {
            nodes.push(launch_native_node(i)?);
        }
        for node in &mut nodes {
            let hello = read_hello(node, deadline)?;
            node.hello = Some(hello);
        }
        assert_eq!(
            nodes
                .iter()
                .map(|node| node.owned.identity.as_ref().unwrap().pid)
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
        configure_nodes(&mut nodes, prefix_round)?;
        report["nodes"]=serde_json::json!(nodes.iter().map(|n|serde_json::json!({"identity":n.owned.identity,"node":n.input.node_id,"data_root":n.input.data_root,"hello":n.hello})).collect::<Vec<_>>());
        for (source, target) in [(0, 1), (1, 2), (2, 0), (0, 2), (2, 1), (1, 0)] {
            wait_for_native_ping(
                &mut nodes,
                source,
                target,
                if prefix_round {
                    ProbeKind::FormationEndpointPing
                } else {
                    ping_kind
                },
                deadline,
                &root,
                &mut report,
            )?;
        }
        for (source, target) in [(0, 1), (1, 2), (2, 0)] {
            let peer = nodes[target].input.node_id.clone();
            let result = probe(
                &mut nodes[source],
                &peer,
                status_kind,
                format!("baseline-dfs-{}", sinomo_core::Id::generate()),
                OP_CAP,
            );
            record_probe(&root, &mut report, "baseline-native-dfs-status", &result)?;
            if !endpoint_mode {
                assert!(
                    matches!(&result,Ok(value) if value.result.is_ok()),
                    "native DFS status route unavailable: {result:?}"
                );
            }
        }
        if endpoint_mode {
            nodes[1].alive_identity()?;
            nodes[2].alive_identity()?;
            let intended = nodes[2].input.node_id.clone();
            let wrong = probe(
                &mut nodes[0],
                &intended,
                if registered_endpoint {
                    ProbeKind::RegisteredEndpointWrongPeer
                } else {
                    ProbeKind::EndpointWrongPeer
                },
                format!("wrong-contact-{}", sinomo_core::Id::generate()),
                OP_CAP,
            );
            record_probe(
                &root,
                &mut report,
                "wrong-live-peer-contact-refusal",
                &wrong,
            )?;
            assert!(
                !matches!(&wrong, Ok(value) if value.result.is_ok()),
                "wrong live endpoint cannot authenticate as the intended peer"
            );
            nodes[1].alive_identity()?;
            nodes[2].alive_identity()?;
        }
        if registered_endpoint {
            let intended = nodes[2].input.node_id.clone();
            let bad = probe(
                &mut nodes[0],
                &intended,
                ProbeKind::RegisteredEndpointWrongId,
                format!("unknown-endpoint-{}", sinomo_core::Id::generate()),
                OP_CAP,
            );
            record_probe(
                &root,
                &mut report,
                "native-unknown-endpoint-id-refusal",
                &bad,
            )?;
            assert!(
                matches!(&bad, Ok(value) if value.result.is_err() && value.contact.is_some()),
                "unknown endpoint must reach actual registered native refusal: {bad:?}"
            );
            nodes[2].alive_identity()?;
            let good = probe(
                &mut nodes[0],
                &intended,
                ProbeKind::RegisteredEndpointPing,
                format!("after-unknown-endpoint-{}", sinomo_core::Id::generate()),
                OP_CAP,
            );
            record_probe(
                &root,
                &mut report,
                "actual-valid-endpoint-after-refusal",
                &good,
            )?;
            assert!(
                matches!(&good, Ok(value) if value.result.is_ok()),
                "same live peer must still answer its genuine endpoint: {good:?}"
            );
        }
        if prefix_round {
            let mut complete = 0;
            let mut observations = Vec::new();
            let mut rounds = 0;
            // Listener/ping readiness is not DFS-owner readiness. Observe the
            // existing owner within the ORIGINAL startup deadline, without
            // warming/reopening it or replacing an expected generation.
            while complete == 0 && Instant::now() < deadline {
                rounds += 1;
                for node in &mut nodes {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    let data = observe_native_prefix(
                        node,
                        &root,
                        &mut report,
                        "baseline-owned-native-prefix",
                        OP_CAP.min(left),
                    )?;
                    complete += usize::from(data.as_ref().is_some_and(|v| v["complete"] == true));
                    observations.push(data);
                    if complete > 0 {
                        break;
                    }
                }
                if complete == 0 {
                    // At most30 three-node rounds fit60s: retain all refusals
                    // without exhausting the unchanged128-record evidence cap.
                    std::thread::sleep(
                        Duration::from_secs(2)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
            }
            let expired = Instant::now() >= deadline;
            report["prefix_baseline"] = serde_json::json!({"complete_rounds":complete,
                "observation_rounds":rounds,"original_startup_deadline_expired":expired,
                "observations":observations});
            assert!(
                complete > 0 && !expired,
                "native positive prerequisite has no actual complete full3 PREFIX round within original startup60s"
            );
        }
        let b = nodes[1].alive_identity()?;
        let c = nodes[2].alive_identity()?;
        let a_id = nodes[0].input.node_id.clone();
        let a_data = nodes[0].input.data_root.clone();
        let a_dir = DirectoryId::read(&a_data)?;
        let a_pin = File::open(&a_data)?;
        let killed = nodes[0].owned.kill_and_reap(Instant::now() + REAP_CAP)?;
        assert_eq!(killed.signal(), Some(9));
        assert_eq!(DirectoryId::read(&a_data)?, a_dir);
        assert_eq!(DirectoryId::of(&a_pin.metadata()?), a_dir);
        report["a_kill"] = serde_json::json!({"identity":nodes[0].owned.identity,"signal":killed.signal(),"only_A_killed":true});
        assert_eq!(nodes[1].alive_identity()?, b);
        assert_eq!(nodes[2].alive_identity()?, c);
        let mut entries = 0;
        let mut bytes = 0;
        let rawpins = PinnedTree::open(a_data, a_dir.uid, &mut entries, &mut bytes)?;
        let raw = rawpins.capture()?;
        let encoded = crate::metadata_io::encode_sobj(&raw)?;
        assert!(encoded.len() <= RAW_CAP + DATA_CAP);
        crate::metadata_io::write_file_atomic(
            &root.join("raw-A-after-kill-before-any-reopen.sobj"),
            &encoded,
        )?;
        report["a_raw"] = serde_json::json!({"files":raw.files.len(),"entries":entries,"bytes":bytes,"blake3":blake3::hash(&encoded).to_hex().to_string(),"before_any_reopen":true});
        for (source, target) in [(1, 2), (2, 1)] {
            let peer = nodes[target].input.node_id.clone();
            let result = probe(
                &mut nodes[source],
                &peer,
                ping_kind,
                format!("survivor-{}", sinomo_core::Id::generate()),
                OP_CAP,
            );
            record_probe(
                &root,
                &mut report,
                "post-A-kill-survivor-native-ping",
                &result,
            )?;
            assert!(
                matches!(&result,Ok(value) if value.result.is_ok()),
                "survivor native ping failed: {result:?}"
            );
            assert_eq!(nodes[1].alive_identity()?, b);
            assert_eq!(nodes[2].alive_identity()?, c);
        }
        let failed = probe(
            &mut nodes[1],
            &a_id,
            ping_kind,
            format!("dead-A-{}", sinomo_core::Id::generate()),
            OP_CAP,
        );
        record_probe(&root, &mut report, "post-A-kill-actual-failure", &failed)?;
        assert!(
            !matches!(&failed,Ok(value) if value.result.is_ok()),
            "dead A cannot satisfy a fresh native nonce"
        );
        for (source, target) in [(1, 2), (2, 1)] {
            let peer = nodes[target].input.node_id.clone();
            let status = probe(
                &mut nodes[source],
                &peer,
                status_kind,
                format!("survivor-dfs-{}", sinomo_core::Id::generate()),
                OP_CAP,
            );
            record_probe(
                &root,
                &mut report,
                "post-A-kill-dfs-observation-or-refusal",
                &status,
            )?;
        }
        assert_eq!(nodes[1].alive_identity()?, b);
        assert_eq!(nodes[2].alive_identity()?, c);
        assert_eq!(
            rawpins.capture()?,
            raw,
            "A's retained files must not be rewritten by survivor observation"
        );
        report["survivors"] = serde_json::json!({"B":b,"C":c,"same_processes_alive_after_calls":true,"A_data_unchanged":true});
        if prefix_round {
            let mut observations = Vec::new();
            for node in &mut nodes[1..] {
                let data = observe_native_prefix(
                    node,
                    &root,
                    &mut report,
                    "after-A-death-owned-native-prefix",
                    OP_CAP,
                )?;
                assert!(
                    !data.as_ref().is_some_and(|value| value["complete"] == true),
                    "full expected participants cannot complete a new round after A death"
                );
                observations.push(data);
            }
            report["prefix_after_loss"] =
                serde_json::json!({"observations":observations,"serving_authority":false});
            assert_eq!(nodes[1].alive_identity()?, b);
            assert_eq!(nodes[2].alive_identity()?, c);
        }

        if let Some(wrong_owner) = retained_wrong_owner {
            let old = nodes[0].owned.identity.as_ref().unwrap().clone();
            let original_hello = nodes[0].hello.as_ref().unwrap().clone();
            let mut expected = retained_expectation(&nodes[0], &raw)?;
            if wrong_owner {
                // Only supplied observation is wrong; retained bytes are never
                // replaced, deleted or made available through a fresh root.
                expected.data_identity[1] ^= 1;
            }
            report["restart_expected"] = serde_json::to_value(&expected)?;
            let restart_deadline = Instant::now() + READY_CAP;
            restart_retained_node(&mut nodes[0], expected)?;
            report["restart_process"] = serde_json::to_value(&nodes[0].owned.identity)?;
            if wrong_owner {
                let status = wait_for_retained_refusal(&mut nodes[0], restart_deadline)?;
                assert!(
                    !status.success() && status.signal().is_none(),
                    "expected explicit preflight refusal, not crash"
                );
                assert!(
                    !nodes[0]
                        .input
                        .owned_root
                        .join("restart-hello.sobj")
                        .try_exists()?
                );
                let log = fs::read_to_string(nodes[0].input.owned_root.join("restart.stderr.log"))?;
                assert!(
                    log.contains("retained native physical source was replaced"),
                    "wrong rejection: {log}"
                );
                assert_eq!(
                    rawpins.capture()?,
                    raw,
                    "refused restart cannot mutate retained DATA"
                );
                report["restart_refusal"] = serde_json::json!({"exit":status.code(),"signal":status.signal(),"data_unchanged":true,"hello_absent":true});
            } else {
                let hello = read_hello(&mut nodes[0], restart_deadline)?;
                assert_ne!(hello.pid, old.pid);
                assert_eq!(hello.node_id, original_hello.node_id);
                assert_eq!(hello.public_key, original_hello.public_key);
                assert_eq!(hello.address, original_hello.address);
                nodes[0].hello = Some(hello.clone());
                nodes[0].released = true;
                for (source, target) in [(1, 0), (0, 1), (2, 0), (0, 2)] {
                    wait_for_native_ping(
                        &mut nodes,
                        source,
                        target,
                        ProbeKind::EndpointPing,
                        restart_deadline,
                        &root,
                        &mut report,
                    )?;
                    assert_eq!(nodes[1].alive_identity()?, b);
                    assert_eq!(nodes[2].alive_identity()?, c);
                }
                assert_eq!(DirectoryId::read(&nodes[0].input.data_root)?, a_dir);
                // The immutable native identity file remains byte/physical equal;
                // genuine restarted WAL activity is not required to be static.
                let key = sinomo_core::node::NodeIdentityPaths::new(&nodes[0].input.data_root).main;
                let captured_key = raw
                    .files
                    .iter()
                    .find(|f| f.relative == key.strip_prefix(&raw.root).unwrap())
                    .unwrap();
                assert_eq!(
                    FileId::of(&fs::symlink_metadata(&key)?),
                    captured_key.physical
                );
                assert_eq!(fs::read(key)?, captured_key.bytes);
                report["reconnected"] = serde_json::json!({"new_A":nodes[0].alive_identity()?,"hello":hello,"same_data_root":true,"same_identity_file":true,
                    "B":b,"C":c,"qualification":"fresh authenticated endpoint observations only; no private PREFIX or current serving authority"});
            }
            assert_eq!(nodes[1].alive_identity()?, b);
            assert_eq!(nodes[2].alive_identity()?, c);
        }
        Ok(())
    }));
    report["test_outcome"] = match &tested {
        Ok(Ok(())) => serde_json::json!("completed"),
        Ok(Err(e)) => serde_json::json!(e.to_string()),
        Err(_) => serde_json::json!("panic; original assertion retained"),
    };
    if !matches!(&tested, Ok(Ok(()))) {
        report["native_failure_diagnostics"] =
            preserve_native_failure_diagnostics(&mut nodes, &root);
    }
    let drain = normal_drain(&mut nodes, &mut report);
    report["normal_drain_result"] = serde_json::json!(
        drain
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.to_string())
    );
    report["bootstrap_owner_observations"] = serde_json::json!(
        nodes
            .iter()
            .map(|node| {
                let path = node
                    .input
                    .owned_root
                    .join(if node.input.retained.is_some() {
                        "restart-bootstrap-owner-observation.sobj"
                    } else {
                        "bootstrap-owner-observation.sobj"
                    });
                let observed = read_data::<serde_json::Value>(&path);
                serde_json::json!({"node":node.input.node_id,"process":node.owned.identity,
            "observation":observed.as_ref().map(|v|v).map_err(|e|e.to_string()),
            "qualification":"post-drain diagnostic DATA; killed owner can be unavailable"})
            })
            .collect::<Vec<_>>()
    );
    let saved = save_report(&root.join("per-node-native-process-report.json"), &report);
    if !matches!(&tested, Ok(Ok(()))) || drain.is_err() || saved.is_err() {
        for node in &mut nodes {
            node.owned.keep();
        }
    }
    // Drop retains the exact original5s fallback kill/reap ownership on errors.
    drop(nodes);
    if let Err(panic) = tested {
        std::panic::resume_unwind(panic);
    }
    tested.unwrap().unwrap();
    drain.unwrap();
    saved.unwrap();
}

#[test]
fn native_per_node_tcp_survivors_keep_authenticated_calls_after_a_sigkill() {
    run_native_process_case(false, None, false, false);
}

#[test]
fn native_authenticated_endpoint_processes_survive_a_loss_without_routing_authority() {
    run_native_process_case(true, None, false, false);
}

#[test]
fn native_retained_a_reconnects_same_identity_while_b_c_stay_alive() {
    run_native_process_case(true, Some(false), false, false);
}

#[test]
fn native_retained_a_wrong_physical_owner_refuses_without_data_mutation() {
    run_native_process_case(true, Some(true), false, false);
}

#[test]
fn native_registered_endpoint_calls_survive_a_loss_without_route_fallback() {
    run_native_process_case(true, None, true, false);
}

#[test]
fn native_owned_prefix_full_set_observation_refuses_after_a_process_loss() {
    run_native_process_case(true, None, true, true);
}

#[test]
fn generic_component_carrier_cannot_fallback_for_explicit_endpoint() {
    use sinomo_core::network::{
        ClusterObjectRpcOptions, NetworkDeliveryState, NetworkError, ObjectRpcCall, ObjectRpcResult,
    };
    use sinomo_module::ComponentObjectRpcTransport;
    struct Generic(std::sync::atomic::AtomicUsize);
    impl ComponentObjectRpcTransport for Generic {
        fn call(
            &self,
            _: &NodeId,
            _: ObjectRpcCall,
            _: ClusterObjectRpcOptions,
            _: bool,
        ) -> Result<ObjectRpcResult, NetworkError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("explicit endpoint may not fall back to generic call");
        }
        fn call_async<'a>(
            &'a self,
            _: &'a NodeId,
            _: ObjectRpcCall,
            _: ClusterObjectRpcOptions,
            _: bool,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a,
            >,
        > {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { panic!("explicit endpoint may not fall back to generic async call") })
        }
    }
    let carrier = Generic(std::sync::atomic::AtomicUsize::new(0));
    let target = NodeId::generate();
    let endpoint = sinomo_core::network::daemon_lan_endpoint(&target, "127.0.0.1:1".to_string(), 0);
    let call = ObjectRpcCall::value(
        sinomo_core::cluster::cluster_network_ping_rpc_request("unsupported-native-endpoint", 0)
            .unwrap(),
    );
    let result = carrier.call_endpoint(
        &endpoint,
        call,
        ClusterObjectRpcOptions::default().with_overall_timeout(OP_CAP),
        false,
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("generic carrier manufactured endpoint result"),
    };
    assert_eq!(
        error.delivery_state(),
        Some(NetworkDeliveryState::DefinitelyNotSent)
    );
    assert_eq!(carrier.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}
