//! Strict, opt-in kernel FUSE qualification against an isolated native daemon.
//! No model turn is submitted. Failure retains the private DATA and receipt.
#![cfg(all(target_os = "linux", feature = "dfs-fuse"))]

use sinomo_client::{ChatDaemonClient, ChatHistoryClient, SifsBrowser, chat_contract::ChatEvent};
use sinomo_dfs_fuse::{
    DfsFuseMountOptions, mount_sifs_background, preflight_linux_sifs_background_mount,
};
use sinomo_runtime::{
    api::{
        DaemonApiClient, DaemonPaths, DaemonProbeStatus, DaemonStatus,
        SINOMO_APP_DAEMON_LAUNCH_ID_ENV, probe_daemon_status,
    },
    cluster::ClusterControlInitRequest,
    local::SinomoDataDir,
};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Node {
    child: Child,
    status: Option<DaemonStatus>,
    sifs_readiness: Option<serde_json::Value>,
    exit: Arc<Mutex<ExitObservation>>,
}

#[derive(Default)]
struct ExitObservation {
    forced: bool,
    status: Option<ExitStatus>,
}

impl Node {
    fn start(data: &SinomoDataDir, port: u16, phase: &str) -> Self {
        let launch = format!("native-sifs-{}-{port}-{phase}", std::process::id());
        let mut node = Self {
            child: Command::new(env!("CARGO_BIN_EXE_sinomo"))
                .args([
                    "daemon",
                    "run",
                    "--app-owned",
                    "--host",
                    "127.0.0.1",
                    "--port",
                ])
                .arg(port.to_string())
                .env("SINOMO_DATA_DIR", data.root())
                .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, &launch)
                .stdin(Stdio::piped())
                .stdout(File::create(data.root().join(format!("{phase}.stdout"))).unwrap())
                .stderr(File::create(data.root().join(format!("{phase}.stderr"))).unwrap())
                .spawn()
                .expect("start the isolated candidate daemon"),
            status: None,
            sifs_readiness: None,
            exit: Arc::new(Mutex::new(ExitObservation::default())),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                node.child.try_wait().unwrap().is_none(),
                "isolated daemon exited; inspect retained {phase}.stderr"
            );
            if let DaemonProbeStatus::Running(status) =
                probe_daemon_status(&DaemonPaths::new(data), Duration::from_millis(300))
            {
                assert_eq!(
                    status.pid,
                    node.child.id(),
                    "never attach to another process"
                );
                assert_eq!(status.app_launch_id.as_deref(), Some(launch.as_str()));
                assert_eq!(status.port, port);
                assert_eq!(status.commit.as_deref(), Some(env!("SINOMO_GIT_COMMIT")));
                assert_eq!(
                    fs::read_link(format!("/proc/{}/exe", status.pid)).unwrap(),
                    fs::canonicalize(env!("CARGO_BIN_EXE_sinomo")).unwrap(),
                    "kernel executable identity must match the candidate"
                );
                assert_eq!(
                    fs::canonicalize(&status.data_dir).unwrap(),
                    fs::canonicalize(data.root()).unwrap()
                );
                if let Some(cluster) = &status.cluster {
                    // Status establishes process identity, not publication of
                    // component capabilities. Observe the same local catalog
                    // used by chat discovery before exercising its SIFS reads.
                    // Only a successful, not-yet-published observation waits;
                    // RPC/decoding/permission failures remain immediate errors.
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    assert!(
                        !remaining.is_zero(),
                        "isolated SIFS catalog readiness timed out"
                    );
                    let client = DaemonApiClient::at_address(&status.address)
                        .with_timeout(remaining.min(Duration::from_millis(300)));
                    let request = sinomo_runtime::network::ObjectRpcRequest::encode_body(
                        sinomo_runtime::kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
                        sinomo_runtime::kv::CLUSTER_KV_RPC_CATEGORY,
                        &sinomo_runtime::service::CLUSTER_SERVICES_KV_PREFIX.to_string(),
                    )
                    .unwrap();
                    let read: sinomo_runtime::kv::KvLocalRead<Vec<sinomo_runtime::kv::KvEntry>> =
                        client
                            .call_request(&request)
                            .expect("observe owned local service catalog")
                            .decode_body()
                            .expect("decode owned local service catalog");
                    let instances =
                        sinomo_runtime::service::service_instances_from_entries(&read.value)
                            .expect("decode advertised provider identities");
                    let providers = sinomo_runtime::service::service_instances_providing_capability(
                        instances,
                        sinomo_runtime::dfs::SIFS_PATH_ACCESS_CAPABILITY,
                    );
                    if !providers
                        .iter()
                        .any(|provider| provider.node_id == cluster.node_id)
                    {
                        assert!(
                            Instant::now() < deadline,
                            "owned SIFS provider was not published"
                        );
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                    // Published capability is distinct from the resident
                    // metadata generation completing bootstrap. Status reads
                    // do not initialize stores or grant serving authority.
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    assert!(!remaining.is_zero(), "isolated DFS bootstrap timed out");
                    let request = sinomo_runtime::dfs::dfs_status_rpc_request().unwrap();
                    let response = client
                        .with_timeout(remaining.min(Duration::from_millis(300)))
                        .call_request(&request)
                        .expect("observe resident DFS readiness");
                    let dfs_status = sinomo_runtime::dfs::decode_dfs_status_rpc_response(&response)
                        .expect("decode resident DFS status");
                    if !dfs_status.incomplete_reasons.is_empty()
                        || dfs_status.file_system_count == 0
                        || dfs_status.object_count == 0
                        || dfs_status.metadata_shards.is_empty()
                        || dfs_status.metadata_shards.iter().any(|shard| {
                            shard.primary_node_id.as_ref() != Some(&cluster.node_id)
                                || shard.journal_last_sequence == 0
                        })
                    {
                        assert!(
                            Instant::now() < deadline,
                            "owned DFS bootstrap is incomplete: {dfs_status:?}"
                        );
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                    node.sifs_readiness = Some(serde_json::json!({
                        "pid": status.pid, "launch_id": status.app_launch_id,
                        "node_id": cluster.node_id, "catalog_revision": read.revision.0,
                        "observed_term": read.observed_term, "applied_index": read.applied_index,
                        "providers": providers,
                        "dfs_status": dfs_status,
                        "authority": "observational prerequisite only; normal route checks remain required"
                    }));
                }
                node.status = Some(status);
                return node;
            }
            assert!(
                Instant::now() < deadline,
                "isolated candidate readiness timed out; inspect retained logs"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn status(&self) -> &DaemonStatus {
        self.status.as_ref().unwrap()
    }

    fn wait_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                self.exit.lock().unwrap().status = Some(status);
                break;
            }
            assert!(Instant::now() < deadline, "owned daemon did not drain");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn stop(&mut self) {
        drop(self.child.stdin.take());
        self.wait_exit();
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            drop(self.child.stdin.take());
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(25));
            }
            if self.child.try_wait().ok().flatten().is_none() {
                // This Child handle owns only this fixture's exact process.
                self.exit.lock().unwrap().forced = true;
                let _ = self.child.kill();
                eprintln!("native SIFS fixture required forced child cleanup");
            }
        }
        self.exit.lock().unwrap().status = self.child.wait().ok();
    }
}

fn mount_entry(path: &Path) -> Option<String> {
    let encoded = path
        .to_str()
        .unwrap()
        .replace('\\', "\\134")
        .replace(' ', "\\040")
        .replace('\t', "\\011")
        .replace('\n', "\\012");
    fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .find(|line| line.split_whitespace().nth(4) == Some(encoded.as_str()))
        .map(str::to_string)
}

fn record(file: &mut File, value: serde_json::Value) {
    writeln!(file, "{value}").unwrap();
    file.sync_all().unwrap();
    eprintln!("native-sifs-evidence {value}");
}

fn undeleted_chat_shutdown(fixture_unwind: bool) {
    use std::os::unix::fs::MetadataExt;
    let mode = if fixture_unwind { "unwind" } else { "normal" };
    let root = sinomo_testdir::TestDir::new(&format!("native-sifs-undeleted-{mode}")).keep();
    eprintln!("native-sifs-retained-root={}", root.display());
    let data = SinomoDataDir::new(root.join("data"));
    data.ensure_root_layout().unwrap();
    let mut receipt = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("receipt.jsonl"))
        .unwrap();
    let binary = fs::canonicalize(env!("CARGO_BIN_EXE_sinomo")).unwrap();
    let digest = blake3::hash(&fs::read(&binary).unwrap())
        .to_hex()
        .to_string();
    let options = DfsFuseMountOptions::new(data.external_mounts_dir().join("capability-check"))
        .with_data_dir(data.root());
    let capability = preflight_linux_sifs_background_mount(&options)
        .expect("native FUSE capability is required, without fallback");
    assert!(
        capability.direct_fuse_device_access,
        "device access denied; do not reroute"
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"intent", "mode":mode, "data":data.root(),
        "binary":binary,"binary_blake3":digest,"build_commit":env!("SINOMO_GIT_COMMIT"),
        "explicit_delete":false,"model_turn_submitted":false,"production":false}),
    );
    let port = TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut bootstrap = Node::start(&data, port, "bootstrap");
    record(
        &mut receipt,
        serde_json::json!({"phase":"bootstrap", "status":bootstrap.status()}),
    );
    DaemonApiClient::at_address(&bootstrap.status().address)
        .cluster_init(ClusterControlInitRequest {
            cluster_name: "Isolated undeleted chat shutdown".into(),
            node_display_name: Some("Private shutdown fixture".into()),
            node_role: None,
        })
        .expect("initialize fresh private DATA once");
    bootstrap.wait_exit();
    drop(bootstrap);
    let mut node = Node::start(&data, port, "main");
    let status = node.status().clone();
    let kernel_stat = fs::read_to_string(format!("/proc/{}/stat", status.pid)).unwrap();
    let start_ticks = kernel_stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .to_string();
    let data_meta = fs::metadata(data.root()).unwrap();
    let sentinel = format!(
        "undeleted {mode} {} {digest}",
        status.cluster.as_ref().unwrap().node_id
    );
    let browser = SifsBrowser::open_daemon(&status.address).unwrap();
    browser
        .write_file("/services/shutdown-sentinel", sentinel.as_bytes())
        .expect("single original sentinel write, no retry");
    let client = ChatDaemonClient::at_address(
        &status.address,
        format!("shutdown-{mode}-{}", std::process::id()),
    );
    let opened = client
        .open()
        .expect("open resident chat without a model turn");
    client
        .command(
            sinomo_client::chat_contract::ChatSessionCommand::SetWorkspace {
                session_id: String::new(),
                workspace_root: None,
            },
        )
        .expect("activate the hosted workspace without submitting a model turn");
    let mut cursor = opened.next_event_sequence;
    let deadline = Instant::now() + Duration::from_secs(30);
    let session = 'ready: loop {
        let batch = client
            .events(cursor, 128, Duration::from_millis(200))
            .unwrap();
        cursor = batch.next_sequence;
        for event in batch.events {
            match event.event {
                ChatEvent::Ready { session_id, .. } => break 'ready session_id,
                ChatEvent::Error { error, .. } => panic!("chat initialization: {error}"),
                _ => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "resident chat readiness deadline"
        );
    };
    let mount = data
        .external_mounts_dir()
        .join("sifs/by-process")
        .join(status.pid.to_string());
    let kernel_mount = mount_entry(&mount).expect("real resident-agent kernel mount required");
    assert!(kernel_mount.contains(" - fuse ") || kernel_mount.contains(" - fuse."));
    assert_eq!(
        fs::read(mount.join("services/shutdown-sentinel")).unwrap(),
        sentinel.as_bytes()
    );
    assert!(
        ChatHistoryClient::at_address(&status.address)
            .list()
            .unwrap()
            .iter()
            .any(|chat| chat.id == session),
        "resident chat must exist before shutdown"
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"resident", "status":status,"start_ticks":start_ticks,
        "data_dev":data_meta.dev(),"data_inode":data_meta.ino(),"mount":mount,"mountinfo":kernel_mount,
        "session_id":session,"chat_undeleted":true,"subscription_closed":false}),
    );
    let exit = node.exit.clone();
    if fixture_unwind {
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _owned_node = node;
            panic!("deliberate native fixture unwind after resident mount");
        }));
        let message = failed.unwrap_err();
        assert_eq!(
            message.downcast_ref::<&str>().copied(),
            Some("deliberate native fixture unwind after resident mount")
        );
    } else {
        node.stop();
        drop(node);
    }
    let exit = exit.lock().unwrap();
    record(
        &mut receipt,
        serde_json::json!({"phase":"child-exited", "mode":mode,
        "pid":status.pid,"forced":exit.forced,"exit_code":exit.status.and_then(|s|s.code()),
        "mount_after_exit":mount_entry(&mount),"chat_undeleted":true}),
    );
    assert!(
        !exit.forced,
        "forced termination cannot qualify graceful cleanup"
    );
    assert!(
        exit.status.is_some_and(|s| s.success()),
        "native daemon failed to shut down cleanly"
    );
    assert!(
        !Path::new(&format!("/proc/{}", status.pid)).exists(),
        "exact owned child must be reaped"
    );
    assert!(
        mount_entry(&mount).is_none(),
        "undeleted chat left an owned kernel mount after {mode}"
    );
    assert_eq!(
        fs::read_dir(&mount).unwrap().count(),
        0,
        "no local-directory substitute"
    );
    drop(exit);
    // A new owned generation observes the original undeleted conversation;
    // this is graceful persistence/readback, not abrupt-crash durability.
    let mut readback = Node::start(&data, port, "readback");
    assert_eq!(
        readback.status().cluster.as_ref().unwrap().node_id,
        status.cluster.as_ref().unwrap().node_id
    );
    assert_eq!(
        readback.status().cluster.as_ref().unwrap().cluster_id,
        status.cluster.as_ref().unwrap().cluster_id
    );
    assert_ne!(readback.status().app_launch_id, status.app_launch_id);
    assert!(
        ChatHistoryClient::at_address(&readback.status().address)
            .list()
            .unwrap()
            .iter()
            .any(|chat| chat.id == session),
        "shutdown must preserve the original undeleted chat"
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"retained-chat-readback", "status":readback.status(),
        "sifs_readiness":readback.sifs_readiness,
        "session_id":session,"chat_exists_not_deleted":true}),
    );
    // Reacquire a real agent mount on the retained DATA without touching the
    // prior mounted-mutation session records. Leave this chat undeleted too.
    let resumed = ChatDaemonClient::at_address(
        &readback.status().address,
        format!("readback-{mode}-{}", std::process::id()),
    );
    let opened = resumed.open().unwrap();
    resumed
        .command(
            sinomo_client::chat_contract::ChatSessionCommand::SetWorkspace {
                session_id: String::new(),
                workspace_root: None,
            },
        )
        .expect("activate the hosted workspace without submitting a model turn");
    let mut cursor = opened.next_event_sequence;
    let deadline = Instant::now() + Duration::from_secs(30);
    let resumed_session = 'ready: loop {
        let batch = resumed
            .events(cursor, 128, Duration::from_millis(200))
            .unwrap();
        cursor = batch.next_sequence;
        for event in batch.events {
            match event.event {
                ChatEvent::Ready { session_id, .. } => break 'ready session_id,
                ChatEvent::Error { error, .. } => panic!("retained DATA mount reattach: {error}"),
                _ => {}
            }
        }
        assert!(
            Instant::now() < deadline,
            "retained DATA chat readiness deadline"
        );
    };
    let resumed_mount = data
        .external_mounts_dir()
        .join("sifs/by-process")
        .join(readback.status().pid.to_string());
    let resumed_entry =
        mount_entry(&resumed_mount).expect("retained DATA must remount through the native owner");
    assert!(resumed_entry.contains(" - fuse ") || resumed_entry.contains(" - fuse."));
    assert_eq!(
        fs::read(resumed_mount.join("services/shutdown-sentinel")).unwrap(),
        sentinel.as_bytes()
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"retained-data-remounted", "status":readback.status(),
        "mount":resumed_mount,"mountinfo":resumed_entry,"session_id":resumed_session,
        "original_session_id":session,"mutation_session_records_modified_by_fixture":false}),
    );
    readback.stop();
    assert!(
        readback
            .exit
            .lock()
            .unwrap()
            .status
            .is_some_and(|s| s.success())
    );
    assert!(!readback.exit.lock().unwrap().forced);
    assert!(!Path::new(&format!("/proc/{}", readback.status().pid)).exists());
    assert!(
        mount_entry(
            &data
                .external_mounts_dir()
                .join("sifs/by-process")
                .join(readback.status().pid.to_string())
        )
        .is_none()
    );
    assert_eq!(
        blake3::hash(&fs::read(binary).unwrap()).to_hex().as_str(),
        digest
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"complete","mode":mode,"chat_undeleted":true,
        "kernel_mount_absent":true,"exact_child_reaped":true,"forced":false,
        "original_chat_preserved":true,
        "abrupt_termination_tested":false,"crash_durability_tested":false,"production_acceptance":false}),
    );
}

#[test]
#[ignore = "strict real kernel SIFS and private native process; explicitly invoke to qualify"]
fn native_undeleted_chat_normal_shutdown() {
    undeleted_chat_shutdown(false);
}

#[test]
#[ignore = "strict parent fixture unwind; no explicit chat deletion, forced exit or fallback may pass"]
fn native_undeleted_chat_fixture_unwind() {
    undeleted_chat_shutdown(true);
}

#[test]
#[ignore = "requires existing unprivileged /dev/fuse and fusermount; fails rather than falling back"]
fn native_sifs_kernel_roundtrip_restart_and_cleanup() {
    let root: PathBuf = sinomo_testdir::TestDir::new("native-sifs-kernel").keep();
    eprintln!("native-sifs-retained-root={}", root.display());
    let data = SinomoDataDir::new(root.join("data"));
    data.ensure_root_layout().unwrap();
    let mut receipt = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("receipt.jsonl"))
        .unwrap();
    let binary = fs::canonicalize(env!("CARGO_BIN_EXE_sinomo")).unwrap();
    let binary_digest = blake3::hash(&fs::read(&binary).unwrap())
        .to_hex()
        .to_string();
    let mount_point = data
        .external_mounts_dir()
        .join("sifs/by-process")
        .join(std::process::id().to_string());
    let mut options = DfsFuseMountOptions::new(&mount_point).with_data_dir(data.root());
    options.attr_ttl = Duration::ZERO;
    options.entry_ttl = Duration::ZERO;
    assert!(!mount_point.starts_with(data.root()) && !data.root().starts_with(&mount_point));
    record(
        &mut receipt,
        serde_json::json!({"phase":"intent", "binary":binary, "binary_blake3":binary_digest,
        "build_commit":env!("SINOMO_GIT_COMMIT"), "data":data.root(), "mount":mount_point,
        "model_turn_submitted":false, "fallback_allowed":false, "production":false}),
    );
    let preflight = preflight_linux_sifs_background_mount(&options)
        .expect("kernel/helper capability unavailable; do not escalate or use a fallback");
    assert!(
        preflight.direct_fuse_device_access,
        "direct /dev/fuse access denied; stop without another route"
    );
    assert!(
        !preflight.allow_other_enabled,
        "fixture never broadens mount access"
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"kernel-preflight", "device":preflight.fuse_device,"helper":preflight.mount_helper}),
    );

    let port = TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut bootstrap = Node::start(&data, port, "bootstrap");
    record(
        &mut receipt,
        serde_json::json!({"phase":"bootstrap", "status":bootstrap.status()}),
    );
    DaemonApiClient::at_address(&bootstrap.status().address)
        .cluster_init(ClusterControlInitRequest {
            cluster_name: "Isolated native SIFS qualification".into(),
            node_display_name: Some("Isolated native SIFS fixture".into()),
            node_role: None,
        })
        .expect("initialize only the fresh fixture DATA; no retry after an uncertain result");
    bootstrap.wait_exit();
    drop(bootstrap);

    let mut node = Node::start(&data, port, "main");
    let cluster = node.status().cluster.clone().expect("native Main identity");
    assert_eq!(
        cluster.process,
        sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"main", "status":node.status()}),
    );
    let browser = SifsBrowser::open_daemon(&node.status().address).unwrap();
    let original = "/services/native-sifs-api-source";
    let written = "/services/native-sifs-kernel-written";
    let renamed = "/services/native-sifs-kernel-renamed";
    let api_payload = format!(
        "API {} {} {binary_digest}",
        cluster.cluster_id, cluster.node_id
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"before-api-write", "path":original}),
    );
    browser
        .write_file(original, api_payload.as_bytes())
        .expect("original write outcome; never replay an uncertain write");
    // Exercise the agent's actual mount producer, not just the FUSE adapter.
    // A workspace command activates the dormant view without submitting a model turn.
    let client_id = format!("native-sifs-view-{}", std::process::id());
    let chat = ChatDaemonClient::at_address(&node.status().address, &client_id);
    let opened = chat
        .open()
        .expect("open the isolated hosted chat without a turn");
    chat.command(
        sinomo_client::chat_contract::ChatSessionCommand::SetWorkspace {
            session_id: String::new(),
            workspace_root: None,
        },
    )
    .expect("activate the hosted workspace without submitting a model turn");
    let mut cursor = opened.next_event_sequence;
    let ready_by = Instant::now() + Duration::from_secs(30);
    let (session_id, info) = 'ready: loop {
        let batch = chat
            .events(cursor, 128, Duration::from_millis(200))
            .unwrap();
        cursor = batch.next_sequence;
        for event in batch.events {
            match event.event {
                ChatEvent::Ready { session_id, info } => break 'ready (session_id, info),
                ChatEvent::Error { error, .. } => panic!("hosted initialization failed: {error}"),
                _ => {}
            }
        }
        assert!(
            Instant::now() < ready_by,
            "hosted initialization did not become ready"
        );
    };
    let agent_mount = data
        .external_mounts_dir()
        .join("sifs/by-process")
        .join(node.status().pid.to_string());
    let agent_mount_entry = mount_entry(&agent_mount)
        .expect("hosted agent used a fallback instead of its real SIFS kernel mount");
    assert!(agent_mount_entry.contains(" - fuse ") || agent_mount_entry.contains(" - fuse."));
    assert_eq!(
        fs::read(agent_mount.join(original.trim_start_matches('/'))).unwrap(),
        api_payload.as_bytes()
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"hosted-agent-native-sifs", "client_id":client_id,
        "session_id":session_id, "runtime":info, "view_generation":opened.generation,
        "mountinfo":agent_mount_entry,"model_turn_submitted":false}),
    );
    // The CLI and daemon have different lifetimes and independently fenced
    // FUSE sessions on the same installation.
    let second = mount_sifs_background(options.clone()).expect("concurrent CLI mount");
    let kernel_payload = format!("kernel {session_id} {} {binary_digest}", node.status().pid);
    let path = agent_mount.join(written.trim_start_matches('/'));
    record(
        &mut receipt,
        serde_json::json!({"phase":"before-kernel-write", "path":written}),
    );
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    file.write_all(kernel_payload.as_bytes()).unwrap();
    file.sync_all()
        .expect("commit kernel write through the native DFS boundary");
    drop(file);
    assert_eq!(
        fs::read(second.mount_root().join(written.trim_start_matches('/'))).unwrap(),
        kernel_payload.as_bytes()
    );
    second.shutdown().unwrap();
    assert!(mount_entry(&mount_point).is_none());
    assert!(mount_entry(&agent_mount).is_some());
    assert_eq!(
        browser.read_file(written).unwrap(),
        kernel_payload.as_bytes()
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"before-kernel-rename", "from":written,"to":renamed}),
    );
    fs::rename(&path, agent_mount.join(renamed.trim_start_matches('/'))).unwrap();
    assert_eq!(
        browser.read_file(renamed).unwrap(),
        kernel_payload.as_bytes()
    );
    // Closing a view only detaches it. Retire this disposable chat through
    // its owning service before stopping the process; never clear lease files.
    ChatHistoryClient::at_address(&node.status().address)
        .delete(&session_id)
        .unwrap();
    assert!(chat.close().unwrap().closed);
    let retire_by = Instant::now() + Duration::from_secs(10);
    while mount_entry(&agent_mount).is_some() {
        assert!(
            Instant::now() < retire_by,
            "deleted chat retained its owned mount"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(
        fs::read_dir(&agent_mount).unwrap().count(),
        0,
        "no native-directory copy can impersonate SIFS"
    );
    node.stop();
    assert!(
        mount_entry(&agent_mount).is_none(),
        "agent's process-owned mount survived daemon shutdown"
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"drained", "pid":node.child.id(),"mount_absent":true,"child_reaped":true}),
    );
    drop(node);
    let mut reopened = Node::start(&data, port, "reopened");
    assert_eq!(
        reopened.status().cluster.as_ref().unwrap().cluster_id,
        cluster.cluster_id
    );
    assert_eq!(
        reopened.status().cluster.as_ref().unwrap().node_id,
        cluster.node_id
    );
    let browser = SifsBrowser::open_daemon(&reopened.status().address).unwrap();
    assert_eq!(
        browser.read_file(renamed).unwrap(),
        kernel_payload.as_bytes()
    );
    let handle =
        mount_sifs_background(options).expect("reopen only after the successful owned unmount");
    assert_eq!(
        fs::read(handle.mount_root().join(renamed.trim_start_matches('/'))).unwrap(),
        kernel_payload.as_bytes()
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"reopened", "status":reopened.status(), "mountinfo":mount_entry(&mount_point)}),
    );
    handle.shutdown().unwrap();
    assert!(mount_entry(&mount_point).is_none());
    reopened.stop();
    assert_eq!(
        blake3::hash(&fs::read(&binary).unwrap()).to_hex().as_str(),
        binary_digest
    );
    record(
        &mut receipt,
        serde_json::json!({"phase":"complete", "kernel_sifs_roundtrip":true,"same_data_reopen":true,
        "mount_absent":true,"child_reaped":true,"production_readiness":false,"quietness_qualified":false}),
    );
}
