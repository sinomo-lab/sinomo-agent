use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sinomo_runtime::api::{
    DaemonApiClient, DaemonPaths, DaemonProbeStatus, SINOMO_APP_DAEMON_LAUNCH_ID_ENV,
    SINOMO_DAEMON_API_PROTOCOL_VERSION, SINOMO_DAEMON_DEFAULT_PORT, probe_daemon_status,
};
use sinomo_runtime::cluster::ClusterControlInitRequest;
use sinomo_runtime::local::SinomoDataDir;

const PROBE_TIMEOUT: Duration = Duration::from_millis(300);

struct TestProcess(Child);

impl std::ops::Deref for TestProcess {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for TestProcess {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

// The supervisor launches this test executable as a headless GUI stand-in. It uses the same
// client readiness path as the real GUI, allowing lifecycle and exit-code checks on every OS.
#[test]
#[ignore = "subprocess fixture for desktop_cli_supervises_and_reaps_its_owned_daemon"]
fn supervised_app_fixture() {
    assert!(std::env::var_os(sinomo_client::api::app::update::APP_SUPERVISED_ENV).is_some());
    assert!(std::env::var_os(sinomo_client::api::app::update::APP_LAUNCH_TOKEN_ENV).is_some());
    let data_dir = SinomoDataDir::from_env_or_home().unwrap();
    let status = sinomo_client::connect_to_supervised_daemon(&data_dir, &|_| {}).unwrap();
    std::fs::write(
        data_dir.root().join("fixture-daemon-pid"),
        status.pid.to_string(),
    )
    .unwrap();
    std::process::exit(
        std::env::var("SINOMO_TEST_APP_EXIT")
            .unwrap()
            .parse()
            .unwrap(),
    );
}

#[test]
fn desktop_cli_supervises_and_reaps_its_owned_daemon() {
    for exit_code in [0, 23] {
        let root = sinomo_testdir::TestDir::new("desktop-cli-supervisor");
        let data_dir = SinomoDataDir::new(root.join("data"));
        let mut child = TestProcess(
            desktop_supervisor_command(&data_dir, exit_code)
                .spawn()
                .expect("start CLI desktop supervisor"),
        );
        let exit = wait_for_exit(&mut child, Duration::from_secs(60));
        assert_eq!(exit.code(), Some(exit_code));
        assert!(data_dir.root().join("fixture-daemon-pid").is_file());
        assert!(matches!(
            probe_daemon_status(&DaemonPaths::new(&data_dir), PROBE_TIMEOUT),
            DaemonProbeStatus::NotRunning
        ));
    }
}

#[test]
fn desktop_cli_leaves_an_independent_daemon_running() {
    let root = sinomo_testdir::TestDir::new("desktop-cli-independent-daemon");
    let data_dir = SinomoDataDir::new(root.join("data"));
    let mut daemon = TestProcess(
        Command::new(env!("CARGO_BIN_EXE_sinomo"))
            .args(["daemon", "run", "--host", "127.0.0.1", "--port"])
            .arg(available_loopback_port().to_string())
            .env("SINOMO_DATA_DIR", data_dir.root())
            .env_remove(SINOMO_APP_DAEMON_LAUNCH_ID_ENV)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let paths = DaemonPaths::new(&data_dir);
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        assert!(
            daemon.try_wait().unwrap().is_none(),
            "independent daemon exited"
        );
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, PROBE_TIMEOUT) {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "independent daemon readiness timed out"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(status.app_launch_id.is_none());
    let mut supervisor = TestProcess(desktop_supervisor_command(&data_dir, 0).spawn().unwrap());
    assert!(wait_for_exit(&mut supervisor, Duration::from_secs(30)).success());
    let still_running = match probe_daemon_status(&paths, PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => status,
        other => panic!("independent daemon was stopped: {other:?}"),
    };
    assert_eq!(still_running.pid, daemon.id());
    sinomo_runtime::daemon::send_daemon_shutdown_request(&status.address, Duration::from_secs(5))
        .unwrap();
    assert!(wait_for_exit(&mut daemon, Duration::from_secs(15)).success());
}

#[test]
fn app_owned_daemon_requires_a_launch_identity_and_stops_on_early_eof() {
    let root = sinomo_testdir::TestDir::new("desktop-cli-lease-validation");
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinomo"));
    command
        .args(["daemon", "run", "--app-owned"])
        .env("SINOMO_DATA_DIR", root.path())
        .env_remove(SINOMO_APP_DAEMON_LAUNCH_ID_ENV)
        .stdin(Stdio::null());
    let rejected = command.output().unwrap();
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr)
            .contains("requires an app lifecycle lease identity")
    );
    let mut child = TestProcess(
        command
            .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, "early-eof")
            .args(["--host", "127.0.0.1", "--port"])
            .arg(available_loopback_port().to_string())
            .spawn()
            .unwrap(),
    );
    assert!(wait_for_exit(&mut child, Duration::from_secs(15)).success());
    assert!(matches!(
        probe_daemon_status(
            &DaemonPaths::new(&SinomoDataDir::new(root.path())),
            PROBE_TIMEOUT
        ),
        DaemonProbeStatus::NotRunning
    ));
}

fn desktop_supervisor_command(data_dir: &SinomoDataDir, exit_code: i32) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinomo"));
    command
        .args(["app", "supervise", "--"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "supervised_app_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("SINOMO_DATA_DIR", data_dir.root())
        .env(
            sinomo_runtime::api::SINOMO_APP_DAEMON_BINARY_ENV,
            env!("CARGO_BIN_EXE_sinomo"),
        )
        .env(
            sinomo_runtime::api::SINOMO_DAEMON_PORT_ENV,
            available_loopback_port().to_string(),
        )
        .env("SINOMO_TEST_APP_EXIT", exit_code.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    command
}

#[test]
fn desktop_supervisor_initializes_and_restarts_only_its_owned_runtime() {
    let root = sinomo_testdir::TestDir::new("desktop-supervisor-runtime");
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().unwrap();
    let supervisor = sinomo_client::AppDaemonSupervisor::start(
        data_dir.clone(),
        Some(std::path::PathBuf::from(env!("CARGO_BIN_EXE_sinomo"))),
    )
    .unwrap();
    let status = sinomo_client::connect_to_supervised_daemon(&data_dir, &|_| {}).unwrap();
    assert!(
        status.cluster.is_some(),
        "the bootstrap helper must be replaced before frontend readiness"
    );
    assert!(status.app_launch_id.is_some());
    let paths = DaemonPaths::new(&data_dir);
    let original_pid = status.pid;
    let borrowed = sinomo_client::AppDaemonSupervisor::start(data_dir.clone(), None).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    drop(borrowed);
    let same = sinomo_client::connect_to_supervised_daemon(&data_dir, &|_| {}).unwrap();
    assert_eq!(
        same.pid, original_pid,
        "dropping an attaching GUI must not stop another owner's daemon"
    );
    drop(supervisor);
    assert!(matches!(
        probe_daemon_status(&paths, PROBE_TIMEOUT),
        DaemonProbeStatus::NotRunning
    ));
}

#[test]
fn automatic_port_fallback_is_discovered_through_shared_status() {
    let occupied = TcpListener::bind(("127.0.0.1", SINOMO_DAEMON_DEFAULT_PORT)).ok();
    let root = sinomo_testdir::TestDir::new("sinomo-app-owned-port-fallback");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().expect("data layout");
    let launch_id = format!("fallback-test-{}", std::process::id());
    let mut child = spawn_automatic_helper(&data_dir, &launch_id);
    let paths = DaemonPaths::new(&data_dir);
    let status = wait_for_owned_status(&mut child, &paths, &launch_id);

    assert_ne!(status.port, SINOMO_DAEMON_DEFAULT_PORT);
    assert_eq!(
        DaemonApiClient::connect_for_paths(&paths)
            .expect("discover fallback daemon")
            .address(),
        status.address
    );

    drop(child.stdin.take());
    assert!(wait_for_exit(&mut child, Duration::from_secs(15)).success());
    drop(occupied);
}

#[test]
fn lifetime_pipe_bounds_the_real_daemon_process() {
    let root = sinomo_testdir::TestDir::new("sinomo-app-owned-lifecycle");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().expect("data layout");
    let port = available_loopback_port();
    let launch_id = format!("lifecycle-test-{}-{port}", std::process::id());
    // DFS and history belong to the initialized product, not its bootstrap host.
    let bootstrap_id = format!("bootstrap-{launch_id}");
    let mut bootstrap = spawn_helper(&data_dir, port, &bootstrap_id);
    let bootstrap_status =
        wait_for_owned_status(&mut bootstrap, &DaemonPaths::new(&data_dir), &bootstrap_id);
    DaemonApiClient::at_address(bootstrap_status.address)
        .cluster_init(ClusterControlInitRequest {
            cluster_name: "Lifecycle test".into(),
            node_display_name: Some("Lifecycle test".into()),
            node_role: None,
        })
        .expect("initialize lifecycle test cluster");
    assert!(wait_for_exit(&mut bootstrap, Duration::from_secs(15)).success());
    let mut child = spawn_helper(&data_dir, port, &launch_id);

    let paths = DaemonPaths::new(&data_dir);
    let status = wait_for_owned_status(&mut child, &paths, &launch_id);
    assert_eq!(status.pid, child.id());
    assert_eq!(
        status.api_protocol_version,
        SINOMO_DAEMON_API_PROTOCOL_VERSION
    );
    let sifs = sinomo_client::SifsBrowser::open_daemon(status.address.clone())
        .expect("open SIFS through the frontend client");
    sifs.list("/")
        .expect("list SIFS through the frontend client");
    // Cluster service publication follows local daemon readiness asynchronously. Wait for
    // the same provider-backed catalog used by chat before opening a session.
    let agent_client = sinomo_client::AgentManagementClient::at_address(status.address.clone());
    let catalog_deadline = Instant::now() + Duration::from_secs(15);
    let agents = loop {
        let snapshot = agent_client
            .snapshot(None)
            .expect("query agent catalog readiness");
        if snapshot.error.is_none() {
            break snapshot;
        }
        assert!(
            Instant::now() < catalog_deadline,
            "agent catalog was not ready: {:?}",
            snapshot.error
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let chat = sinomo_client::ChatDaemonClient::at_address(
        status.address.clone(),
        format!("lifecycle-chat-{}-{port}", std::process::id()),
    )
    .with_timeout(Duration::from_secs(5));
    chat.open().expect("open daemon-hosted chat session");
    let ready = chat
        .events(0, 32, Duration::from_secs(3))
        .expect("poll daemon-hosted chat events");
    assert!(
        ready.events.iter().any(|event| matches!(
            event.event,
            sinomo_client::chat_contract::ChatEvent::Ready { .. }
        )),
        "daemon-hosted chat did not publish readiness: {:?}",
        ready.events
    );
    assert!(chat.close().expect("close chat session").closed);
    let history_client = sinomo_client::ChatHistoryClient::at_address(status.address.clone());
    let channel_id = sinomo_runtime::events::SessionId::generate().to_string();
    history_client
        .create_channel(
            sinomo_client::chat_contract::ChatHistoryCreateChannelRequest {
                session_id: channel_id.clone(),
                title: "Lifecycle channel".to_string(),
                agent_key: "main-agent".to_string(),
                agent_name: "main-agent".to_string(),
                sandbox_node_id: None,
            },
        )
        .expect("create channel through daemon history contract");
    assert!(
        history_client
            .list()
            .expect("list chat history through daemon contract")
            .iter()
            .any(|session| session.id == channel_id)
    );
    assert!(
        history_client
            .channel(channel_id.clone())
            .expect("load channel through daemon history contract")
            .is_some()
    );
    history_client
        .delete(channel_id)
        .expect("delete channel through daemon history contract");
    assert!(agents.agents.iter().any(|agent| agent.id == "main-agent"));
    let custom_agent = sinomo_client::agent_contract::AgentDefinition::custom(
        "lifecycle-agent",
        sinomo_client::agent_contract::AgentRole::Main,
        "Exercise the daemon agent management contract.",
    );
    agent_client
        .save(sinomo_client::agent_contract::AgentSaveRequest {
            definition: custom_agent,
            previous_id: None,
        })
        .expect("save agent through daemon management contract");
    assert!(
        agent_client
            .snapshot(Some("lifecycle-agent".to_string()))
            .expect("reload saved agent through daemon management contract")
            .agents
            .iter()
            .any(|agent| agent.id == "lifecycle-agent")
    );
    assert!(
        agent_client
            .delete("lifecycle-agent")
            .expect("delete agent through daemon management contract")
    );
    let _skills = sinomo_client::SkillClient::at_address(status.address.clone())
        .list(true)
        .expect("read skill catalog through daemon contract");
    // Evolution is a separately deployed native module. This base-product lifecycle fixture
    // intentionally has no external module artifacts; those routes have their own integration tests.

    drop(child.stdin.take());
    let exit = wait_for_exit(&mut child, Duration::from_secs(15));
    assert!(exit.success(), "lease-triggered daemon exit: {exit}");
    assert!(matches!(
        probe_daemon_status(&paths, PROBE_TIMEOUT),
        DaemonProbeStatus::NotRunning
    ));
}

#[test]
fn losing_helper_never_stops_the_existing_daemon() {
    let root = sinomo_testdir::TestDir::new("sinomo-app-owned-existing-owner");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().expect("data layout");
    let port = available_loopback_port();
    let first_launch = format!("existing-owner-{}-{port}", std::process::id());
    let mut owner = spawn_helper(&data_dir, port, &first_launch);
    let paths = DaemonPaths::new(&data_dir);
    let original = wait_for_owned_status(&mut owner, &paths, &first_launch);

    let losing_launch = format!("losing-owner-{}-{port}", std::process::id());
    let mut losing = spawn_helper(&data_dir, port, &losing_launch);
    let losing_exit = wait_for_exit(&mut losing, Duration::from_secs(10));
    assert!(!losing_exit.success(), "the lock-losing helper should fail");

    let still_running = match probe_daemon_status(&paths, PROBE_TIMEOUT) {
        DaemonProbeStatus::Running(status) => status,
        other => panic!("existing daemon was disturbed by losing helper: {other:?}"),
    };
    assert_eq!(still_running.pid, original.pid);
    assert_eq!(
        still_running.app_launch_id.as_deref(),
        Some(first_launch.as_str())
    );

    drop(owner.stdin.take());
    assert!(wait_for_exit(&mut owner, Duration::from_secs(15)).success());
}

#[test]
fn identity_changing_cluster_init_allows_an_owned_replacement() {
    let root = sinomo_testdir::TestDir::new("sinomo-app-owned-cluster-init-restart");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().expect("data layout");
    let port = available_loopback_port();
    let paths = DaemonPaths::new(&data_dir);
    let first_launch = format!("cluster-init-first-{}-{port}", std::process::id());
    let mut first = spawn_helper(&data_dir, port, &first_launch);
    let first_status = wait_for_owned_status(&mut first, &paths, &first_launch);
    assert!(first_status.cluster.is_none());

    DaemonApiClient::at_address(first_status.address)
        .with_timeout(Duration::from_secs(5))
        .cluster_init(ClusterControlInitRequest {
            cluster_name: "App-owned cluster".to_string(),
            node_display_name: Some("App-owned node".to_string()),
            node_role: None,
        })
        .expect("initialize cluster through daemon");
    assert!(wait_for_exit(&mut first, Duration::from_secs(15)).success());

    let second_launch = format!("cluster-init-second-{}-{port}", std::process::id());
    let mut second = spawn_helper(&data_dir, port, &second_launch);
    let second_status = wait_for_owned_status(&mut second, &paths, &second_launch);
    assert!(
        second_status.cluster.is_some(),
        "replacement daemon must capture initialized cluster identity"
    );

    drop(second.stdin.take());
    assert!(wait_for_exit(&mut second, Duration::from_secs(15)).success());
}

fn spawn_helper(data_dir: &SinomoDataDir, port: u16, launch_id: &str) -> TestProcess {
    TestProcess(
        Command::new(env!("CARGO_BIN_EXE_sinomo"))
            .args(["daemon", "run", "--app-owned"])
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .env("SINOMO_DATA_DIR", data_dir.root())
            .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, launch_id)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn app daemon helper"),
    )
}

fn spawn_automatic_helper(data_dir: &SinomoDataDir, launch_id: &str) -> TestProcess {
    TestProcess(
        Command::new(env!("CARGO_BIN_EXE_sinomo"))
            .args(["daemon", "run", "--app-owned"])
            .arg("--host")
            .arg("127.0.0.1")
            .env("SINOMO_DATA_DIR", data_dir.root())
            .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, launch_id)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn automatic app daemon helper"),
    )
}

fn available_loopback_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind port probe")
        .local_addr()
        .expect("port probe address")
        .port()
}

fn wait_for_owned_status(
    child: &mut Child,
    paths: &DaemonPaths,
    launch_id: &str,
) -> sinomo_runtime::api::DaemonStatus {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(exit) = child.try_wait().expect("inspect helper") {
            panic!("app daemon exited before readiness: {exit}");
        }
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(paths, PROBE_TIMEOUT)
            && status.pid == child.id()
            && status.app_launch_id.as_deref() == Some(launch_id)
        {
            return status;
        }
        assert!(Instant::now() < deadline, "app daemon readiness timed out");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(exit) = child.try_wait().expect("inspect helper") {
            return exit;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let exit = child.wait().expect("reap timed-out helper");
            panic!("app daemon did not exit after lifetime lease closed: {exit}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
