//! End-to-end chat through a real daemon whose model is the `scripted`
//! provider. Needs `--features scripted-provider`.
//!
//! The daemon runs with `SINOMO_SCRIPTED_PROVIDER_SCRIPT` naming the script
//! for that process; the provider writes `<script>.report.json`, which the
//! test checks once the daemon is done with it.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sinomo_client::chat_contract::{
    ChatEvent, ChatHistoryAuthor, ChatHistoryLoadMode, ChatHistoryLoadRequest, ChatHistoryMessage,
    ChatSessionCommand,
};
use sinomo_lm::{
    ModelScript, SCRIPTED_PROVIDER_SCRIPT_ENV, ScriptedExpectation, ScriptedReply, ScriptedReport,
};
use sinomo_runtime::api::{
    DaemonApiClient, DaemonPaths, DaemonProbeStatus, SINOMO_APP_DAEMON_LAUNCH_ID_ENV,
    probe_daemon_status,
};
use sinomo_runtime::cluster::ClusterControlInitRequest;
use sinomo_runtime::local::SinomoDataDir;

const PROBE_TIMEOUT: Duration = Duration::from_millis(300);
const TURN_TIMEOUT: Duration = Duration::from_secs(60);

fn answer(text: &str) -> ScriptedReply {
    ScriptedReply::new().text(
        serde_json::json!({
            "response_mode": "answer",
            "text": text,
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "listening",
        })
        .to_string(),
    )
}

#[test]
fn a_scripted_chat_turn_runs_and_its_context_survives_a_daemon_restart() {
    let root = sinomo_testdir::TestDir::new("sinomo-scripted-chat");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().expect("data layout");
    let scripts = root.join("scripts");
    let before_restart = scripts.join("before-restart.json");
    ModelScript::new()
        .expect(ScriptedExpectation::last_user_contains("Bluefin"))
        .reply(answer("Noted: the release codename is Bluefin."))
        .save(&before_restart)
        .unwrap();
    let after_restart = scripts.join("after-restart.json");
    ModelScript::new()
        .expect(ScriptedExpectation::last_user_contains("Which codename"))
        .expect(ScriptedExpectation::contains("Bluefin"))
        .reply(answer("You said the codename is Bluefin."))
        .save(&after_restart)
        .unwrap();

    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    let mut daemon = Daemon::start(&data_dir, port, "before-restart", &before_restart);
    run_cli(
        &data_dir,
        &[
            "models",
            "default",
            "--provider",
            "scripted",
            "--model",
            "scripted",
        ],
    );
    daemon.wait_for_agent_catalog();

    let chat = daemon.chat("scripted-chat-1");
    let (session_id, info) = chat.ready();
    assert_eq!(info.provider, "scripted", "{info:?}");
    let events = chat.send(&session_id, "Remember the release codename: Bluefin.");
    assert!(
        assistant_text(&events).contains("Bluefin"),
        "reply events: {events:?}"
    );
    daemon.stop();
    ScriptedReport::load(&before_restart.with_extension("report.json"))
        .unwrap()
        .assert_finished();

    let daemon = Daemon::start(&data_dir, port, "after-restart", &after_restart);
    daemon.wait_for_agent_catalog();
    let history = daemon.history(&session_id);
    assert!(
        history
            .iter()
            .any(|message| message.text.contains("Bluefin")),
        "stored history: {history:?}"
    );
    let chat = daemon.chat("scripted-chat-2");
    chat.ready();
    chat.command(ChatSessionCommand::Resume {
        session_id: session_id.clone(),
        history,
    });
    let events = chat.send(&session_id, "Which codename did I give you?");
    assert!(
        assistant_text(&events).contains("Bluefin"),
        "reply events: {events:?}"
    );
    drop(daemon);
    ScriptedReport::load(&after_restart.with_extension("report.json"))
        .unwrap()
        .assert_finished();
}

/// Exploratory: what the user and the model see after the daemon dies while
/// a model request is in flight.
#[test]
#[ignore = "exploratory crash probe"]
fn probe_crash_during_model_request() {
    let root = sinomo_testdir::TestDir::new("sinomo-scripted-crash-model").keep();
    eprintln!("root: {}", root.display());
    let data_dir = SinomoDataDir::new(root.clone());
    data_dir.ensure_root_layout().expect("data layout");
    let before = root.join("scripts").join("before-crash.json");
    ModelScript::new()
        .reply(answer("never sent").after_gate("never"))
        .save(&before)
        .unwrap();
    let after = root.join("scripts").join("after-crash.json");
    ModelScript::new()
        .reply(answer("Back again."))
        .reply(answer("spare"))
        .save(&after)
        .unwrap();

    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    let mut daemon = Daemon::start(&data_dir, port, "before-crash", &before);
    run_cli(
        &data_dir,
        &[
            "models",
            "default",
            "--provider",
            "scripted",
            "--model",
            "scripted",
        ],
    );
    daemon.wait_for_agent_catalog();
    let chat = daemon.chat("crash-1");
    let (session_id, _) = chat.ready();
    chat.command(ChatSessionCommand::Send {
        session_id: session_id.clone(),
        text: "Start the long job.".to_string(),
        attachments: Vec::new(),
    });
    wait_for_requests(&before, 1);
    let before_kill = chat.drain(Duration::from_millis(500));
    eprintln!("events before kill: {before_kill:#?}");
    daemon.kill();

    let daemon = Daemon::start(&data_dir, port, "after-crash", &after);
    daemon.wait_for_agent_catalog();
    let history = daemon.history(&session_id);
    eprintln!("history after restart: {history:#?}");
    let chat = daemon.chat("crash-2");
    chat.ready();
    chat.command(ChatSessionCommand::Resume {
        session_id: session_id.clone(),
        history,
    });
    eprintln!(
        "events after resume: {:#?}",
        chat.drain(Duration::from_secs(3))
    );
    eprintln!(
        "requests after resume (before any message): {}",
        ScriptedReport::load(&after.with_extension("report.json"))
            .map(|report| report.requests)
            .unwrap_or_default()
    );
    let events = chat.send(&session_id, "Are you still there?");
    eprintln!("events for next message: {events:#?}");
    eprintln!(
        "model requests after restart: {}",
        std::fs::read_to_string(after.with_extension("requests.jsonl")).unwrap_or_default()
    );
}

#[test]
#[ignore = "exploratory crash probe"]
fn probe_crash_during_inline_tool() {
    probe_crash_during_tool(false);
}

#[test]
#[ignore = "exploratory crash probe"]
fn probe_crash_during_detached_tool() {
    probe_crash_during_tool(true);
}

/// Exploratory: the daemon dies while `remote_shell` runs a slow command,
/// inline or detached.
fn probe_crash_during_tool(detached: bool) {
    let name = if detached { "detached" } else { "inline" };
    let root = sinomo_testdir::TestDir::new(&format!("sinomo-scripted-crash-{name}")).keep();
    eprintln!("root: {}", root.display());
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data layout");
    let call = serde_json::json!({
        "response_mode": "tool_calls",
        "text": "",
        "tool_calls": [{
            "tool": "run_command",
            "input": { "command": "ping -n 120 127.0.0.1", "timeout_ms": 300000 },
            "async": detached,
            "summary": "slow job",
        }],
        "background_handoff": null,
        "next_state": "waiting",
    });
    let before = root.join("scripts").join("before-crash.json");
    ModelScript::new()
        .reply(ScriptedReply::new().text(call.to_string()))
        .reply(answer("Started the job; I'll report back."))
        .reply(answer("spare"))
        .save(&before)
        .unwrap();
    let after = root.join("scripts").join("after-crash.json");
    ModelScript::new()
        .reply(answer("Back again."))
        .reply(answer("spare"))
        .reply(answer("spare"))
        .save(&after)
        .unwrap();

    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    run_cli(
        &data_dir,
        &["node", "permissions", "allow", "remote-management"],
    );
    let mut daemon = Daemon::start(&data_dir, port, "before-crash", &before);
    run_cli(
        &data_dir,
        &[
            "models",
            "default",
            "--provider",
            "scripted",
            "--model",
            "scripted",
        ],
    );
    daemon.wait_for_agent_catalog();
    let chat = daemon.chat("crash-1");
    let (first_session, _) = chat.ready();
    chat.command(ChatSessionCommand::SetAgent {
        session_id: first_session,
        key: "coding-agent".to_string(),
        history: Vec::new(),
    });
    let session_id = sinomo_runtime::events::SessionId::generate().to_string();
    chat.command(ChatSessionCommand::NewSession {
        session_id: session_id.clone(),
    });
    eprintln!("agent events: {:#?}", chat.drain(Duration::from_secs(3)));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    chat.command(ChatSessionCommand::SetWorkspace {
        session_id: session_id.clone(),
        workspace_root: Some(workspace),
    });
    eprintln!(
        "workspace events: {:#?}",
        chat.drain(Duration::from_secs(2))
    );
    chat.command(ChatSessionCommand::Send {
        session_id: session_id.clone(),
        text: "Run the slow job.".to_string(),
        attachments: Vec::new(),
    });
    let before_kill = chat.drain(Duration::from_secs(4));
    eprintln!("events before kill: {before_kill:#?}");
    eprintln!("pings before kill: {}", ping_count());
    daemon.kill();
    std::thread::sleep(Duration::from_secs(1));
    eprintln!("pings after kill: {}", ping_count());

    let daemon = Daemon::start(&data_dir, port, "after-crash", &after);
    daemon.wait_for_agent_catalog();
    let history = daemon.history(&session_id);
    eprintln!("history after restart: {history:#?}");
    let chat = daemon.chat("crash-2");
    chat.ready();
    chat.command(ChatSessionCommand::Resume {
        session_id: session_id.clone(),
        history,
    });
    eprintln!(
        "events after resume: {:#?}",
        chat.drain(Duration::from_secs(5))
    );
    eprintln!(
        "requests after resume (before any message): {}",
        ScriptedReport::load(&after.with_extension("report.json"))
            .map(|report| report.requests)
            .unwrap_or_default()
    );
    let events = chat.send(&session_id, "Did the job finish?");
    eprintln!("events for next message: {events:#?}");
    eprintln!("pings at end: {}", ping_count());
}

fn ping_count() -> usize {
    Command::new("tasklist")
        .args(["/FI", "IMAGENAME eq PING.EXE"])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter(|line| line.to_ascii_uppercase().starts_with("PING.EXE"))
                .count()
        })
        .unwrap_or_default()
}

fn wait_for_requests(script: &Path, count: usize) {
    let deadline = Instant::now() + TURN_TIMEOUT;
    loop {
        let requests = ScriptedReport::load(&script.with_extension("report.json"))
            .map(|report| report.requests)
            .unwrap_or_default();
        if requests >= count {
            return;
        }
        assert!(Instant::now() < deadline, "the model was not asked");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn assistant_text(events: &[ChatEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            ChatEvent::AssistantTextDelta { text, .. }
            | ChatEvent::Token { text, .. }
            | ChatEvent::AgentToken { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

struct Daemon {
    child: Child,
    address: String,
}

impl Daemon {
    fn start(data_dir: &SinomoDataDir, port: u16, launch_id: &str, script: &Path) -> Self {
        let launch_id = format!("{launch_id}-{}-{port}", std::process::id());
        let mut child = daemon_command(data_dir, port, &launch_id)
            .env(SCRIPTED_PROVIDER_SCRIPT_ENV, script)
            .spawn()
            .expect("spawn daemon");
        let address = wait_for_owned_status(&mut child, data_dir, &launch_id).address;
        Self { child, address }
    }

    /// Waits until chat can start: cluster services (SIFS for the agent
    /// catalog) are published after the daemon reports readiness.
    fn wait_for_agent_catalog(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let sifs = sinomo_client::SifsBrowser::open_daemon(self.address.clone())
                .and_then(|sifs| sifs.list("/").map(|_| ()));
            match sifs {
                Ok(()) => break,
                Err(error) => assert!(Instant::now() < deadline, "SIFS was not ready: {error}"),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let agents = sinomo_client::AgentManagementClient::at_address(self.address.clone());
        loop {
            let snapshot = agents.snapshot(None).expect("query agent catalog");
            if snapshot.error.is_none() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "agent catalog was not ready: {:?}",
                snapshot.error
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn chat(&self, client_id: &str) -> Chat {
        let client = sinomo_client::ChatDaemonClient::at_address(
            self.address.clone(),
            format!("{client_id}-{}", std::process::id()),
        )
        .with_timeout(Duration::from_secs(10));
        let opened = client.open().expect("open daemon-hosted chat");
        Chat {
            client,
            next_sequence: std::cell::Cell::new(opened.next_event_sequence),
        }
    }

    fn history(&self, session_id: &str) -> Vec<ChatHistoryMessage> {
        let page = sinomo_client::ChatHistoryClient::at_address(self.address.clone())
            .load(ChatHistoryLoadRequest {
                session_id: session_id.to_string(),
                mode: ChatHistoryLoadMode::Initial,
                sequence: None,
                limit: 100,
                include_overview: false,
                known_message_ids: Vec::new(),
            })
            .expect("load chat history")
            .page;
        page.messages
            .into_iter()
            .map(|message| ChatHistoryMessage {
                // The roles the app sends when it resumes a chat.
                role: match message.author {
                    ChatHistoryAuthor::User { .. } => "user",
                    ChatHistoryAuthor::Agent { .. } => "agent",
                    ChatHistoryAuthor::System => "system",
                }
                .to_string(),
                text: message.content,
            })
            .collect()
    }

    /// Kills the daemon process without letting it shut down.
    fn kill(mut self) {
        self.child.kill().expect("kill daemon");
        self.child.wait().expect("reap daemon");
    }

    /// Ends the daemon's lifetime lease and waits for it to exit.
    fn stop(&mut self) {
        drop(self.child.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(20);
        while self.child.try_wait().expect("inspect daemon").is_none() {
            assert!(Instant::now() < deadline, "daemon did not stop");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            self.stop();
        }
    }
}

struct Chat {
    client: sinomo_client::ChatDaemonClient,
    next_sequence: std::cell::Cell<u64>,
}

impl Chat {
    fn command(&self, command: ChatSessionCommand) {
        self.client.command(command).expect("chat command");
    }

    /// Waits for the chat's session to be ready.
    fn ready(&self) -> (String, sinomo_client::chat_contract::RuntimeInfo) {
        let mut ready = None;
        self.poll_until(|event| {
            if let ChatEvent::Ready { session_id, info } = event {
                ready = Some((session_id.clone(), info.clone()));
                true
            } else {
                false
            }
        });
        ready.expect("chat ready")
    }

    /// Sends `text` and returns the events up to the end of its turn.
    fn send(&self, session_id: &str, text: &str) -> Vec<ChatEvent> {
        self.command(ChatSessionCommand::Send {
            session_id: session_id.to_string(),
            text: text.to_string(),
            attachments: Vec::new(),
        });
        self.poll_until(
            |event| matches!(event, ChatEvent::TurnDone { session_id: done } if done == session_id),
        )
    }

    /// The events published within `period`.
    fn drain(&self, period: Duration) -> Vec<ChatEvent> {
        let deadline = Instant::now() + period;
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let response = self
                .client
                .events(self.next_sequence.get(), 64, Duration::from_millis(200))
                .expect("poll chat events");
            self.next_sequence.set(response.next_sequence);
            seen.extend(response.events.into_iter().map(|record| record.event));
        }
        seen
    }

    fn poll_until(&self, mut done: impl FnMut(&ChatEvent) -> bool) -> Vec<ChatEvent> {
        let deadline = Instant::now() + TURN_TIMEOUT;
        let mut seen = Vec::new();
        loop {
            let response = self
                .client
                .events(self.next_sequence.get(), 64, Duration::from_millis(500))
                .expect("poll chat events");
            self.next_sequence.set(response.next_sequence);
            for record in response.events {
                if let ChatEvent::Error { error, .. } = &record.event {
                    panic!("chat error: {error}\nevents so far: {seen:?}");
                }
                let finished = done(&record.event);
                seen.push(record.event);
                if finished {
                    return seen;
                }
            }
            assert!(
                Instant::now() < deadline,
                "chat did not finish; events: {seen:?}"
            );
        }
    }
}

fn initialize_cluster(data_dir: &SinomoDataDir, port: u16) {
    let launch_id = format!("bootstrap-{}-{port}", std::process::id());
    let mut bootstrap = daemon_command(data_dir, port, &launch_id)
        .spawn()
        .expect("spawn bootstrap daemon");
    let status = wait_for_owned_status(&mut bootstrap, data_dir, &launch_id);
    DaemonApiClient::at_address(status.address)
        .cluster_init(ClusterControlInitRequest {
            cluster_name: "Scripted chat test".into(),
            node_display_name: Some("Scripted chat test".into()),
            node_role: None,
        })
        .expect("initialize cluster");
    let deadline = Instant::now() + Duration::from_secs(20);
    while bootstrap.try_wait().expect("inspect bootstrap").is_none() {
        assert!(Instant::now() < deadline, "bootstrap daemon did not exit");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn daemon_command(data_dir: &SinomoDataDir, port: u16, launch_id: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinomo"));
    command
        .args(["daemon", "run", "--app-owned", "--host", "127.0.0.1"])
        .arg("--port")
        .arg(port.to_string())
        .env("SINOMO_DATA_DIR", data_dir.root())
        .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, launch_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(daemon_log(data_dir, launch_id));
    command
}

/// The daemon's stderr, kept in the test's data dir for diagnosis.
fn daemon_log(data_dir: &SinomoDataDir, launch_id: &str) -> Stdio {
    std::fs::File::create(data_dir.root().join(format!("daemon-{launch_id}.log")))
        .map(Stdio::from)
        .unwrap_or_else(|_| Stdio::null())
}

fn run_cli(data_dir: &SinomoDataDir, args: &[&str]) {
    let output = Command::new(env!("CARGO_BIN_EXE_sinomo"))
        .args(args)
        .env("SINOMO_DATA_DIR", data_dir.root())
        .output()
        .expect("run sinomo");
    assert!(
        output.status.success(),
        "sinomo {args:?} failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
    data_dir: &SinomoDataDir,
    launch_id: &str,
) -> sinomo_runtime::api::DaemonStatus {
    let paths = DaemonPaths::new(data_dir);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(exit) = child.try_wait().expect("inspect daemon") {
            panic!("daemon exited before readiness: {exit}");
        }
        if let DaemonProbeStatus::Running(status) = probe_daemon_status(&paths, PROBE_TIMEOUT)
            && status.pid == child.id()
            && status.app_launch_id.as_deref() == Some(launch_id)
        {
            return status;
        }
        assert!(Instant::now() < deadline, "daemon readiness timed out");
        std::thread::sleep(Duration::from_millis(100));
    }
}
