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
fn daemon_recovers_pending_views_before_any_client_attaches() {
    let root = sinomo_testdir::TestDir::new("sinomo-chat-boot-recovery");
    let data = SinomoDataDir::new(root.path());
    data.ensure_root_layout().unwrap();
    let port = available_loopback_port();
    initialize_cluster(&data, port);
    let directory = data.root().join("events/agent/chat-hosts");
    std::fs::create_dir_all(&directory).unwrap();
    let damaged = directory.join(format!("{}.json", blake3::hash(b"damaged")));
    std::fs::write(&damaged, b"{").unwrap();
    let seed = |id: &str, state: &str, command: ChatSessionCommand| {
        let path = directory.join(format!("{}.json", blake3::hash(id.as_bytes()).to_hex()));
        let hash = blake3::hash(&serde_json::to_vec(&command).unwrap())
            .to_hex()
            .to_string();
        let record = serde_json::json!({
            "schema_version": 1, "execution_id": id, "bootstrap": [],
            "entries": [{"id": id, "command": command, "command_hash": hash, "state": state}]
        });
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        path
    };
    let running = seed(
        "running-send",
        "Running",
        ChatSessionCommand::Send {
            session_id: sinomo_runtime::events::SessionId::generate().to_string(),
            text: "do not replay this uncertain send".into(),
            attachments: vec![],
        },
    );
    let pending = seed(
        "pending-model",
        "Pending",
        ChatSessionCommand::SetModel {
            provider: "scripted".into(),
            model: "scripted".into(),
            account: "default".into(),
        },
    );
    let script = root.join("model.json");
    ModelScript::new().save(&script).unwrap();
    let _daemon = Daemon::start(&data, port, "recover-journals", &script);
    let state = |path: &Path| {
        let journal: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        journal["entries"][0]["state"].clone()
    };
    let deadline = Instant::now() + TURN_TIMEOUT;
    while state(&running) != "Interrupted" || state(&pending) != "Complete" {
        assert!(
            Instant::now() < deadline,
            "unexpected journal states: {}, {}",
            state(&running),
            state(&pending)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(std::fs::read(damaged).unwrap(), b"{");
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
    let chat = daemon.resume("scripted-chat-2", &session_id);
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

#[test]
fn idle_chat_views_survive_dormancy_and_resume_with_their_context() {
    let root = sinomo_testdir::TestDir::new("sinomo-scripted-dormancy");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().unwrap();
    let script = root.join("scripts/dormancy.json");
    ModelScript::new()
        .expect(ScriptedExpectation::last_user_contains("Bluefin"))
        .reply(answer("Remembered Bluefin."))
        .expect(ScriptedExpectation::last_user_contains("Which codename"))
        .expect(ScriptedExpectation::contains("Bluefin"))
        .reply(answer("The codename is Bluefin."))
        .save(&script)
        .unwrap();
    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    let daemon = Daemon::start(&data_dir, port, "dormancy", &script);
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
    let first = daemon.chat("dormant-first");
    let (session_id, _) = first.ready();
    let second = daemon.resume("dormant-second", &session_id);
    assert!(
        assistant_text(&first.send(&session_id, "Remember the codename Bluefin."))
            .contains("Bluefin")
    );
    first.poll_until(|event| matches!(event, ChatEvent::ControlState(state) if state.session_id == session_id && !state.runtime_attached));
    let revision = owner_revision(&daemon.address, &session_id);
    first.command(ChatSessionCommand::ControlState {
        session_id: session_id.clone(),
    });
    first.poll_until(
        |event| matches!(event, ChatEvent::ControlState(state) if !state.runtime_attached),
    );
    second.command(ChatSessionCommand::ControlState {
        session_id: session_id.clone(),
    });
    second.poll_until(
        |event| matches!(event, ChatEvent::ControlState(state) if !state.runtime_attached),
    );
    std::thread::sleep(Duration::from_secs(12));
    assert_eq!(
        owner_revision(&daemon.address, &session_id),
        revision,
        "passive views renewed execution ownership"
    );
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(17));
    let mut callers = Vec::new();
    for _ in 0..16 {
        let client = second.client.clone();
        let command = ChatSessionCommand::Send {
            session_id: session_id.clone(),
            text: "Which codename did I give you?".into(),
            attachments: vec![],
        };
        let barrier = barrier.clone();
        callers.push(std::thread::spawn(move || {
            barrier.wait();
            client.command_with_id(command, Some("same-dormant-wake".into()))
        }));
    }
    barrier.wait();
    for caller in callers {
        assert!(caller.join().unwrap().unwrap().accepted);
    }
    let events = second.poll_until(
        |event| matches!(event, ChatEvent::TurnDone { session_id: id } if id == &session_id),
    );
    assert!(assistant_text(&events).contains("Bluefin"));
    first.poll_until(|event| matches!(event, ChatEvent::UserMessage { session_id: id, text } if id == &session_id && text.contains("Which codename")));
    assert!(owner_revision(&daemon.address, &session_id) > revision);
    drop(daemon);
    ScriptedReport::load(&script.with_extension("report.json"))
        .unwrap()
        .assert_finished();
}

#[test]
fn immediate_cancel_stops_an_admitted_send_before_or_during_model_start() {
    let root = sinomo_testdir::TestDir::new("sinomo-scripted-immediate-cancel");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().unwrap();
    let script = root.join("scripts/immediate-cancel.json");
    ModelScript::new()
        .reply(ScriptedReply::await_cancellation())
        .reply(ScriptedReply::await_cancellation())
        .reply(ScriptedReply::await_cancellation())
        .save(&script)
        .unwrap();
    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    let daemon = Daemon::start(&data_dir, port, "immediate-cancel", &script);
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
    let chat = daemon.chat("immediate-cancel");
    let (initial_session_id, _) = chat.ready();
    let history = sinomo_client::ChatHistoryClient::at_address(daemon.address.clone());
    assert!(
        history
            .list()
            .expect("list untouched drafts")
            .iter()
            .all(|entry| entry.id != initial_session_id),
        "opening a default draft must not publish an empty conversation"
    );
    let session_id = sinomo_runtime::events::SessionId::generate().to_string();
    chat.command(ChatSessionCommand::NewSession {
        session_id: session_id.clone(),
    });
    chat.poll_until(
        |event| matches!(event, ChatEvent::SessionReset { session_id: id } if id == &session_id),
    );
    assert!(
        history
            .list()
            .expect("list new draft")
            .iter()
            .all(|entry| entry.id != session_id),
        "switching to a new default draft must not publish an empty conversation"
    );
    for _ in 0..3 {
        chat.command(ChatSessionCommand::Send {
            session_id: session_id.clone(),
            text: "Wait until cancelled".into(),
            attachments: vec![],
        });
        let stopped = Instant::now();
        chat.command(ChatSessionCommand::Cancel {
            session_id: session_id.clone(),
            reason: "stop immediately after admission".into(),
        });
        chat.poll_until(|event| matches!(event, ChatEvent::TurnDone { session_id: done } if done == &session_id));
        assert!(
            stopped.elapsed() < Duration::from_secs(3),
            "queued cancellation did not finish promptly"
        );
    }
}

#[test]
fn a_live_model_turn_keeps_its_lease_and_can_be_cancelled() {
    let root = sinomo_testdir::TestDir::new("sinomo-scripted-owned-active-turn");
    let data_dir = SinomoDataDir::new(root.path().to_path_buf());
    data_dir.ensure_root_layout().unwrap();
    let script = root.join("scripts/active.json");
    ModelScript::new()
        .reply(ScriptedReply::await_cancellation())
        .save(&script)
        .unwrap();
    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    let daemon = Daemon::start(&data_dir, port, "active-turn", &script);
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
    let chat = daemon.chat("active-turn");
    let (session_id, _) = chat.ready();
    chat.command(ChatSessionCommand::Send {
        session_id: session_id.clone(),
        text: "Wait until cancelled.".into(),
        attachments: vec![],
    });
    wait_for_requests(&script, 1);
    let before = owner_revision(&daemon.address, &session_id);
    std::thread::sleep(Duration::from_secs(12));
    assert!(
        owner_revision(&daemon.address, &session_id) > before,
        "active work lost ownership renewal"
    );
    let cancelled = Instant::now();
    chat.command(ChatSessionCommand::Cancel {
        session_id: session_id.clone(),
        reason: "native dormancy control".into(),
    });
    chat.poll_until(
        |event| matches!(event, ChatEvent::TurnDone { session_id: id } if id == &session_id),
    );
    assert!(cancelled.elapsed() < Duration::from_secs(3));
    drop(daemon);
    ScriptedReport::load(&script.with_extension("report.json"))
        .unwrap()
        .assert_finished();
}

fn owner_revision(address: &str, chat: &str) -> u64 {
    let payload = sinomo_runtime::kv::cluster_kv_local_entry_rpc_payload(&format!(
        "/agent/chat-hosts/{chat}"
    ))
    .unwrap();
    let request = sinomo_runtime::network::ObjectRpcRequest::from_encoded_body(
        payload.route,
        payload.category,
        payload.body,
    );
    let response = DaemonApiClient::at_address(address)
        .call_request(&request)
        .unwrap();
    sinomo_runtime::kv::decode_cluster_kv_local_entry_rpc_body(&response.body)
        .unwrap()
        .value
        .unwrap()
        .modify_revision
        .0
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
    let daemon = Daemon::start(&data_dir, port, "before-crash", &before);
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
    let chat = daemon.resume("crash-2", &session_id);
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
fn a_detached_call_lost_in_a_crash_is_reported_after_restart() {
    let after_crash = ModelScript::new()
        .expect(ScriptedExpectation::contains(
            "interrupted before it finished",
        ))
        .reply(answer(
            "The slow job was interrupted by a restart, so it may not have finished.",
        ));
    let crashed = crash_during_slow_job("detached", true, after_crash);

    let daemon = crashed.restart();
    let chat = daemon.resume("crash-2", &crashed.session_id);
    // The host continues the session with the interrupted call on its own.
    let events = chat.poll_until(|event| {
        matches!(event, ChatEvent::TurnDone { session_id } if *session_id == crashed.session_id)
    });
    assert!(
        assistant_text(&events).contains("interrupted by a restart"),
        "continuation events: {events:?}"
    );
    drop(daemon);
    crashed.assert_after_crash_script_finished();
}

/// Exploratory: what the user and the model see after the daemon dies while
/// an inline tool call runs.
#[test]
#[ignore = "exploratory crash probe"]
fn probe_crash_during_inline_tool() {
    let after_crash = ModelScript::new()
        .reply(answer("Back again."))
        .reply(answer("spare"));
    let crashed = crash_during_slow_job("inline", false, after_crash);
    let daemon = crashed.restart();
    let history = daemon.history(&crashed.session_id);
    eprintln!("history after restart: {history:#?}");
    let chat = daemon.resume("crash-2", &crashed.session_id);
    eprintln!(
        "events after resume: {:#?}",
        chat.drain(Duration::from_secs(5))
    );
    let events = chat.send(&crashed.session_id, "Did the job finish?");
    eprintln!("events for next message: {events:#?}");
    eprintln!(
        "model requests after restart: {}",
        std::fs::read_to_string(crashed.after_crash.with_extension("requests.jsonl"))
            .unwrap_or_default()
    );
}

/// A chat whose daemon was killed while a coding agent's slow command ran.
struct CrashedSlowJob {
    _root: sinomo_testdir::TestDir,
    data_dir: SinomoDataDir,
    port: u16,
    session_id: String,
    after_crash: std::path::PathBuf,
}

impl CrashedSlowJob {
    /// Starts a daemon whose model replays the after-crash script.
    fn restart(&self) -> Daemon {
        let daemon = Daemon::start(&self.data_dir, self.port, "after-crash", &self.after_crash);
        daemon.wait_for_agent_catalog();
        daemon
    }

    fn assert_after_crash_script_finished(&self) {
        ScriptedReport::load(&self.after_crash.with_extension("report.json"))
            .unwrap()
            .assert_finished();
    }
}

/// Runs a slow `run_command` in a coding-agent chat, inline or detached, and
/// kills the daemon while it runs.
fn crash_during_slow_job(name: &str, detached: bool, after_crash: ModelScript) -> CrashedSlowJob {
    let root = sinomo_testdir::TestDir::new(&format!("sinomo-scripted-crash-{name}"));
    let data_dir = SinomoDataDir::new(root.join("data"));
    data_dir.ensure_root_layout().expect("data layout");
    let command = if cfg!(windows) {
        "echo started > slow-job-started && ping -n 120 127.0.0.1"
    } else {
        "printf started > slow-job-started && exec sleep 120"
    };
    let call = serde_json::json!({
        "response_mode": "tool_calls",
        "text": "",
        "tool_calls": [{
            "tool": "run_command",
            "input": { "command": command, "timeout_ms": 300000 },
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
        .save(&before)
        .unwrap();
    let after = root.join("scripts").join("after-crash.json");
    after_crash.save(&after).unwrap();

    let port = available_loopback_port();
    initialize_cluster(&data_dir, port);
    // The coding agent's commands need the node-local host permission.
    run_cli(
        &data_dir,
        &["node", "permissions", "allow", "remote-management"],
    );
    let daemon = Daemon::start(&data_dir, port, "before-crash", &before);
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
    chat.command(ChatSessionCommand::SetAgent {
        session_id: session_id.clone(),
        key: "coding-agent".to_string(),
        history: Vec::new(),
    });
    chat.drain(Duration::from_secs(1));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let started = workspace.join("slow-job-started");
    chat.command(ChatSessionCommand::SetWorkspace {
        session_id: session_id.clone(),
        workspace_root: Some(workspace),
    });
    chat.drain(Duration::from_secs(1));
    chat.command(ChatSessionCommand::Send {
        session_id: session_id.clone(),
        text: "Run the slow job.".to_string(),
        attachments: Vec::new(),
    });
    if detached {
        chat.poll_until(|event| {
            matches!(event, ChatEvent::TurnDone { session_id: done } if done == &session_id)
        });
    } else {
        wait_for_requests(&before, 1);
    }
    let deadline = Instant::now() + TURN_TIMEOUT;
    while !started.is_file() {
        assert!(
            Instant::now() < deadline,
            "the command never started in the selected workspace before the crash"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    daemon.kill();
    CrashedSlowJob {
        _root: root,
        data_dir,
        port,
        session_id,
        after_crash: after,
    }
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

    /// Explicitly selects the test model before waiting for an execution runtime.
    fn chat(&self, client_id: &str) -> Chat {
        self.open_chat(client_id, None)
    }

    fn resume(&self, client_id: &str, session_id: &str) -> Chat {
        self.open_chat(client_id, Some(session_id))
    }

    fn open_chat(&self, client_id: &str, session_id: Option<&str>) -> Chat {
        let client = sinomo_client::ChatDaemonClient::at_address(
            self.address.clone(),
            format!("{client_id}-{}", std::process::id()),
        )
        .with_timeout(Duration::from_secs(10));
        let opened = client.open().expect("open daemon-hosted chat");
        let mut chat = Chat {
            client,
            next_sequence: std::cell::Cell::new(opened.next_event_sequence),
            ready: None,
        };
        if let Some(session_id) = session_id {
            chat.command(ChatSessionCommand::Resume {
                session_id: session_id.into(),
                history: self.history(session_id),
            });
        }
        // Fresh scripted controls explicitly select a model. Resume only
        // attaches the view; component recovery owns unfinished continuations.
        if session_id.is_none() {
            chat.command(ChatSessionCommand::SetModel {
                provider: "scripted".into(),
                model: "scripted".into(),
                account: "default".into(),
            });
        } else {
            return chat;
        }
        let mut ready = None;
        chat.poll_until(|event| {
            if let ChatEvent::Ready { session_id, info } = event {
                ready = Some((session_id.clone(), info.clone()));
                true
            } else {
                false
            }
        });
        chat.ready = ready;
        chat
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
    ready: Option<(String, sinomo_client::chat_contract::RuntimeInfo)>,
}

impl Chat {
    fn command(&self, command: ChatSessionCommand) {
        self.client.command(command).expect("chat command");
    }

    /// The chat's session and runtime once it was ready.
    fn ready(&self) -> (String, sinomo_client::chat_contract::RuntimeInfo) {
        self.ready.clone().expect("the chat was ready")
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

    /// The events up to and including the first one `done` accepts.
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
        .stdout(daemon_log(data_dir, &format!("{launch_id}.out")))
        .stderr(daemon_log(data_dir, launch_id));
    command
}

/// A file in the test's data dir for the daemon's output, kept for diagnosis.
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
            let log = std::fs::read_to_string(
                data_dir.root().join(format!("daemon-{launch_id}.log")),
            )
            .unwrap_or_default();
            panic!("daemon exited before readiness: {exit}\n{log}");
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
