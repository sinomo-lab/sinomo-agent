use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

type ApiError = sinomo_client::api::DaemonApiError;

#[derive(Clone)]
struct Connection {
    generation: String,
    events: Arc<Mutex<Vec<wire::SequencedChatEvent>>>,
    changed: Arc<tokio::sync::Notify>,
    polls: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
    fail_command: Arc<AtomicBool>,
    commands: Sender<Option<String>>,
    attachments: Sender<Option<String>>,
    reads: Sender<u64>,
}

impl Connection {
    fn publish(&self, sequence: u64) {
        self.events.lock().unwrap().push(wire::SequencedChatEvent {
            sequence,
            event: ChatEvent::Notice {
                session_id: "session".into(),
                notice: format!("event {sequence}"),
            },
        });
        self.changed.notify_waiters();
    }
}

struct PendingRead(Arc<AtomicUsize>, bool);

impl Drop for PendingRead {
    fn drop(&mut self) {
        if !self.1 {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl ChatConnection for Connection {
    async fn attach(
        &self,
        execution: Option<String>,
    ) -> Result<wire::ChatSessionOpenResponse, ApiError> {
        let _ = self.attachments.send(execution);
        Ok(wire::ChatSessionOpenResponse {
            client_id: "client".into(),
            next_event_sequence: 1,
            attached: true,
            generation: self.generation.clone(),
            runtime_ready: true,
            handoff_protocol: wire::CHAT_HANDOFF_PROTOCOL_VERSION,
        })
    }

    async fn command_with_id(
        &self,
        _: wire::ChatSessionCommand,
        id: Option<String>,
    ) -> Result<wire::ChatSessionCommandResponse, ApiError> {
        let _ = self.commands.send(id);
        if self.fail_command.swap(false, Ordering::Relaxed) {
            return Err(ApiError::Network {
                address: "test".into(),
                detail: "reply lost after delivery".into(),
            });
        }
        Ok(wire::ChatSessionCommandResponse {
            accepted: true,
            reattach_required: false,
        })
    }

    async fn events(
        &self,
        cursor: u64,
        limit: usize,
        wait: Duration,
    ) -> Result<wire::ChatSessionEventsResponse, ApiError> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        let mut pending = PendingRead(self.cancelled.clone(), false);
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let _ = self.reads.send(cursor);
        let ready = self
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.sequence > cursor);
        if !ready {
            let _ = tokio::time::timeout(wait, changed).await;
        }
        let events: Vec<_> = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.sequence > cursor)
            .take(limit)
            .cloned()
            .collect();
        pending.1 = true;
        Ok(wire::ChatSessionEventsResponse {
            next_sequence: events.last().map(|e| e.sequence).unwrap_or(cursor),
            events,
            first_available_sequence: 1,
            generation: self.generation.clone(),
            execution_available: true,
        })
    }

    async fn close(&self) -> Result<wire::ChatSessionCloseResponse, ApiError> {
        Ok(wire::ChatSessionCloseResponse { closed: true })
    }
}

struct Fixture {
    connection: Connection,
    commands: Receiver<Option<String>>,
    attachments: Receiver<Option<String>>,
    reads: Receiver<u64>,
}

impl Fixture {
    fn new() -> Self {
        let (commands_tx, commands) = channel();
        let (attachments_tx, attachments) = channel();
        let (reads_tx, reads) = channel();
        Self {
            connection: Connection {
                generation: "generation".into(),
                events: Arc::new(Mutex::new(Vec::new())),
                changed: Arc::new(tokio::sync::Notify::new()),
                polls: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                fail_command: Arc::new(AtomicBool::new(false)),
                commands: commands_tx,
                attachments: attachments_tx,
                reads: reads_tx,
            },
            commands,
            attachments,
            reads,
        }
    }

    fn start(&self, saved: ChatConnectionSnapshot) -> ChatHandle {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        );
        let (tx, command_rx) = channel();
        let (event_tx, rx) = channel();
        let ready = Arc::new(tokio::sync::Notify::new());
        let checkpoint = Arc::new(Mutex::new(saved));
        let connected = Arc::new(AtomicBool::new(false));
        let worker = {
            let runtime = runtime.clone();
            let connection = self.connection.clone();
            let ready = ready.clone();
            let checkpoint = checkpoint.clone();
            let connected = connected.clone();
            thread::spawn(move || {
                runtime.block_on(run_connected_worker(
                    connection, command_rx, event_tx, None, checkpoint, connected, ready,
                ))
            })
        };
        ChatHandle {
            tx: ChatCommandSender { inner: tx, ready },
            rx,
            checkpoint,
            connected,
            runtime,
            worker: Some(worker),
        }
    }

    fn read(&self) -> u64 {
        self.reads
            .recv_timeout(Duration::from_secs(2))
            .expect("event read starts")
    }
}

#[test]
fn idle_read_does_not_delay_commands_or_restart() {
    let fixture = Fixture::new();
    let handle = fixture.start(ChatConnectionSnapshot::default());
    assert_eq!(fixture.read(), 0);
    thread::sleep(Duration::from_millis(350));
    assert_eq!(fixture.connection.polls.load(Ordering::Relaxed), 1);
    handle.send_command(ChatCommand::ControlState {
        session_id: "session".into(),
    });
    fixture
        .commands
        .recv_timeout(Duration::from_millis(300))
        .expect("command wakes idle actor");
    assert_eq!(fixture.connection.polls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.connection.cancelled.load(Ordering::Relaxed), 0);
    handle
        .prepare_restart()
        .recv_timeout(Duration::from_millis(300))
        .expect("restart wakes idle actor")
        .unwrap();
}

#[test]
fn replay_checkpoint_advances_only_after_ui_consumes_events() {
    let fixture = Fixture::new();
    let handle = fixture.start(ChatConnectionSnapshot::default());
    fixture.read();
    handle.drain();
    fixture.connection.publish(1);
    assert_eq!(fixture.read(), 1);
    assert_eq!(handle.connection_snapshot().after_sequence, 0);
    assert_eq!(handle.drain().len(), 1);
    assert_eq!(handle.connection_snapshot().after_sequence, 1);
    fixture.connection.publish(2);
    assert_eq!(fixture.read(), 2);
    handle
        .prepare_restart()
        .recv_timeout(Duration::from_millis(300))
        .unwrap()
        .unwrap();
    let saved = handle.connection_snapshot();
    let execution_id = saved.execution_id.clone();
    drop(handle);
    let restored = fixture.start(saved);
    assert_eq!(fixture.read(), 1);
    assert_eq!(fixture.read(), 2);
    let notices: Vec<_> = restored
        .drain()
        .into_iter()
        .filter_map(|event| {
            if let ChatEvent::Notice { notice, .. } = event {
                Some(notice)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(notices, ["event 2"]);
    assert_eq!(restored.connection_snapshot().execution_id, execution_id);
}

#[test]
fn uncertain_command_retry_preserves_mutation_and_execution_identity() {
    let fixture = Fixture::new();
    fixture
        .connection
        .fail_command
        .store(true, Ordering::Relaxed);
    let saved = ChatConnectionSnapshot::default();
    let execution_id = saved.execution_id.clone();
    let handle = fixture.start(saved);
    fixture.read();
    let start = Instant::now();
    handle.send_command(ChatCommand::Send {
        session_id: "session".into(),
        text: "test".into(),
        attachments: vec![],
    });
    let first = fixture
        .commands
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    let retry = fixture
        .commands
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(first, retry);
    assert!(
        start.elapsed() >= Duration::from_millis(200),
        "uncertain reply caused a hot retry"
    );
    for _ in 0..2 {
        assert_eq!(
            fixture
                .attachments
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Some(execution_id.clone())
        );
    }
}

#[test]
fn new_generation_resets_replay_cursor_and_shutdown_cancels_read() {
    let fixture = Fixture::new();
    fixture.connection.publish(1);
    let handle = fixture.start(ChatConnectionSnapshot {
        generation: "old-generation".into(),
        after_sequence: 42,
        ..ChatConnectionSnapshot::default()
    });
    assert_eq!(fixture.read(), 0);
    assert_eq!(fixture.read(), 1);
    assert!(
        handle
            .drain()
            .iter()
            .any(|event| matches!(event, ChatEvent::Reattached { replay_gap: true }))
    );
    assert_eq!(handle.connection_snapshot().after_sequence, 1);
    let start = Instant::now();
    drop(handle);
    assert!(
        start.elapsed() < Duration::from_millis(300),
        "shutdown waited for event deadline"
    );
    assert_eq!(fixture.connection.cancelled.load(Ordering::Relaxed), 1);
}
