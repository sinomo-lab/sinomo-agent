use super::tests::{fake_host, fake_view};
use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Ownership {
    claims: AtomicUsize,
    renewals: AtomicUsize,
    fail_claim: AtomicBool,
}
impl cluster::ChatOwnership for Ownership {
    fn claim(&self, _: &str) -> Result<ChatOwner, String> {
        self.claims.fetch_add(1, Ordering::Relaxed);
        if self.fail_claim.load(Ordering::Relaxed) {
            Err("ownership confirmation unavailable".into())
        } else {
            Ok(ChatOwner::Local)
        }
    }
    fn renew(&self, _: &str) -> Result<bool, String> {
        self.renewals.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
    fn release(&self, _: &str) {
        panic!("dormancy retains the expired placement hint");
    }
    fn current_owner(&self, _: &str) -> Result<Option<ChatOwner>, String> {
        Ok(None)
    }
    fn route_hint(&self, _: &str) -> Result<Option<ChatOwner>, String> {
        Ok(Some(ChatOwner::Local))
    }
}

struct Fixture {
    root: sinomo_testdir::TestDir,
    runtime: tokio::runtime::Runtime,
    service: HostedChatService,
    owner: Arc<Ownership>,
    receivers: Arc<Mutex<Vec<Receiver<HostCommand>>>>,
}
impl Fixture {
    fn new() -> Self {
        let root = sinomo_testdir::TestDir::new("chat-dormancy");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let owner = Arc::new(Ownership::default());
        let receivers: Arc<Mutex<Vec<Receiver<HostCommand>>>> = Arc::default();
        let mut service = HostedChatService::new("127.0.0.1:1", AgentRuntimeControlRegistry::new());
        let _ = service.environment.set(HostEnvironment {
            data_dir: sinomo_runtime::local::SinomoDataDir::new(&root),
            runtime: runtime.handle().clone(),
            cluster: ChatCluster {
                ownership: owner.clone(),
                ..ChatCluster::standalone()
            },
        });
        let collected = receivers.clone();
        service.host_factory = Some(Arc::new(move |_, fanout| {
            let (mut host, receiver) = fake_host(Duration::ZERO);
            host.fanout = fanout;
            collected.lock().unwrap().push(receiver);
            host
        }));
        Self {
            root,
            runtime,
            service,
            owner,
            receivers,
        }
    }
    fn window(&self, view: &str, client: &str, chat: &str) {
        self.service
            .views
            .lock()
            .unwrap()
            .insert(view.into(), fake_view(&self.root, &self.runtime, view));
        self.service
            .clients
            .lock()
            .unwrap()
            .insert(client.into(), view.into());
        self.service.attach_view(view, chat, &[]).unwrap();
    }
    fn activate(&self, chat: &str) -> (Arc<AtomicBool>, Arc<Mutex<()>>) {
        let hosts = self.service.ensure_route(chat, &[]).unwrap();
        let Some(ChatRoute::Local(host)) = hosts.get(chat) else {
            panic!("local host expected")
        };
        (host.alive.clone(), host.lifecycle.clone())
    }
    fn age(&self, chat: &str) {
        let hosts = self.service.hosts.lock().unwrap();
        let Some(ChatRoute::Local(host)) = hosts.get(chat) else {
            panic!("local host expected")
        };
        host.activity.lock().last_active =
            HostTime::ago(CHAT_HOST_WARM_IDLE + Duration::from_secs(1));
    }
    fn retire(&self, chat: &str) {
        let (alive, lifecycle) = self.activate(chat);
        self.age(chat);
        let _gate = lifecycle.lock().unwrap();
        drop(
            self.service
                .begin_dormancy(chat, &alive)
                .expect("quiescent host can sleep"),
        );
    }
}
fn peer(chat: &str, id: &str) -> wire::ChatPeerCommandRequest {
    wire::ChatPeerCommandRequest {
        chat_id: chat.into(),
        origin: "remote-view".into(),
        request_id: id.into(),
        command: wire::ChatSessionCommand::Send {
            session_id: chat.into(),
            text: "test".into(),
            attachments: vec![],
        },
    }
}

#[test]
fn retained_views_and_event_reads_do_not_claim_or_renew_execution() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    fixture.window("view-a", "client-a", &chat);
    fixture.window("view-b", "client-b", &chat);
    assert_eq!(fixture.owner.claims.load(Ordering::Relaxed), 0);
    let (alive, lifecycle) = fixture.activate(&chat);
    let events = fixture.service.views.lock().unwrap()["view-a"]
        .events
        .clone();
    let generation = events.state.0.lock().unwrap().generation.clone();
    fixture.service.hosts.lock().unwrap()[&chat]
        .fanout()
        .push(wire::ChatEvent::Notice {
            session_id: chat.clone(),
            notice: "before sleep".into(),
        });
    fixture.age(&chat);
    let _gate = lifecycle.lock().unwrap();
    drop(fixture.service.begin_dormancy(&chat, &alive).unwrap());
    drop(_gate);
    fixture.service.maintain_routes().unwrap();
    fixture
        .service
        .command(wire::ChatSessionCommandRequest {
            client_id: "client-a".into(),
            request_id: None,
            command: wire::ChatSessionCommand::ControlState {
                session_id: chat.clone(),
            },
        })
        .unwrap();
    let response = fixture
        .service
        .peer_events(wire::ChatPeerEventsRequest {
            chat_id: chat.clone(),
            after_sequence: 0,
            max_events: 64,
            wait_ms: 0,
        })
        .unwrap();
    assert!(!response.not_owner);
    assert_eq!(fixture.owner.renewals.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.owner.claims.load(Ordering::Relaxed), 1);
    let views = fixture.service.views.lock().unwrap();
    assert_eq!(views["view-a"].chat_id().as_deref(), Some(chat.as_str()));
    assert_eq!(views["view-b"].chat_id().as_deref(), Some(chat.as_str()));
    assert_eq!(
        views["view-a"].events.state.0.lock().unwrap().generation,
        generation
    );
    assert!(views["view-a"].events.state.0.lock().unwrap().events.iter().any(|e| matches!(&e.event, wire::ChatEvent::ControlState(state) if !state.runtime_attached)));
    assert!(matches!(
        fixture.service.hosts.lock().unwrap().get(&chat),
        Some(ChatRoute::Dormant(_))
    ));
}

#[test]
fn peer_retry_receipts_and_stream_identity_survive_dormancy() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    fixture.window("view", "client", &chat);
    fixture
        .service
        .peer_command(peer(&chat, "original"))
        .unwrap();
    drop(fixture.receivers.lock().unwrap()[0].try_recv().unwrap());
    let fanout = fixture.service.hosts.lock().unwrap()[&chat]
        .fanout()
        .clone();
    let generation = fanout.lock().generation.clone();
    fixture.retire(&chat);
    assert!(
        fixture
            .service
            .peer_command(peer(&chat, "original"))
            .unwrap()
            .accepted
    );
    assert_eq!(
        fixture.owner.claims.load(Ordering::Relaxed),
        1,
        "a known retry must not wake a host"
    );
    assert!(
        fixture
            .service
            .peer_command(peer(&chat, "next"))
            .unwrap()
            .accepted
    );
    assert_eq!(fixture.owner.claims.load(Ordering::Relaxed), 2);
    assert!(
        fixture
            .service
            .peer_command(peer(&chat, "original"))
            .unwrap()
            .accepted
    );
    let receivers = fixture.receivers.lock().unwrap();
    assert!(matches!(
        receivers[1].try_recv().unwrap(),
        HostCommand::Run { .. }
    ));
    assert!(
        receivers[1].try_recv().is_err(),
        "old success executed again"
    );
    assert_eq!(
        fixture.service.hosts.lock().unwrap()[&chat]
            .fanout()
            .lock()
            .generation,
        generation
    );
}

#[test]
fn queued_commands_remain_pinned_across_a_long_mobile_suspension() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    let (alive, lifecycle) = fixture.activate(&chat);
    fixture.service.peer_command(peer(&chat, "queued")).unwrap();
    fixture.age(&chat);
    let _gate = lifecycle.lock().unwrap();
    assert!(fixture.service.begin_dormancy(&chat, &alive).is_none());
    let command = fixture.receivers.lock().unwrap()[0].try_recv().unwrap();
    fixture.age(&chat);
    assert!(
        fixture.service.begin_dormancy(&chat, &alive).is_none(),
        "receiving does not finish the admitted operation"
    );
    drop(command);
    fixture.age(&chat);
    assert!(fixture.service.begin_dormancy(&chat, &alive).is_some());
}

#[test]
fn activation_waits_for_the_old_runtime_teardown() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    let (alive, lifecycle) = fixture.activate(&chat);
    fixture.age(&chat);
    let gate = lifecycle.lock().unwrap();
    let retired = fixture.service.begin_dormancy(&chat, &alive).unwrap();
    let service = fixture.service.clone();
    let request = peer(&chat, "after-sleep");
    let (tx, rx) = channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(service.peer_command(request));
    });
    let premature = rx.recv_timeout(Duration::from_millis(50));
    let claims = fixture.owner.claims.load(Ordering::Relaxed);
    drop(retired);
    drop(gate);
    let result = match premature {
        Ok(result) => result,
        Err(_) => rx.recv_timeout(Duration::from_secs(2)).unwrap(),
    };
    worker.join().unwrap();
    assert_eq!(
        claims, 1,
        "new ownership was acquired before teardown finished"
    );
    assert!(result.unwrap().accepted);
    assert_eq!(fixture.owner.claims.load(Ordering::Relaxed), 2);
}

#[test]
fn failed_activation_is_not_acknowledged_or_replayed_with_a_new_identity() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    fixture.window("view", "client", &chat);
    fixture.owner.fail_claim.store(true, Ordering::Relaxed);
    let request = wire::ChatSessionCommandRequest {
        client_id: "client".into(),
        request_id: Some("stable".into()),
        command: peer(&chat, "stable").command,
    };
    assert!(
        fixture
            .service
            .command(request.clone())
            .unwrap_err()
            .contains("ownership confirmation")
    );
    fixture.owner.fail_claim.store(false, Ordering::Relaxed);
    assert!(
        fixture
            .service
            .command(request)
            .unwrap_err()
            .contains("ownership confirmation")
    );
    assert!(fixture.receivers.lock().unwrap().is_empty());
    let journal = fixture.service.views.lock().unwrap()["view"]
        .journal
        .clone();
    assert!(
        journal.recovery().unwrap().1.is_empty(),
        "failed non-admission cannot execute on restart"
    );
}

#[test]
fn active_participants_controls_and_runtime_work_block_dormancy() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    let (alive, lifecycle) = fixture.activate(&chat);
    fixture.age(&chat);
    let activity = {
        let hosts = fixture.service.hosts.lock().unwrap();
        let ChatRoute::Local(host) = &hosts[&chat] else {
            unreachable!()
        };
        host.activity.clone()
    };
    let _gate = lifecycle.lock().unwrap();
    activity.lock().participant_work = true;
    assert!(fixture.service.begin_dormancy(&chat, &alive).is_none());
    activity.lock().participant_work = false;
    activity.lock().busy = true;
    assert!(fixture.service.begin_dormancy(&chat, &alive).is_none());
    activity.lock().busy = false;
    let admission = activity.admit();
    fixture.age(&chat);
    assert!(fixture.service.begin_dormancy(&chat, &alive).is_none());
    drop(admission);
    fixture.age(&chat);
    assert!(fixture.service.begin_dormancy(&chat, &alive).is_some());
}

#[test]
fn owned_admission_waits_leave_the_bounded_blocking_pool_available() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let service = HostedChatService::new("unused", AgentRuntimeControlRegistry::new());
    runtime.block_on(async {
        let mut requests = Vec::new();
        for _ in 0..16 {
            let service = service.clone();
            let executor = tokio::runtime::Handle::current();
            requests.push(tokio::spawn(async move {
                service
                    .owned_request(move || {
                        executor
                            .block_on(tokio::task::spawn_blocking(|| 42))
                            .map_err(|e| e.to_string())
                    })
                    .await
            }));
        }
        for request in requests {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), request)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                42
            );
        }
        service
            .drain(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    });
}

#[test]
fn remote_event_waits_are_asynchronous_and_keep_publication_wakeups() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let fanout = ChatFanout::default();
        fanout.push(wire::ChatEvent::Notice {
            session_id: "chat".into(),
            notice: "first".into(),
        });
        let mut readers = Vec::new();
        for _ in 0..16 {
            let fanout = fanout.clone();
            readers.push(tokio::spawn(async move {
                fanout
                    .poll_peer_async(&wire::ChatPeerEventsRequest {
                        chat_id: "chat".into(),
                        after_sequence: 1,
                        max_events: 8,
                        wait_ms: 5000,
                    })
                    .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(readers.iter().all(|reader| !reader.is_finished()));
        tokio::time::timeout(
            Duration::from_millis(300),
            tokio::task::spawn_blocking(|| ()),
        )
        .await
        .unwrap()
        .unwrap();
        fanout.push(wire::ChatEvent::Notice {
            session_id: "chat".into(),
            notice: "second".into(),
        });
        for reader in readers {
            let response = tokio::time::timeout(Duration::from_millis(300), reader)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.next_sequence, 2);
            assert_eq!(response.events.len(), 1);
        }
    });
}

#[tokio::test]
async fn an_empty_dormant_peer_stream_waits_instead_of_spinning_at_cursor_zero() {
    let fanout = ChatFanout::default();
    let start = Instant::now();
    let response = fanout
        .poll_peer_async(&wire::ChatPeerEventsRequest {
            chat_id: "chat".into(),
            after_sequence: 0,
            max_events: 8,
            wait_ms: 50,
        })
        .await;
    assert!(start.elapsed() >= Duration::from_millis(40));
    assert!(response.events.is_empty());
    assert!(
        fanout.has_views(),
        "remote observation retains presentation, not execution"
    );
}

#[test]
fn an_expired_warm_confirmation_is_refreshed_before_new_execution() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    fixture.activate(&chat);
    {
        let hosts = fixture.service.hosts.lock().unwrap();
        let ChatRoute::Local(host) = &hosts[&chat] else {
            unreachable!()
        };
        *host.renewed_at.lock().unwrap() = HostTime::ago(CHAT_OWNER_TTL);
    }
    assert!(
        fixture
            .service
            .peer_command(peer(&chat, "after-suspend"))
            .unwrap()
            .accepted
    );
    assert_eq!(fixture.owner.renewals.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.owner.claims.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_local_enqueue_is_terminal_and_cannot_reappear_during_recovery() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    fixture.window("view", "client", &chat);
    fixture.activate(&chat);
    fixture.receivers.lock().unwrap().clear();
    let request = wire::ChatSessionCommandRequest {
        client_id: "client".into(),
        request_id: Some("disconnected".into()),
        command: peer(&chat, "unused").command,
    };
    assert_eq!(
        fixture.service.command(request.clone()).unwrap_err(),
        "chat host stopped"
    );
    assert_eq!(
        fixture.service.command(request).unwrap_err(),
        "chat host stopped"
    );
    let journal = fixture.service.views.lock().unwrap()["view"]
        .journal
        .clone();
    assert!(journal.recovery().unwrap().1.is_empty());
}

#[test]
fn cancelling_a_fresh_warm_host_does_not_wait_for_its_lifecycle_gate() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    let (_, lifecycle) = fixture.activate(&chat);
    let calls = Arc::new(Mutex::new(Vec::new()));
    fixture
        .service
        .runtime_control
        .register_session(Arc::new(super::tests::CancelRecorder(
            SessionId::from_str(&chat).unwrap(),
            calls.clone(),
            None,
        )))
        .unwrap();
    let storage = local::AgentChatSifsStorage::new(
        sinomo_runtime::local::SinomoDataDir::new(&fixture.root),
        "unused",
        fixture.runtime.handle(),
    );
    let service = fixture.service.clone();
    let gate = lifecycle.lock().unwrap();
    let request = wire::ChatSessionCommand::Cancel {
        session_id: chat.clone(),
        reason: "cancel while renewing".into(),
    };
    let (tx, rx) = channel();
    let worker = std::thread::spawn(move || {
        let _ =
            tx.send(service.control("view", &storage, &ChatEventLog::new(), &chat, None, request));
    });
    let received = rx.recv_timeout(Duration::from_millis(300));
    let responsive = received.is_ok();
    drop(gate);
    let result = received.unwrap_or_else(|_| rx.recv_timeout(Duration::from_secs(2)).unwrap());
    worker.join().unwrap();
    assert!(responsive, "Cancel waited for the lifecycle gate");
    assert!(result.is_err(), "fixture reports its actual control error");
    assert_eq!(*calls.lock().unwrap(), ["cancel while renewing"]);
}

#[test]
fn capacity_eviction_cannot_remove_a_placeholder_during_cold_activation() {
    let fixture = Fixture::new();
    let chat = SessionId::generate().to_string();
    let retained = DormantChat::default();
    let lifecycle = retained.lifecycle.clone();
    fixture
        .service
        .hosts
        .lock()
        .unwrap()
        .insert(chat.clone(), ChatRoute::Dormant(retained));
    let gate = lifecycle.lock().unwrap();
    {
        let mut hosts = fixture.service.hosts.lock().unwrap();
        assert_eq!(
            fixture
                .service
                .evict_idle_hosts(&mut hosts, Some(Duration::ZERO), 1)
                .unwrap(),
            0
        );
        assert!(hosts.contains_key(&chat));
    }
    drop(gate);
    assert_eq!(
        fixture
            .service
            .evict_idle_hosts(
                &mut fixture.service.hosts.lock().unwrap(),
                Some(Duration::ZERO),
                1
            )
            .unwrap(),
        1
    );
}
