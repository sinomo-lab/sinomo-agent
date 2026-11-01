//! Real listener-error cleanup with one deliberately held blocking-pool task.

use super::*;
use std::net::{Shutdown, TcpStream};
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};

const CLEANUP_ERROR: &str = "native negative component drain";
// Existing component/task joins are 3s each, drain is 100ms, runtime is 1s.
// The fixture has no network-context task and grants no new product timeout.
const OBSERVATION_BOUND: Duration = Duration::from_secs(8);

struct NativeRun {
    listener: Option<TcpStream>,
    release: Option<mpsc::Sender<()>>,
    daemon: Option<thread::JoinHandle<()>>,
    worker: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl NativeRun {
    fn interrupt_listener(&mut self) -> std::io::Result<()> {
        let listener = self.listener.take().expect("one exact listener shutdown");
        listener.shutdown(Shutdown::Both)
    }

    fn cleanup(&mut self) -> Result<(), String> {
        if self.listener.is_some() {
            let _ = self.interrupt_listener();
        }
        // Release before joining even when the bounded old-code oracle failed.
        self.release.take();
        let daemon_result = self.daemon.take().map(|daemon| daemon.join());
        let worker = self.worker.lock().unwrap().take();
        let worker_result = worker.map(|worker| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(3), worker)
                        .await
                        .map_err(|error| error.to_string())?
                        .map_err(|error| error.to_string())
                })
        });
        if daemon_result.is_some_and(|result| result.is_err()) {
            return Err("native daemon thread panicked".into());
        }
        worker_result.unwrap_or(Ok(()))
    }
}

impl Drop for NativeRun {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            eprintln!("native listener-error cleanup failed: {error}");
        }
    }
}

fn actual_accept_error_with_held_worker(fail_component_drain: bool) {
    let root = sinomo_testdir::TestDir::new(if fail_component_drain {
        "daemon-accept-original-error-precedence"
    } else {
        "daemon-accept-bounded-error-cleanup"
    });
    let data_dir = sinomo_core::local::SinomoDataDir::new(&root);
    data_dir.ensure_root_layout().unwrap();
    let paths = DaemonPaths::new(&data_dir);
    let mut prepared = prepare_daemon_control_listener(DaemonRunOptions {
        host: "127.0.0.1".into(),
        port: 0,
        port_fallback: false,
    })
    .unwrap();
    // Bind the fixture options to the actual OS-selected, still-owned socket.
    prepared.options.port = prepared.listener.local_addr().unwrap().port();
    let address = format!("127.0.0.1:{}", prepared.options.port);
    // The safe OwnedFd conversions preserve this duplicate's unique ownership.
    // Only shutdown is used: Linux applies it to the same listening socket,
    // while the daemon retains its independent descriptor. No raw close occurs.
    let listener = TcpStream::from(OwnedFd::from(prepared.listener.try_clone().unwrap()));
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let (started_tx, started_rx) = mpsc::channel();
    let exited = Arc::new(AtomicBool::new(false));
    let disposed = Arc::new(AtomicBool::new(false));
    let drain_called = Arc::new(AtomicBool::new(false));
    let worker = Arc::new(Mutex::new(None));
    let factory: crate::DaemonComponentFactory = Arc::new(
        sinomo_module::StaticComponentFactory::new(
            sinomo_module::ComponentDescriptor::new("test.accept-error-worker", "1.0.0"),
            {
                let worker = Arc::clone(&worker);
                let exited = Arc::clone(&exited);
                let disposed = Arc::clone(&disposed);
                let drain_called = Arc::clone(&drain_called);
                move |context, scope| {
                    let runtime = context.runtime().expect("normal daemon runtime").clone();
                    let receiver = Arc::clone(&release_rx);
                    let worker = Arc::clone(&worker);
                    let exited = Arc::clone(&exited);
                    let started = started_tx.clone();
                    scope.on_host_ready(move || {
                        let receiver = receiver.lock().unwrap().take().expect("one native worker");
                        let exited = Arc::clone(&exited);
                        let started = started.clone();
                        *worker.lock().unwrap() = Some(runtime.spawn_blocking(move || {
                            started.send(()).unwrap();
                            let _ = receiver.recv();
                            exited.store(true, Ordering::Release);
                        }));
                    });
                    let drain_called = Arc::clone(&drain_called);
                    scope.on_task_drain("native accept-error cleanup witness", move |_| {
                        drain_called.store(true, Ordering::Release);
                        async move {
                            if fail_component_drain {
                                Err(CLEANUP_ERROR.to_string())
                            } else {
                                Ok(())
                            }
                        }
                    });
                    let disposed = Arc::clone(&disposed);
                    scope.defer(move || disposed.store(true, Ordering::Release));
                    Ok(())
                }
            },
        )
        .unwrap(),
    );
    let catalog = crate::service_adapter::component_catalog_from_factories(vec![factory]).unwrap();
    let (result_tx, result_rx) = mpsc::channel();
    let daemon = thread::spawn(move || {
        let result = crate::control::run_daemon_control_loop_with_prepared_listener(
            prepared,
            paths,
            "daemon",
            None,
            catalog,
            crate::DaemonRuntimeConfig::default(),
        );
        let _ = result_tx.send((Instant::now(), result));
    });
    let mut native = NativeRun {
        listener: Some(listener),
        release: Some(release_tx),
        daemon: Some(daemon),
        worker,
    };
    started_rx.recv_timeout(OBSERVATION_BOUND).unwrap();
    let fault_at = Instant::now();
    native.interrupt_listener().unwrap();
    let returned = result_rx.recv_timeout(OBSERVATION_BOUND);
    let held_at_return = !exited.load(Ordering::Acquire);
    let drained_at_return = drain_called.load(Ordering::Acquire);
    let disposed_at_return = disposed.load(Ordering::Acquire);
    // Always release/join before assertions: the old default Runtime::drop
    // waits here indefinitely until this fixture releases its actual worker.
    let cleanup = native.cleanup();
    assert!(cleanup.is_ok(), "{cleanup:?}");
    let (finished, result) =
        returned.expect("accept failure must return while blocking work is held");
    assert!(finished.duration_since(fault_at) < OBSERVATION_BOUND);
    assert!(
        held_at_return,
        "the worker must remain held until after daemon return"
    );
    assert!(drained_at_return, "the normal component shutdown must run");
    if !fail_component_drain {
        assert!(
            disposed_at_return,
            "normal component teardown must complete"
        );
    }
    assert!(
        exited.load(Ordering::Acquire),
        "the actual worker must be joined"
    );
    let error = result.expect_err("actual listener shutdown must surface its accept error");
    assert!(
        matches!(&error, DaemonError::Network { address: actual, detail }
            if actual == &address && detail.contains("accept async daemon control listener")),
        "the original native accept error must survive component cleanup: {error:?}"
    );
    // Daemon facts are persisted by the canonical component event bus. The
    // Observe operational log is a separate derived view and need not exist
    // after its recorder component has stopped.
    let store = sinomo_core::events::JsonlEventStore::new(
        data_dir
            .root()
            .join(sinomo_core::events::EVENT_LOG_RELATIVE_PATH),
    );
    let events = sinomo_core::events::EventStore::replay(
        &store,
        sinomo_core::events::ReplayQuery {
            kind_prefix: Some("daemon.lifecycle.".into()),
            limit: Some(32),
            ..Default::default()
        },
    )
    .unwrap();
    let event = events
        .iter()
        .find(|event| {
            event.event.kind.as_str()
                == if fail_component_drain {
                    "daemon.lifecycle.stop-incomplete"
                } else {
                    "daemon.lifecycle.stopped"
                }
        })
        .expect("normal lifecycle result must retain both failure diagnostics");
    assert_eq!(
        event.event.payload["component"].as_str(),
        Some("daemon-control")
    );
    assert_eq!(event.event.payload["severity"].as_str(), Some("error"));
    let fields = &event.event.payload["fields"];
    assert_eq!(
        fields["accept_loop_error"].as_str(),
        Some(error.to_string().as_str())
    );
    if fail_component_drain {
        assert!(
            fields["component_shutdown_error"]
                .as_str()
                .unwrap()
                .contains(CLEANUP_ERROR)
        );
    } else {
        assert!(fields["component_shutdown_error"].is_null());
    }
}

#[test]
fn actual_accept_error_runs_bounded_cleanup_with_blocking_worker_held() {
    actual_accept_error_with_held_worker(false);
}

#[test]
fn actual_accept_error_survives_component_shutdown_failure() {
    actual_accept_error_with_held_worker(true);
}
