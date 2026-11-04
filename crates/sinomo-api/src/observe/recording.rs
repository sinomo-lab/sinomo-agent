//! Nonblocking producer side of Observe's durable event intake.
//! Hosts install a local writer; native modules use the same authenticated Observe route.
use sinomo_core::{base::EventEnvelope, local::SinomoDataDir};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
pub const APPEND_ROUTE: &str = "/observe/events/append";
pub enum Submission {
    Event(EventEnvelope),
    Flush(mpsc::Sender<()>),
}
struct Sink {
    id: u64,
    sender: mpsc::SyncSender<Submission>,
    worker: Option<std::thread::JoinHandle<()>>,
    stop: std::sync::Arc<AtomicBool>,
}
static SINKS: OnceLock<Mutex<BTreeMap<PathBuf, Sink>>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
pub struct Registration {
    root: PathBuf,
    id: u64,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(sinks) = SINKS.get() {
            let mut sinks = sinks.lock().unwrap_or_else(|e| e.into_inner());
            if sinks.get(&self.root).is_some_and(|sink| sink.id == self.id) {
                sinks.remove(&self.root);
            }
        }
    }
}
pub fn register(data: &SinomoDataDir, sender: mpsc::SyncSender<Submission>) -> Registration {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let previous = SINKS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(
            data.root(),
            Sink {
                id,
                sender,
                worker: None,
                stop: Default::default(),
            },
        );
    // A host may install its local writer after an early producer connected remotely.
    // Join that forwarding worker outside the registry lock before replacing its owner.
    if let Some(previous) = previous {
        previous.stop.store(true, Ordering::Release);
        drop(previous.sender);
        if let Some(worker) = previous.worker {
            let _ = worker.join();
        }
    }
    Registration {
        root: data.root(),
        id,
    }
}
pub fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub fn dropped(reason: &'static str) {
    metrics::counter!("sinomo.observe.events.delivery_failures","reason"=>reason).increment(1);
    // Logging every dropped record would amplify overload. Keep one warning per 64 drops.
    static COUNT: AtomicU64 = AtomicU64::new(0);
    let count = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if count == 1 || count % 64 == 0 {
        tracing::warn!(target:"sinomo.observe",reason,count,"observation delivery failed");
    }
}
pub fn submit(data: &SinomoDataDir, event: EventEnvelope) {
    let Some(sender) = sender(data) else {
        dropped("producer_busy");
        return;
    };
    if sender.try_send(Submission::Event(event)).is_err() {
        dropped("queue_full_or_closed");
    }
}
fn sender(data: &SinomoDataDir) -> Option<mpsc::SyncSender<Submission>> {
    {
        let sinks = SINKS.get_or_init(Default::default).lock().ok()?;
        if let Some(sink) = sinks.get(&data.root()) {
            return Some(sink.sender.clone());
        }
        if sinks.len() >= 16 {
            return None;
        }
    }
    let (tx, rx) = mpsc::sync_channel(4096);
    let root = data.root();
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let worker_stop = stop.clone();
    let worker=std::thread::Builder::new().name("sinomo-observe-producer".into()).spawn(move||{
        let data_dir=SinomoDataDir::new(root);
        let paths=crate::DaemonPaths::new(&data_dir);
        let mut client=None;
        while let Ok(submission)=rx.recv(){
            if worker_stop.load(Ordering::Acquire){dropped("shutdown_deadline");continue;}
            match submission{
            Submission::Flush(ack)=>{let _=ack.send(());},
            Submission::Event(event)=>{
                let result:Result<(),String>=(||{
                    if client.is_none(){client=Some(crate::DaemonApiClient::connect_for_paths_with_timeout(&paths,Duration::from_secs(2)).map_err(|e|e.to_string())?.with_timeout(Duration::from_secs(5)));}
                    let request=sinomo_core::network::ObjectRpcRequest::encode_body(APPEND_ROUTE,super::OBSERVE_RPC_CATEGORY,&event).map_err(|e|e.to_string())?;
                    let acknowledged:serde_json::Value=crate::transport::send_daemon_call_value_on_runtime(data_dir.clone(),client.as_ref().unwrap().address(),None,sinomo_core::network::ObjectRpcCall::value(request),Duration::from_secs(5),None).map_err(|e|e.to_string())?;
                    if acknowledged.get("accepted").and_then(serde_json::Value::as_bool)!=Some(true){return Err("Observe did not acknowledge event retention".into());}Ok(())
                })();
                if let Err(error)=result{client=None;dropped("transport");static ERRORS:AtomicU64=AtomicU64::new(0);let failures=ERRORS.fetch_add(1,Ordering::Relaxed)+1;if failures==1||failures%64==0{tracing::warn!(target:"sinomo.observe",%error,failures,"Observe event forwarding was not acknowledged");}}
            }
        }}
    }).ok()?;
    let mut sinks = SINKS.get_or_init(Default::default).lock().ok()?;
    if let Some(existing) = sinks.get(&data.root()) {
        let sender = existing.sender.clone();
        drop(sinks);
        drop(tx);
        let _ = worker.join();
        return Some(sender);
    }
    sinks.insert(
        data.root(),
        Sink {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            sender: tx.clone(),
            worker: Some(worker),
            stop,
        },
    );
    Some(tx)
}
/// Only host teardown calls this; requests never wait for persistence.
pub fn flush(data: &SinomoDataDir, timeout: Duration) -> bool {
    let sender = SINKS
        .get()
        .and_then(|s| s.lock().ok())
        .and_then(|s| s.get(&data.root()).map(|s| s.sender.clone()));
    let Some(sender) = sender else {
        return true;
    };
    let (tx, rx) = mpsc::channel();
    sender.try_send(Submission::Flush(tx)).is_ok() && rx.recv_timeout(timeout).is_ok()
}

/// Native-module shutdown must drain producer workers before its code can unload.
/// Returns only after every forwarding thread has exited; no callback outlives the library.
pub fn shutdown_producers(timeout: Duration) {
    let Some(sinks) = SINKS.get() else {
        return;
    };
    let roots = sinks
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|(_, sink)| sink.worker.is_some())
        .map(|(root, _)| root.clone())
        .collect::<Vec<_>>();
    let deadline = std::time::Instant::now() + timeout;
    for root in &roots {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if !flush(&SinomoDataDir::new(root), remaining) {
            dropped("shutdown_flush");
        }
    }
    let workers = {
        let mut sinks = sinks.lock().unwrap_or_else(|e| e.into_inner());
        roots
            .into_iter()
            .filter_map(|root| sinks.remove(&root))
            .collect::<Vec<_>>()
    };
    for sink in &workers {
        sink.stop.store(true, Ordering::Release);
    }
    let mut joins = Vec::new();
    for sink in workers {
        drop(sink.sender);
        if let Some(worker) = sink.worker {
            joins.push(worker);
        }
    }
    for worker in joins {
        let _ = worker.join();
    }
}
