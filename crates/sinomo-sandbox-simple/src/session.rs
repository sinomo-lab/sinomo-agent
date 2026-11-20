use super::*;
use sinomo_sandbox::{
    ByteRange, SandboxFileSystem, SandboxId, SandboxInstanceManifest, load_command_record,
};
use std::collections::HashMap;
use std::sync::{Condvar, LazyLock, Mutex, mpsc};

type Key = (PathBuf, Option<String>, SandboxCommandId);
static ACTIVE: LazyLock<Mutex<HashMap<Key, Arc<Execution>>>> = LazyLock::new(Default::default);

fn key(
    layout: &LocalSandboxInstanceLayout,
    manifest: &SandboxInstanceManifest,
    id: &SandboxCommandId,
) -> Result<Key, SandboxCommandExecutionError> {
    let root = layout
        .root
        .canonicalize()
        .map_err(|e| invalid_request(e.to_string()))?;
    Ok((
        root,
        manifest.incarnation_id.as_ref().map(ToString::to_string),
        id.clone(),
    ))
}

#[derive(Clone, Copy, Default)]
pub struct SandboxNativeExecutionOptions {
    pub interactive: bool,
    pub cleanup_on_exit: bool,
}

struct Input {
    sender: Option<mpsc::SyncSender<Vec<u8>>>,
    remaining: u64,
}

pub(super) struct Execution {
    pub id: SandboxCommandId,
    pub options: SandboxNativeExecutionOptions,
    pub cancellation: SandboxCancellationToken,
    input: Mutex<Input>,
    receiver: Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    pub streams: Mutex<(Vec<u8>, Vec<u8>)>,
    pub foreground_status: Mutex<Option<i32>>,
    done: Mutex<Option<Result<(), String>>>,
    finished: Condvar,
    ready: Mutex<Option<mpsc::Sender<()>>>,
    sink: Option<Arc<dyn SandboxOutputSink>>,
}

pub(super) type ResultSender =
    mpsc::Sender<Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError>>;
pub struct SandboxNativeExecution {
    pub command_id: SandboxCommandId,
    result: mpsc::Receiver<Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError>>,
}

impl SandboxNativeExecution {
    pub fn wait(self) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError> {
        self.result
            .recv()
            .map_err(|_| invalid_request("native execution worker stopped"))?
    }
}

impl Execution {
    pub fn started(&self) {
        if let Some(ready) = self.ready.lock().unwrap().take() {
            let _ = ready.send(());
        }
    }
    pub fn input(&self) -> Option<mpsc::Receiver<Vec<u8>>> {
        self.receiver.lock().unwrap().take()
    }
    pub fn close_input(&self) {
        self.input.lock().unwrap().sender.take();
    }
    pub fn launch<F>(
        layout: &LocalSandboxInstanceLayout,
        manifest: &SandboxInstanceManifest,
        id: SandboxCommandId,
        options: SandboxNativeExecutionOptions,
        cancellation: SandboxCancellationToken,
        sink: Option<Arc<dyn SandboxOutputSink>>,
        work: F,
    ) -> Result<SandboxNativeExecution, SandboxCommandExecutionError>
    where
        F: FnOnce(
                &Arc<Self>,
                &ResultSender,
            ) -> Result<SandboxLocalCommandOutcome, SandboxCommandExecutionError>
            + Send
            + 'static,
    {
        let key = key(layout, manifest, &id)?;
        let (tx, rx) = mpsc::sync_channel(16);
        let (ready, admitted) = mpsc::channel();
        let state = Arc::new(Self {
            id: id.clone(),
            ready: Mutex::new(Some(ready)),
            options,
            cancellation,
            streams: Default::default(),
            foreground_status: Default::default(),
            done: Default::default(),
            finished: Default::default(),
            input: Mutex::new(Input {
                sender: options.interactive.then_some(tx),
                remaining: manifest.limits.max_input_bytes,
            }),
            receiver: Mutex::new(options.interactive.then_some(rx)),
            sink,
        });
        ACTIVE.lock().unwrap().insert(key.clone(), state.clone());
        let (send, result) = mpsc::channel();
        let worker_key = key.clone();
        std::thread::Builder::new()
            .name("sandbox-native".into())
            .spawn(move || {
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(&state, &send)))
                        .unwrap_or_else(|_| {
                            Err(invalid_request("native execution worker panicked"))
                        });
                state.close_input();
                state.ready.lock().unwrap().take();
                *state.done.lock().unwrap() = Some(match &result {
                    Ok(_)
                    | Err(SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)) => {
                        Ok(())
                    }
                    Err(e) => Err(e.to_string()),
                });
                state.finished.notify_all();
                ACTIVE.lock().unwrap().remove(&worker_key);
                let _ = send.send(result);
            })
            .map_err(|e| {
                ACTIVE.lock().unwrap().remove(&key);
                invalid_request(e.to_string())
            })?;
        let handle = SandboxNativeExecution {
            command_id: id,
            result,
        };
        if admitted.recv().is_err() {
            handle.wait()?;
            return Err(invalid_request("native execution ended before launch"));
        }
        Ok(handle)
    }
}

impl SandboxOutputSink for Execution {
    fn output_chunk(&self, chunk: SandboxOutputChunk) {
        {
            let mut streams = self.streams.lock().unwrap();
            match chunk.stream {
                SandboxOutputStream::Stdout => &mut streams.0,
                SandboxOutputStream::Stderr => &mut streams.1,
            }
            .extend_from_slice(&chunk.bytes);
        }
        if let Some(sink) = &self.sink {
            sink.output_chunk(chunk);
        }
    }
}

pub enum SandboxNativeInteraction<'a> {
    Read,
    Write { bytes: &'a [u8], close: bool },
    Cancel,
}

pub struct SandboxNativeExecutionView {
    pub active: bool,
    pub exit_status: Option<i32>,
    pub error: Option<String>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Access requires the same installed sandbox, incarnation and placement policy
/// as launch. Returned stream windows start at independent byte offsets.
#[allow(clippy::too_many_arguments)]
pub fn interact_with_native_execution(
    store: &LocalSandboxInstanceStore,
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
    node: &NodeId,
    policy: &SandboxPolicy,
    action: SandboxNativeInteraction<'_>,
    stdout_offset: u64,
    stderr_offset: u64,
    limit: usize,
) -> Result<SandboxNativeExecutionView, SandboxCommandExecutionError> {
    let (manifest, layout) = store.load(sandbox_id)?;
    policy
        .validate_manifest(&manifest, node)
        .map_err(SandboxCommandExecutionError::PolicyDenied)?;
    let state = ACTIVE
        .lock()
        .unwrap()
        .get(&key(&layout, &manifest, command_id)?)
        .cloned();
    let limit = limit.min(64 * 1024);
    if let Some(state) = state {
        match action {
            SandboxNativeInteraction::Read => (),
            SandboxNativeInteraction::Cancel => {
                state.cancellation.cancel();
                let result = state
                    .finished
                    .wait_while(state.done.lock().unwrap(), |r| r.is_none())
                    .unwrap();
                if let Some(Err(e)) = result.as_ref() {
                    return Err(invalid_request(e.clone()));
                }
            }
            SandboxNativeInteraction::Write { bytes, close } => {
                let mut input = state.input.lock().unwrap();
                if bytes.len() as u64 > input.remaining {
                    return Err(invalid_request("native execution input budget exceeded"));
                }
                let sender = input
                    .sender
                    .as_ref()
                    .ok_or_else(|| invalid_request("execution stdin is closed"))?;
                if !bytes.is_empty() {
                    sender
                        .try_send(bytes.to_vec())
                        .map_err(|e| invalid_request(e.to_string()))?;
                    input.remaining -= bytes.len() as u64;
                }
                if close {
                    input.sender.take();
                }
            }
        }
        let streams = state.streams.lock().unwrap();
        let window = |s: &[u8], offset: u64| {
            s.get(usize::try_from(offset).unwrap_or(usize::MAX)..)
                .unwrap_or_default()
                .iter()
                .take(limit)
                .copied()
                .collect()
        };
        let done = state.done.lock().unwrap();
        return Ok(SandboxNativeExecutionView {
            active: done.is_none(),
            exit_status: *state.foreground_status.lock().unwrap(),
            error: done.as_ref().and_then(|r| r.as_ref().err()).cloned(),
            stdout: window(&streams.0, stdout_offset),
            stderr: window(&streams.1, stderr_offset),
        });
    }
    // Settlement can publish the record and retire the live handle after the
    // manifest read above. Re-read history before classifying an unknown ID.
    let (latest, _) = store.load(sandbox_id)?;
    if latest.incarnation_id != manifest.incarnation_id
        || !latest.command_history.contains(command_id)
    {
        return Err(invalid_request("unknown execution in this sandbox"));
    }
    if matches!(action, SandboxNativeInteraction::Write { .. }) {
        return Err(invalid_request("execution stdin is closed"));
    }
    let mut fs = LocalSandboxFileSystem::new(layout);
    let record = load_command_record(&mut fs, command_id)?;
    let read = |path, offset| -> Result<Vec<u8>, SandboxCommandExecutionError> {
        if record.response.is_none() {
            return Ok(Vec::new());
        }
        Ok(fs.read(
            &path,
            Some(ByteRange {
                start: offset,
                end: Some(offset.saturating_add(limit as u64)),
            }),
        )?)
    };
    Ok(SandboxNativeExecutionView {
        active: false,
        exit_status: record.response.as_ref().map(|r| r.exit_status),
        error: record.error.clone(),
        stdout: read(
            sinomo_sandbox::sandbox_command_stdout_log_path(command_id)
                .map_err(|e| invalid_request(e.to_string()))?,
            stdout_offset,
        )?,
        stderr: read(
            sinomo_sandbox::sandbox_command_stderr_log_path(command_id)
                .map_err(|e| invalid_request(e.to_string()))?,
            stderr_offset,
        )?,
    })
}
