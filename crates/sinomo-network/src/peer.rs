use crate::error::NetworkError;
use crate::prelude::*;
use crate::rpc::ObjectRpcRequest;
use arc_swap::ArcSwap;
use std::sync::Condvar;

fn try_reserve_vec_deque<T>(
    queue: &mut VecDeque<T>,
    additional: usize,
    operation: &str,
) -> Result<(), NetworkError> {
    queue
        .try_reserve(additional)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: operation.to_string(),
            requested_bytes: additional.saturating_mul(std::mem::size_of::<T>()),
        })
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PeerEndpointCandidate {
    pub endpoint: ClusterEndpointRecord,
    pub status: EndpointCandidateStatus,
    pub score: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

impl PeerEndpointCandidate {
    pub fn discovered(endpoint: ClusterEndpointRecord) -> Self {
        let score = endpoint.priority;
        Self {
            endpoint,
            status: EndpointCandidateStatus::Discovered,
            score,
            failure_reason: None,
        }
    }

    pub fn healthy(mut self, observed_latency_ms: u32) -> Self {
        self.endpoint.last_observed_latency_ms = Some(observed_latency_ms);
        self.status = EndpointCandidateStatus::Healthy;
        self.score = self
            .endpoint
            .priority
            .saturating_add((observed_latency_ms / 10).min(u32::from(u16::MAX)) as u16);
        self
    }

    pub fn failed(mut self, reason: impl Into<String>) -> Self {
        self.status = EndpointCandidateStatus::Failed;
        self.failure_reason = Some(reason.into());
        self.score = u16::MAX;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointCandidateStatus {
    Discovered,
    Probing,
    Healthy,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InProcessObjectRpcMessage {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub request: ObjectRpcRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InboundObjectRpcMessage {
    pub received_by: NodeId,
    pub message: InProcessObjectRpcMessage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AsyncNetworkSendOptions {
    timeout: Option<Duration>,
    nonblocking: bool,
}

impl Default for AsyncNetworkSendOptions {
    fn default() -> Self {
        Self::timeout(crate::DEFAULT_NETWORK_TIMEOUT)
    }
}

impl AsyncNetworkSendOptions {
    pub const fn timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            nonblocking: false,
        }
    }

    pub fn from_timeout(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            nonblocking: false,
        }
    }

    pub const fn nonblocking() -> Self {
        Self {
            timeout: None,
            nonblocking: true,
        }
    }

    pub const fn timeout_duration(self) -> Option<Duration> {
        self.timeout
    }

    pub const fn is_nonblocking(self) -> bool {
        self.nonblocking
    }
}

#[cfg(test)]
mod async_network_send_options_tests {
    use super::*;

    #[test]
    fn default_send_is_bounded_and_unbounded_modes_are_explicit() {
        let bounded = AsyncNetworkSendOptions::default();
        assert_eq!(
            bounded.timeout_duration(),
            Some(crate::DEFAULT_NETWORK_TIMEOUT)
        );
        assert!(!bounded.is_nonblocking());

        let unbounded = AsyncNetworkSendOptions::from_timeout(None);
        assert_eq!(unbounded.timeout_duration(), None);
        assert!(!unbounded.is_nonblocking());

        let nonblocking = AsyncNetworkSendOptions::nonblocking();
        assert_eq!(nonblocking.timeout_duration(), None);
        assert!(nonblocking.is_nonblocking());
    }
}

/// Synchronous in-memory object-RPC harness used by integration tests.
#[derive(Clone, Debug, Default)]
pub struct InProcessNetwork {
    inner: Arc<InProcessNetworkInner>,
}

pub const IN_PROCESS_MAILBOX_CAPACITY: usize = 4096;

impl InProcessNetwork {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&self, node_id: NodeId) -> InProcessTransport {
        let mut state = self.inner.state.lock().expect("in-process network lock");
        state
            .object_rpc_mailboxes
            .entry(node_id.clone())
            .or_default();
        drop(state);

        InProcessTransport {
            local_node_id: node_id,
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug, Default)]
struct InProcessNetworkInner {
    state: Mutex<InProcessNetworkState>,
    incoming: Condvar,
}

#[derive(Clone, Debug, Default)]
struct InProcessNetworkState {
    object_rpc_mailboxes: BTreeMap<NodeId, VecDeque<InboundObjectRpcMessage>>,
}

#[derive(Clone, Debug)]
pub struct InProcessTransport {
    local_node_id: NodeId,
    inner: Arc<InProcessNetworkInner>,
}

impl InProcessTransport {
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn send_object_rpc_request(
        &mut self,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
    ) -> Result<(), NetworkError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| in_process_lock_error())?;
        let mailbox = state
            .object_rpc_mailboxes
            .get_mut(target_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })?;
        if mailbox.len() >= IN_PROCESS_MAILBOX_CAPACITY {
            return Err(NetworkError::MailboxFull {
                node_id: target_node_id.clone(),
                max_frames: IN_PROCESS_MAILBOX_CAPACITY,
            });
        }

        try_reserve_vec_deque(mailbox, 1, "enqueue in-process object rpc message")?;
        mailbox.push_back(InboundObjectRpcMessage {
            received_by: target_node_id.clone(),
            message: InProcessObjectRpcMessage {
                source_node_id: self.local_node_id.clone(),
                target_node_id: target_node_id.clone(),
                request,
            },
        });
        drop(state);
        self.inner.incoming.notify_all();
        Ok(())
    }

    pub fn drain_incoming_object_rpc_messages(
        &mut self,
    ) -> Result<Vec<InboundObjectRpcMessage>, NetworkError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| in_process_lock_error())?;
        let mailbox = state
            .object_rpc_mailboxes
            .get_mut(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;

        let mut messages = Vec::new();
        crate::object::try_reserve_vec(
            &mut messages,
            mailbox.len(),
            "drain in-process object rpc messages",
        )?;
        messages.extend(mailbox.drain(..));
        Ok(messages)
    }

    pub fn drain_incoming_object_rpc_requests(
        &mut self,
    ) -> Result<Vec<ObjectRpcRequest>, NetworkError> {
        let messages = self.drain_incoming_object_rpc_messages()?;
        let mut requests = Vec::new();
        crate::object::try_reserve_vec(
            &mut requests,
            messages.len(),
            "drain in-process object rpc requests",
        )?;
        requests.extend(messages.into_iter().map(|inbound| inbound.message.request));
        Ok(requests)
    }

    pub fn pending_incoming_object_rpc_request_count(&self) -> Result<Option<usize>, NetworkError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| in_process_lock_error())?;
        let mailbox = state
            .object_rpc_mailboxes
            .get(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;
        Ok(Some(mailbox.len()))
    }

    pub fn wait_for_incoming_object_rpc_request(
        &self,
        timeout: Duration,
    ) -> Result<bool, NetworkError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| in_process_lock_error())?;
        let mailbox = state
            .object_rpc_mailboxes
            .get(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;
        if !mailbox.is_empty() || timeout.is_zero() {
            return Ok(!mailbox.is_empty());
        }

        let local_node_id = self.local_node_id.clone();
        let (guard, _) = self
            .inner
            .incoming
            .wait_timeout_while(state, timeout, |state| {
                state
                    .object_rpc_mailboxes
                    .get(&local_node_id)
                    .is_some_and(VecDeque::is_empty)
            })
            .map_err(|_| in_process_lock_error())?;
        state = guard;
        let mailbox = state
            .object_rpc_mailboxes
            .get(&self.local_node_id)
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })?;
        Ok(!mailbox.is_empty())
    }
}

fn in_process_lock_error() -> NetworkError {
    NetworkError::Serialization {
        detail: "in-process network lock poisoned".to_string(),
    }
}

#[derive(Clone)]
pub struct AsyncInProcessNetwork {
    inner: Arc<AsyncInProcessNetworkInner>,
}

impl Default for AsyncInProcessNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncInProcessNetwork {
    pub fn new() -> Self {
        Self::with_mailbox_capacity(IN_PROCESS_MAILBOX_CAPACITY)
    }

    pub fn with_mailbox_capacity(mailbox_capacity: usize) -> Self {
        Self::with_mailbox_capacity_and_per_target_send_limit(mailbox_capacity, mailbox_capacity)
    }

    pub fn with_mailbox_capacity_and_per_target_send_limit(
        mailbox_capacity: usize,
        per_target_send_limit: usize,
    ) -> Self {
        Self {
            inner: Arc::new(AsyncInProcessNetworkInner {
                mailbox_capacity: mailbox_capacity.max(1),
                per_target_send_limit: per_target_send_limit.max(1),
                targets: ArcSwap::from_pointee(BTreeMap::new()),
                target_update_lock: Mutex::new(()),
            }),
        }
    }

    pub fn add_node(&self, node_id: NodeId) -> AsyncInProcessTransport {
        let (sender, incoming) = tokio::sync::mpsc::channel(self.inner.mailbox_capacity);
        let target = AsyncInProcessTarget {
            sender,
            mailbox_capacity: self.inner.mailbox_capacity,
            send_limiter: Arc::new(tokio::sync::Semaphore::new(
                self.inner.per_target_send_limit,
            )),
        };
        let _guard = self
            .inner
            .target_update_lock
            .lock()
            .expect("async in-process network target update lock");
        let mut targets = (*self.inner.targets.load_full()).clone();
        targets.insert(node_id.clone(), target);
        self.inner.targets.store(Arc::new(targets));

        AsyncInProcessTransport {
            local_node_id: node_id,
            inner: Arc::clone(&self.inner),
            incoming,
            pending: VecDeque::new(),
        }
    }
}

struct AsyncInProcessNetworkInner {
    mailbox_capacity: usize,
    per_target_send_limit: usize,
    targets: ArcSwap<BTreeMap<NodeId, AsyncInProcessTarget>>,
    target_update_lock: Mutex<()>,
}

#[derive(Clone)]
struct AsyncInProcessTarget {
    sender: tokio::sync::mpsc::Sender<InboundObjectRpcMessage>,
    mailbox_capacity: usize,
    send_limiter: Arc<tokio::sync::Semaphore>,
}

pub struct AsyncInProcessTransport {
    local_node_id: NodeId,
    inner: Arc<AsyncInProcessNetworkInner>,
    incoming: tokio::sync::mpsc::Receiver<InboundObjectRpcMessage>,
    pending: VecDeque<InboundObjectRpcMessage>,
}

impl AsyncInProcessTransport {
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn target_channel(
        &self,
        target_node_id: &NodeId,
    ) -> Result<AsyncInProcessTarget, NetworkError> {
        self.inner
            .targets
            .load_full()
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })
    }

    fn pending_count(&self) -> usize {
        self.pending.len() + self.incoming.len()
    }

    pub async fn send_object_rpc_request_async(
        &self,
        target_node_id: &NodeId,
        request: ObjectRpcRequest,
        options: AsyncNetworkSendOptions,
    ) -> Result<(), NetworkError> {
        let target = self.target_channel(target_node_id)?;
        send_async_in_process_object_rpc_message(
            target,
            &self.local_node_id,
            target_node_id,
            request,
            options,
        )
        .await
    }

    pub async fn drain_incoming_object_rpc_messages(
        &mut self,
        max_messages: usize,
    ) -> Result<Vec<InboundObjectRpcMessage>, NetworkError> {
        let take = max_messages.max(1);
        let mut messages = Vec::new();
        crate::object::try_reserve_vec(
            &mut messages,
            take.min(self.pending_count()),
            "drain pending object rpc messages",
        )?;
        while messages.len() < take {
            let Some(message) = self.pending.pop_front() else {
                break;
            };
            messages.push(message);
        }
        while messages.len() < take {
            let Ok(message) = self.incoming.try_recv() else {
                break;
            };
            messages.push(message);
        }
        Ok(messages)
    }

    pub async fn drain_incoming_object_rpc_requests(
        &mut self,
        max_messages: usize,
    ) -> Result<Vec<ObjectRpcRequest>, NetworkError> {
        let messages = self
            .drain_incoming_object_rpc_messages(max_messages)
            .await?;
        let mut requests = Vec::new();
        crate::object::try_reserve_vec(
            &mut requests,
            messages.len(),
            "drain pending object rpc requests",
        )?;
        requests.extend(messages.into_iter().map(|inbound| inbound.message.request));
        Ok(requests)
    }

    pub async fn pending_incoming_object_rpc_request_count(
        &mut self,
    ) -> Result<Option<usize>, NetworkError> {
        Ok(Some(self.pending_count()))
    }

    pub async fn wait_for_incoming_object_rpc_request(
        &mut self,
        timeout: Duration,
    ) -> Result<bool, NetworkError> {
        if self.pending_count() > 0 {
            return Ok(true);
        }
        if timeout.is_zero() {
            return Ok(false);
        }
        match tokio::time::timeout(timeout, self.incoming.recv()).await {
            Ok(Some(message)) => {
                try_reserve_vec_deque(&mut self.pending, 1, "queue pending object rpc message")?;
                self.pending.push_back(message);
                Ok(true)
            }
            Ok(None) => Err(NetworkError::Io {
                operation: "async in-process object rpc receive".to_string(),
                detail: format!("object rpc mailbox for {} is closed", self.local_node_id),
            }),
            Err(_) => Ok(false),
        }
    }
}

async fn send_async_in_process_object_rpc_message(
    target: AsyncInProcessTarget,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    request: ObjectRpcRequest,
    options: AsyncNetworkSendOptions,
) -> Result<(), NetworkError> {
    let inbound = InboundObjectRpcMessage {
        received_by: target_node_id.clone(),
        message: InProcessObjectRpcMessage {
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            request,
        },
    };
    if options.is_nonblocking() {
        let _permit =
            target
                .send_limiter
                .try_acquire_owned()
                .map_err(|_| NetworkError::MailboxFull {
                    node_id: target_node_id.clone(),
                    max_frames: target.mailbox_capacity,
                })?;
        return target
            .sender
            .try_send(inbound)
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => NetworkError::MailboxFull {
                    node_id: target_node_id.clone(),
                    max_frames: target.mailbox_capacity,
                },
                tokio::sync::mpsc::error::TrySendError::Closed(_) => NetworkError::UnknownNode {
                    node_id: target_node_id.clone(),
                },
            });
    }

    match options.timeout_duration() {
        Some(timeout) => {
            let deadline = tokio::time::Instant::now() + timeout;
            let _permit = tokio::time::timeout_at(deadline, target.send_limiter.acquire_owned())
                .await
                .map_err(|_| async_transport_send_timed_out(target_node_id, timeout))?
                .map_err(|source| NetworkError::Io {
                    operation: format!("async object rpc transport send to {target_node_id}"),
                    detail: format!("send limiter closed: {source}"),
                })?;
            tokio::time::timeout_at(deadline, target.sender.send(inbound))
                .await
                .map_err(|_| async_transport_send_timed_out(target_node_id, timeout))?
        }
        None => {
            let _permit = target
                .send_limiter
                .acquire_owned()
                .await
                .map_err(|source| NetworkError::Io {
                    operation: format!("async object rpc transport send to {target_node_id}"),
                    detail: format!("send limiter closed: {source}"),
                })?;
            target.sender.send(inbound).await
        }
    }
    .map_err(|_| NetworkError::UnknownNode {
        node_id: target_node_id.clone(),
    })
}

fn async_transport_send_timed_out(target_node_id: &NodeId, timeout: Duration) -> NetworkError {
    NetworkError::Io {
        operation: format!("async transport send to {target_node_id}"),
        detail: format!("timed out after {}ms", timeout.as_millis()),
    }
}
