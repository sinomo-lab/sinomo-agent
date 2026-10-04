//! Which node runs a chat, and how other nodes reach it.
//!
//! A chat has one live runtime in the cluster. The node running it holds an
//! ownership record in cluster KV and renews it while the chat is hosted.
//! Windows on other nodes forward commands to the owner and follow its
//! events. When the owner stops renewing (it stopped or lost the cluster),
//! another node takes the chat over and restores it from durable state.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sinomo_runtime::api::app::chat as wire;
use sinomo_runtime::kv::{
    CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE, CLUSTER_KV_RPC_CATEGORY, CLUSTER_KV_TXN_ROUTE,
    ClusterKvEntryRequest, ClusterKvTxnRpcResponse, KvAuthoritativeRead, KvCompare, KvEntry, KvOp,
    KvTxn, KvTxnRequest,
};
use sinomo_runtime::network::ObjectRpcRequest;
use sinomo_runtime::node::NodeId;

/// A host that has not renewed its ownership for this long is presumed gone.
pub(super) const CHAT_OWNER_TTL: Duration = Duration::from_secs(45);
const CLUSTER_KV_TIMEOUT: Duration = Duration::from_secs(15);
const PEER_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_EVENTS_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ChatOwner {
    /// This node runs the chat.
    Local,
    /// Another node runs the chat; route to it.
    Remote(NodeId),
}

/// Decides which node runs each chat.
pub(super) trait ChatOwnership: Send + Sync {
    /// Become the owner unless a live owner elsewhere holds the chat.
    fn claim(&self, chat_id: &str) -> Result<ChatOwner, String>;
    /// Extend this node's ownership. `Ok(false)` means another node owns it.
    fn renew(&self, chat_id: &str) -> Result<bool, String>;
    /// Give up ownership when the chat is no longer hosted here.
    fn release(&self, chat_id: &str);
    /// The chat's live owner, without claiming it. `None` when no node
    /// hosts it.
    fn current_owner(&self, chat_id: &str) -> Result<Option<ChatOwner>, String>;
}

/// Reaches the chat service on another node.
pub(super) trait ChatPeers: Send + Sync {
    fn command(
        &self,
        node: &NodeId,
        request: wire::ChatPeerCommandRequest,
    ) -> Result<wire::ChatPeerCommandResponse, String>;
    fn events(
        &self,
        node: &NodeId,
        request: wire::ChatPeerEventsRequest,
    ) -> Result<wire::ChatPeerEventsResponse, String>;
    fn delete(&self, node: &NodeId, request: wire::ChatPeerDeleteRequest) -> Result<(), String>;
}

/// Ownership and peer access for this node.
#[derive(Clone)]
pub(super) struct ChatCluster {
    pub(super) node_id: Option<NodeId>,
    pub(super) ownership: Arc<dyn ChatOwnership>,
    pub(super) peers: Arc<dyn ChatPeers>,
}

impl ChatCluster {
    /// A node that is not part of a cluster owns every chat it shows.
    pub(super) fn standalone() -> Self {
        Self {
            node_id: None,
            ownership: Arc::new(StandaloneOwnership),
            peers: Arc::new(NoPeers),
        }
    }

    pub(super) fn for_data_dir(
        data_dir: &sinomo_runtime::local::SinomoDataDir,
        daemon_address: &str,
    ) -> Self {
        let Ok(state) = crate::read_cluster_state(&crate::ClusterPaths::new(data_dir)) else {
            return Self::standalone();
        };
        let node_id = state.local_node_id;
        Self {
            node_id: Some(node_id.clone()),
            ownership: Arc::new(KvChatOwnership {
                records: DaemonKvRecords {
                    daemon_address: daemon_address.to_string(),
                },
                node_id: node_id.to_string(),
                instance: sinomo_agent::agent_runtime_host_instance().to_string(),
            }),
            peers: Arc::new(DaemonChatPeers {
                daemon_address: daemon_address.to_string(),
                data_dir: data_dir.clone(),
            }),
        }
    }

    /// The name another node uses for one of this node's views.
    pub(super) fn qualify_origin(&self, view: &str) -> String {
        match &self.node_id {
            Some(node) => format!("{node}/{view}"),
            None => view.to_string(),
        }
    }

    /// This node's view named by a qualified origin, if it is one of ours.
    pub(super) fn local_origin<'a>(&self, origin: &'a str) -> Option<&'a str> {
        match &self.node_id {
            Some(node) => origin
                .strip_prefix(node.to_string().as_str())
                .and_then(|rest| rest.strip_prefix('/')),
            None => Some(origin),
        }
    }
}

struct StandaloneOwnership;

impl ChatOwnership for StandaloneOwnership {
    fn claim(&self, _: &str) -> Result<ChatOwner, String> {
        Ok(ChatOwner::Local)
    }
    fn renew(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn release(&self, _: &str) {}
    fn current_owner(&self, _: &str) -> Result<Option<ChatOwner>, String> {
        Ok(None)
    }
}

struct NoPeers;

impl ChatPeers for NoPeers {
    fn command(
        &self,
        node: &NodeId,
        _: wire::ChatPeerCommandRequest,
    ) -> Result<wire::ChatPeerCommandResponse, String> {
        Err(format!("node `{node}` is unreachable outside a cluster"))
    }
    fn events(
        &self,
        node: &NodeId,
        _: wire::ChatPeerEventsRequest,
    ) -> Result<wire::ChatPeerEventsResponse, String> {
        Err(format!("node `{node}` is unreachable outside a cluster"))
    }
    fn delete(&self, node: &NodeId, _: wire::ChatPeerDeleteRequest) -> Result<(), String> {
        Err(format!("node `{node}` is unreachable outside a cluster"))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct ChatOwnerRecord {
    pub(super) node_id: String,
    /// The owning process; a restarted daemon takes its chats back at once.
    pub(super) instance: String,
    pub(super) renewed_at_unix_ms: u64,
}

pub(super) fn chat_owner_key(chat_id: &str) -> String {
    format!("/agent/chat-hosts/{chat_id}")
}

pub(super) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// Authoritative reads and conditional commits of cluster KV records.
pub(super) trait KvRecords: Send + Sync {
    fn read(&self, key: &str) -> Result<Option<KvEntry>, String>;
    /// Whether the transaction's compares held and it committed.
    fn commit(&self, txn: KvTxn) -> Result<bool, String>;
}

impl<T: KvRecords> KvRecords for Arc<T> {
    fn read(&self, key: &str) -> Result<Option<KvEntry>, String> {
        T::read(self, key)
    }
    fn commit(&self, txn: KvTxn) -> Result<bool, String> {
        T::commit(self, txn)
    }
}

/// Cluster KV through the daemon's KV API.
struct DaemonKvRecords {
    daemon_address: String,
}

impl DaemonKvRecords {
    fn call<Req: Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        route: &str,
        request: &Req,
    ) -> Result<Resp, String> {
        let rpc = ObjectRpcRequest::encode_body(route, CLUSTER_KV_RPC_CATEGORY, request)
            .map_err(|error| error.to_string())?;
        sinomo_runtime::api::send_daemon_rpc(&self.daemon_address, &rpc, CLUSTER_KV_TIMEOUT)
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())
    }
}

impl KvRecords for DaemonKvRecords {
    fn read(&self, key: &str) -> Result<Option<KvEntry>, String> {
        let read: KvAuthoritativeRead<Option<KvEntry>> = self.call(
            CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
            &ClusterKvEntryRequest {
                key: key.to_string(),
            },
        )?;
        Ok(read.value)
    }

    fn commit(&self, txn: KvTxn) -> Result<bool, String> {
        let response: ClusterKvTxnRpcResponse =
            self.call(CLUSTER_KV_TXN_ROUTE, &KvTxnRequest::generated(txn))?;
        Ok(response
            .txn_response
            .is_some_and(|response| response.succeeded))
    }
}

/// Ownership records in cluster KV.
pub(super) struct KvChatOwnership<R> {
    pub(super) records: R,
    pub(super) node_id: String,
    pub(super) instance: String,
}

impl<R: KvRecords> KvChatOwnership<R> {
    fn read(&self, chat_id: &str) -> Result<Option<(ChatOwnerRecord, KvEntry)>, String> {
        self.records
            .read(&chat_owner_key(chat_id))?
            .map(|entry| {
                serde_json::from_slice::<ChatOwnerRecord>(&entry.value)
                    .map(|record| (record, entry))
                    .map_err(|error| format!("invalid chat owner record: {error}"))
            })
            .transpose()
    }

    /// Write this node's record if `compare` still holds.
    fn write_if(&self, chat_id: &str, compare: KvCompare) -> Result<bool, String> {
        let record = ChatOwnerRecord {
            node_id: self.node_id.clone(),
            instance: self.instance.clone(),
            renewed_at_unix_ms: now_unix_ms(),
        };
        let value = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        self.commit(KvTxn {
            compares: vec![compare],
            success: vec![KvOp::Put {
                key: chat_owner_key(chat_id),
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })
    }

    fn commit(&self, txn: KvTxn) -> Result<bool, String> {
        self.records.commit(txn)
    }

    fn is_ours(&self, record: &ChatOwnerRecord) -> bool {
        record.node_id == self.node_id && record.instance == self.instance
    }

    fn is_fresh(record: &ChatOwnerRecord) -> bool {
        now_unix_ms().saturating_sub(record.renewed_at_unix_ms) <= CHAT_OWNER_TTL.as_millis() as u64
    }
}

impl<R: KvRecords> ChatOwnership for KvChatOwnership<R> {
    fn claim(&self, chat_id: &str) -> Result<ChatOwner, String> {
        for _ in 0..3 {
            let compare = match self.read(chat_id)? {
                None => KvCompare::Missing {
                    key: chat_owner_key(chat_id),
                },
                Some((record, _)) if self.is_ours(&record) => return Ok(ChatOwner::Local),
                Some((record, entry)) => {
                    let fresh = Self::is_fresh(&record);
                    // A live owner elsewhere keeps the chat. This node's own
                    // earlier process does not: it has restarted.
                    if fresh && record.node_id != self.node_id {
                        let node = record
                            .node_id
                            .parse::<NodeId>()
                            .map_err(|error| format!("invalid chat owner node: {error}"))?;
                        return Ok(ChatOwner::Remote(node));
                    }
                    KvCompare::ModifyRevisionEquals {
                        key: chat_owner_key(chat_id),
                        revision: entry.modify_revision,
                    }
                }
            };
            if self.write_if(chat_id, compare)? {
                return Ok(ChatOwner::Local);
            }
        }
        Err(format!("could not settle the owner of chat `{chat_id}`"))
    }

    fn renew(&self, chat_id: &str) -> Result<bool, String> {
        match self.read(chat_id)? {
            Some((record, entry)) if self.is_ours(&record) => self.write_if(
                chat_id,
                KvCompare::ModifyRevisionEquals {
                    key: chat_owner_key(chat_id),
                    revision: entry.modify_revision,
                },
            ),
            _ => Ok(false),
        }
    }

    fn release(&self, chat_id: &str) {
        let result = (|| {
            if let Some((record, entry)) = self.read(chat_id)?
                && self.is_ours(&record)
            {
                self.commit(KvTxn {
                    compares: vec![KvCompare::ModifyRevisionEquals {
                        key: chat_owner_key(chat_id),
                        revision: entry.modify_revision,
                    }],
                    success: vec![KvOp::Delete {
                        key: chat_owner_key(chat_id),
                    }],
                    failure: Vec::new(),
                })?;
            }
            Ok::<_, String>(())
        })();
        if let Err(error) = result {
            tracing::warn!(%chat_id, %error, "release chat ownership failed");
        }
    }

    fn current_owner(&self, chat_id: &str) -> Result<Option<ChatOwner>, String> {
        Ok(match self.read(chat_id)? {
            Some((record, _)) if self.is_ours(&record) => Some(ChatOwner::Local),
            Some((record, _)) if Self::is_fresh(&record) && record.node_id != self.node_id => {
                Some(ChatOwner::Remote(
                    record
                        .node_id
                        .parse::<NodeId>()
                        .map_err(|error| format!("invalid chat owner node: {error}"))?,
                ))
            }
            _ => None,
        })
    }
}

/// Other nodes' chat services, through the daemon's node-targeted RPC.
struct DaemonChatPeers {
    daemon_address: String,
    data_dir: sinomo_runtime::local::SinomoDataDir,
}

impl DaemonChatPeers {
    fn call<Req: Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        node: &NodeId,
        route: &str,
        request: &Req,
        timeout: Duration,
    ) -> Result<Resp, String> {
        let rpc = ObjectRpcRequest::encode_body(route, wire::CHAT_RPC_CATEGORY, request)
            .map_err(|error| error.to_string())?;
        sinomo_runtime::api::DaemonApiClient::at_address(self.daemon_address.clone())
            .with_timeout(timeout)
            .object_rpc_to_target_from_data_dir_with_affinity(
                &self.data_dir,
                node,
                rpc,
                timeout,
                sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
            )
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())
    }
}

impl ChatPeers for DaemonChatPeers {
    fn command(
        &self,
        node: &NodeId,
        request: wire::ChatPeerCommandRequest,
    ) -> Result<wire::ChatPeerCommandResponse, String> {
        self.call(
            node,
            wire::CHAT_PEER_COMMAND_ROUTE,
            &request,
            PEER_COMMAND_TIMEOUT,
        )
    }

    fn events(
        &self,
        node: &NodeId,
        request: wire::ChatPeerEventsRequest,
    ) -> Result<wire::ChatPeerEventsResponse, String> {
        self.call(
            node,
            wire::CHAT_PEER_EVENTS_ROUTE,
            &request,
            PEER_EVENTS_TIMEOUT,
        )
    }

    fn delete(&self, node: &NodeId, request: wire::ChatPeerDeleteRequest) -> Result<(), String> {
        let _: wire::ChatHistoryMutationResponse = self.call(
            node,
            wire::CHAT_PEER_DELETE_ROUTE,
            &request,
            PEER_COMMAND_TIMEOUT,
        )?;
        Ok(())
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use sinomo_runtime::kv::{KvRevision, KvVersion};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Cluster KV in memory, evaluating the compares ownership relies on.
    #[derive(Default)]
    pub(in crate::chat_service) struct MemoryKv {
        entries: Mutex<(u64, BTreeMap<String, KvEntry>)>,
    }

    impl MemoryKv {
        pub(in crate::chat_service) fn put_record(&self, chat_id: &str, record: &ChatOwnerRecord) {
            assert!(
                self.commit(KvTxn {
                    compares: Vec::new(),
                    success: vec![KvOp::Put {
                        key: chat_owner_key(chat_id),
                        value: serde_json::to_vec(record).unwrap(),
                        lease_id: None,
                    }],
                    failure: Vec::new(),
                })
                .unwrap()
            );
        }
    }

    impl KvRecords for MemoryKv {
        fn read(&self, key: &str) -> Result<Option<KvEntry>, String> {
            Ok(self.entries.lock().unwrap().1.get(key).cloned())
        }

        fn commit(&self, txn: KvTxn) -> Result<bool, String> {
            let mut guard = self.entries.lock().unwrap();
            let (revision, entries) = &mut *guard;
            let holds = txn.compares.iter().all(|compare| match compare {
                KvCompare::Missing { key } => !entries.contains_key(key),
                KvCompare::ModifyRevisionEquals { key, revision } => entries
                    .get(key)
                    .is_some_and(|entry| entry.modify_revision == *revision),
                other => panic!("unsupported compare {other:?}"),
            });
            if !holds {
                return Ok(false);
            }
            for op in txn.success {
                *revision += 1;
                match op {
                    KvOp::Put { key, value, .. } => {
                        let create_revision = entries
                            .get(&key)
                            .map(|entry| entry.create_revision)
                            .unwrap_or(KvRevision(*revision));
                        entries.insert(
                            key.clone(),
                            KvEntry {
                                key,
                                value,
                                create_revision,
                                modify_revision: KvRevision(*revision),
                                version: KvVersion(*revision),
                                lease_id: None,
                            },
                        );
                    }
                    KvOp::Delete { key } => {
                        entries.remove(&key);
                    }
                    other => panic!("unsupported op {other:?}"),
                }
            }
            Ok(true)
        }
    }

    pub(in crate::chat_service) fn ownership(
        kv: &Arc<MemoryKv>,
        node: &str,
        instance: &str,
    ) -> KvChatOwnership<Arc<MemoryKv>> {
        KvChatOwnership {
            records: kv.clone(),
            node_id: node.to_string(),
            instance: instance.to_string(),
        }
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).unwrap()
    }

    #[test]
    fn one_node_owns_a_chat_until_it_stops_renewing() {
        let kv = Arc::new(MemoryKv::default());
        let a = node("node_a").to_string();
        let b = node("node_b").to_string();
        let on_a = ownership(&kv, &a, "a-1");
        let on_b = ownership(&kv, &b, "b-1");

        assert_eq!(on_a.claim("chat").unwrap(), ChatOwner::Local);
        assert_eq!(on_a.claim("chat").unwrap(), ChatOwner::Local);
        assert_eq!(
            on_b.claim("chat").unwrap(),
            ChatOwner::Remote(node("node_a"))
        );
        assert!(on_a.renew("chat").unwrap());
        assert!(!on_b.renew("chat").unwrap(), "a follower never renews");

        // The owner stopped renewing long enough ago: another node takes over,
        // and the old owner learns it lost the chat.
        kv.put_record(
            "chat",
            &ChatOwnerRecord {
                node_id: a.clone(),
                instance: "a-1".into(),
                renewed_at_unix_ms: now_unix_ms() - CHAT_OWNER_TTL.as_millis() as u64 - 1_000,
            },
        );
        assert_eq!(on_b.claim("chat").unwrap(), ChatOwner::Local);
        assert!(!on_a.renew("chat").unwrap());
        assert_eq!(
            on_a.claim("chat").unwrap(),
            ChatOwner::Remote(node("node_b"))
        );

        // Releasing hands the chat to whoever claims it next.
        on_a.release("chat");
        assert_eq!(
            on_a.claim("chat").unwrap(),
            ChatOwner::Remote(node("node_b"))
        );
        on_b.release("chat");
        assert_eq!(on_a.claim("chat").unwrap(), ChatOwner::Local);
    }

    #[test]
    fn a_restarted_daemon_takes_its_chats_back_at_once() {
        let kv = Arc::new(MemoryKv::default());
        let a = node("node_a").to_string();
        assert_eq!(
            ownership(&kv, &a, "before-restart").claim("chat").unwrap(),
            ChatOwner::Local
        );
        let restarted = ownership(&kv, &a, "after-restart");
        assert_eq!(restarted.claim("chat").unwrap(), ChatOwner::Local);
        assert!(!ownership(&kv, &a, "before-restart").renew("chat").unwrap());
    }

    #[test]
    fn origins_are_qualified_by_node_and_resolved_back() {
        let cluster = ChatCluster {
            node_id: Some(node("node_a")),
            ..ChatCluster::standalone()
        };
        let qualified = cluster.qualify_origin("view-1");
        assert_eq!(cluster.local_origin(&qualified), Some("view-1"));
        let other = ChatCluster {
            node_id: Some(node("node_b")),
            ..ChatCluster::standalone()
        };
        assert_eq!(other.local_origin(&qualified), None);
    }
}
