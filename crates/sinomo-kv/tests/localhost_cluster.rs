use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterId, ClusterMemberRecord, NodeId};
use sinomo_kv::{
    InProcessKvStore, KvCommand, KvCompare, KvEntry, KvOp, KvRevision, KvSnapshot, KvTxn, LeaseId,
    cluster_member_key,
};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LocalhostKvRequest {
    Apply { command: KvCommand },
    Get { key: String },
    Snapshot,
    InstallSnapshot { snapshot: KvSnapshot },
    Stop,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum LocalhostKvResponse {
    Revision { revision: KvRevision },
    Entry { entry: Option<KvEntry> },
    Snapshot { snapshot: KvSnapshot },
    Ok,
    Error { message: String },
}

struct LocalhostKvNode {
    address: SocketAddr,
    handle: Option<JoinHandle<()>>,
}

impl LocalhostKvNode {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind localhost kv node");
        let address = listener.local_addr().expect("local addr");
        let store = Arc::new(Mutex::new(InProcessKvStore::new()));
        let handle_store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.expect("accept localhost kv request");
                let request = read_request(&mut stream);
                let should_stop = matches!(request, Ok(LocalhostKvRequest::Stop));
                let response = match request {
                    Ok(request) => handle_request(&handle_store, request),
                    Err(error) => LocalhostKvResponse::Error {
                        message: error.to_string(),
                    },
                };
                write_response(&mut stream, &response);
                if should_stop {
                    break;
                }
            }
        });

        Self {
            address,
            handle: Some(handle),
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn stop(mut self) {
        let _ = send_request(self.address, LocalhostKvRequest::Stop);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("join localhost kv node");
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Result<LocalhostKvRequest, serde_json::Error> {
    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    reader
        .read_line(&mut line)
        .expect("read localhost kv request");
    serde_json::from_str(&line)
}

fn write_response(stream: &mut TcpStream, response: &LocalhostKvResponse) {
    serde_json::to_writer(&mut *stream, response).expect("serialize localhost kv response");
    writeln!(stream).expect("write localhost kv response newline");
}

fn handle_request(
    store: &Arc<Mutex<InProcessKvStore>>,
    request: LocalhostKvRequest,
) -> LocalhostKvResponse {
    let mut store = store.lock().expect("lock kv store");
    match request {
        LocalhostKvRequest::Apply { command } => match store.apply_command(command) {
            Ok(revision) => LocalhostKvResponse::Revision { revision },
            Err(error) => LocalhostKvResponse::Error {
                message: error.to_string(),
            },
        },
        LocalhostKvRequest::Get { key } => LocalhostKvResponse::Entry {
            entry: store.get(&key).cloned(),
        },
        LocalhostKvRequest::Snapshot => LocalhostKvResponse::Snapshot {
            snapshot: store.snapshot(),
        },
        LocalhostKvRequest::InstallSnapshot { snapshot } => {
            *store = InProcessKvStore::restore(snapshot);
            LocalhostKvResponse::Ok
        }
        LocalhostKvRequest::Stop => LocalhostKvResponse::Ok,
    }
}

fn send_request(
    address: SocketAddr,
    request: LocalhostKvRequest,
) -> Result<LocalhostKvResponse, String> {
    let mut stream = TcpStream::connect(address).map_err(|source| source.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|source| source.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|source| source.to_string())?;
    serde_json::to_writer(&mut stream, &request).map_err(|source| source.to_string())?;
    writeln!(stream).map_err(|source| source.to_string())?;

    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .map_err(|source| source.to_string())?;
    let response: LocalhostKvResponse =
        serde_json::from_str(&line).map_err(|source| source.to_string())?;
    if let LocalhostKvResponse::Error { message } = &response {
        return Err(message.clone());
    }
    Ok(response)
}

fn apply_to(address: SocketAddr, command: KvCommand) -> KvRevision {
    match send_request(address, LocalhostKvRequest::Apply { command })
        .expect("apply command over localhost")
    {
        LocalhostKvResponse::Revision { revision } => revision,
        response => panic!("unexpected apply response: {response:?}"),
    }
}

fn replicate_to(addresses: &[SocketAddr], command: KvCommand) -> Vec<KvRevision> {
    addresses
        .iter()
        .map(|address| apply_to(*address, command.clone()))
        .collect()
}

fn get_entry(address: SocketAddr, key: &str) -> Option<KvEntry> {
    match send_request(
        address,
        LocalhostKvRequest::Get {
            key: key.to_string(),
        },
    )
    .expect("get entry over localhost")
    {
        LocalhostKvResponse::Entry { entry } => entry,
        response => panic!("unexpected get response: {response:?}"),
    }
}

fn snapshot(address: SocketAddr) -> KvSnapshot {
    match send_request(address, LocalhostKvRequest::Snapshot).expect("snapshot over localhost") {
        LocalhostKvResponse::Snapshot { snapshot } => snapshot,
        response => panic!("unexpected snapshot response: {response:?}"),
    }
}

fn install_snapshot(address: SocketAddr, snapshot: KvSnapshot) {
    match send_request(address, LocalhostKvRequest::InstallSnapshot { snapshot })
        .expect("install snapshot over localhost")
    {
        LocalhostKvResponse::Ok => {}
        response => panic!("unexpected install snapshot response: {response:?}"),
    }
}

fn wait_for_entry(address: SocketAddr, key: &str, expected: &[u8]) -> KvEntry {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(entry) = get_entry(address, key) {
            if entry.value == expected {
                return entry;
            }
        }
        assert!(
            Instant::now() < deadline,
            "entry {key} did not converge on {address}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn cluster_member(
    cluster_id: ClusterId,
    node_id: NodeId,
    display_name: &str,
) -> ClusterMemberRecord {
    let mut member = ClusterMemberRecord::local_voter(
        cluster_id,
        node_id,
        display_name,
        time::OffsetDateTime::now_utc(),
    );
    member.node_public_key = format!("pk_{display_name}");
    member
}

fn start_three_nodes() -> (Vec<LocalhostKvNode>, Vec<SocketAddr>) {
    let nodes = vec![
        LocalhostKvNode::start(),
        LocalhostKvNode::start(),
        LocalhostKvNode::start(),
    ];
    let addresses = nodes.iter().map(LocalhostKvNode::address).collect();
    (nodes, addresses)
}

fn stop_nodes(nodes: Vec<LocalhostKvNode>) {
    for node in nodes {
        node.stop();
    }
}

#[test]
fn localhost_three_node_cluster_replicates_membership_and_kv_operations() {
    let (nodes, addresses) = start_three_nodes();
    let cluster_id = ClusterId::new("cluster_localhost").expect("cluster");
    let node_ids = [
        NodeId::new("node_alpha").expect("node"),
        NodeId::new("node_beta").expect("node"),
        NodeId::new("node_gamma").expect("node"),
    ];

    for (node_id, display_name) in node_ids.iter().zip(["alpha", "beta", "gamma"]) {
        let member = cluster_member(cluster_id.clone(), node_id.clone(), display_name);
        replicate_to(&addresses, KvCommand::PutClusterMember(member));
    }
    replicate_to(
        &addresses,
        KvCommand::Op(KvOp::LeaseGrant {
            lease_id: LeaseId(1),
            ttl_ticks: 10,
        }),
    );
    replicate_to(
        &addresses,
        KvCommand::Op(KvOp::Put {
            key: "/services/kv/node_alpha".to_string(),
            value: addresses[0].to_string().into_bytes(),
            lease_id: Some(LeaseId(1)),
        }),
    );
    let revisions = replicate_to(
        &addresses,
        KvCommand::Txn(KvTxn {
            compares: vec![KvCompare::Missing {
                key: "/config/epoch".to_string(),
            }],
            success: vec![KvOp::Put {
                key: "/config/epoch".to_string(),
                value: b"1".to_vec(),
                lease_id: None,
            }],
            failure: vec![],
        }),
    );

    assert_eq!(revisions, vec![KvRevision(5), KvRevision(5), KvRevision(5)]);
    for address in &addresses {
        for node_id in &node_ids {
            assert!(
                get_entry(*address, &cluster_member_key(node_id)).is_some(),
                "missing member {node_id} on {address}"
            );
        }
        assert_eq!(
            get_entry(*address, "/config/epoch").expect("epoch").value,
            b"1"
        );
        assert_eq!(
            get_entry(*address, "/services/kv/node_alpha")
                .expect("service")
                .lease_id,
            Some(LeaseId(1))
        );
    }

    stop_nodes(nodes);
}

#[test]
fn localhost_three_node_cluster_catches_up_delayed_node_from_snapshot() {
    let (nodes, addresses) = start_three_nodes();
    let first_two = &addresses[..2];

    replicate_to(
        first_two,
        KvCommand::Op(KvOp::Put {
            key: "/config/a".to_string(),
            value: b"one".to_vec(),
            lease_id: None,
        }),
    );
    replicate_to(
        first_two,
        KvCommand::Op(KvOp::Put {
            key: "/config/b".to_string(),
            value: b"two".to_vec(),
            lease_id: None,
        }),
    );

    assert!(get_entry(addresses[2], "/config/b").is_none());
    install_snapshot(addresses[2], snapshot(addresses[0]));
    wait_for_entry(addresses[2], "/config/b", b"two");

    replicate_to(
        &addresses,
        KvCommand::Op(KvOp::Put {
            key: "/config/c".to_string(),
            value: b"three".to_vec(),
            lease_id: None,
        }),
    );

    let snapshots: Vec<KvSnapshot> = addresses.iter().map(|address| snapshot(*address)).collect();
    assert!(
        snapshots
            .iter()
            .all(|snapshot| snapshot.revision == KvRevision(3))
    );
    for address in &addresses {
        assert_eq!(wait_for_entry(*address, "/config/a", b"one").value, b"one");
        assert_eq!(wait_for_entry(*address, "/config/b", b"two").value, b"two");
        assert_eq!(
            wait_for_entry(*address, "/config/c", b"three").value,
            b"three"
        );
    }

    stop_nodes(nodes);
}
