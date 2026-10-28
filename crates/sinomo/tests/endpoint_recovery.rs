//! Isolated native process qualification. No model calls or production DATA.
#![cfg(target_os = "linux")]

use sinomo_runtime::{
    api::{
        DaemonApiClient, DaemonPaths, DaemonProbeStatus, DaemonStatus,
        SINOMO_APP_DAEMON_LAUNCH_ID_ENV, probe_daemon_status,
    },
    cluster::{
        self, ClusterControlInitRequest, ClusterControlInviteRequest, ClusterControlJoinRequest,
        ClusterControlStateAuthorityRequest, EndpointRecoveryAction, EndpointRecoveryOutcome,
        EndpointRecoveryRequest, EndpointRecoverySpec,
    },
    core::{
        ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterTransportKind, Id,
    },
    kv::{self, KvAuthoritativeRead, KvSnapshot},
    local::SinomoDataDir,
    network::{self, ClusterPaths, ObjectRpcRequest},
};
use std::{
    fs::{self, File},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn candidate_binary() -> String {
    std::env::var("SINOMO_ENDPOINT_RECOVERY_CANDIDATE_BINARY")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_sinomo").to_owned())
}

struct Node {
    child: Child,
    status: DaemonStatus,
    data: SinomoDataDir,
}
impl Node {
    fn start(data: &SinomoDataDir, port: u16, phase: &str) -> Self {
        let launch = format!("endpoint-recovery-{}-{phase}", std::process::id());
        let bind = if data
            .root()
            .file_name()
            .is_some_and(|name| name == "learner")
        {
            "127.0.0.1"
        } else {
            "0.0.0.0"
        };
        let peer = data
            .root()
            .file_name()
            .is_some_and(|name| name != "learner");
        let binary = if data.root().file_name().is_some_and(|name| name == "seed") {
            std::env::var("SINOMO_ENDPOINT_RECOVERY_SEED_BINARY")
                .or_else(|_| std::env::var("SINOMO_ENDPOINT_RECOVERY_PEER_BINARY"))
                .unwrap_or_else(|_| candidate_binary())
        } else if peer {
            std::env::var("SINOMO_ENDPOINT_RECOVERY_PEER_BINARY")
                .unwrap_or_else(|_| candidate_binary())
        } else {
            candidate_binary()
        };
        let mut child = Command::new(&binary)
            .args(["daemon", "run", "--app-owned", "--host", bind, "--port"])
            .arg(port.to_string())
            .env("SINOMO_DATA_DIR", data.root())
            .env(SINOMO_APP_DAEMON_LAUNCH_ID_ENV, &launch)
            .stdin(Stdio::piped())
            .stdout(File::create(data.root().join(format!("{phase}.stdout"))).unwrap())
            .stderr(File::create(data.root().join(format!("{phase}.stderr"))).unwrap())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let DaemonProbeStatus::Running(status) =
                probe_daemon_status(&DaemonPaths::new(data), Duration::from_millis(300))
            {
                let node = Self {
                    child,
                    status,
                    data: data.clone(),
                };
                let status = &node.status;
                assert_eq!(status.pid, node.child.id());
                assert_eq!(status.app_launch_id.as_deref(), Some(launch.as_str()));
                assert_eq!(
                    fs::canonicalize(&status.data_dir).unwrap(),
                    fs::canonicalize(data.root()).unwrap()
                );
                assert_eq!(
                    fs::read_link(format!("/proc/{}/exe", status.pid)).unwrap(),
                    fs::canonicalize(&binary).unwrap()
                );
                eprintln!(
                    "native-endpoint-process {}",
                    serde_json::json!({"phase":phase,"pid":status.pid,"start":status.started_at,"node":status.cluster.as_ref().map(|c|&c.node_id),"data":status.data_dir,"binary":binary,"commit":status.commit})
                );
                return node;
            }
            if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "native startup failed: {} phase={phase}",
                    data.root().display()
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn client(&self) -> DaemonApiClient {
        DaemonApiClient::at_address(&self.status.address).with_timeout(Duration::from_secs(15))
    }
    fn wait_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                assert!(s.success(), "native child exit {s}");
                return;
            }
            assert!(Instant::now() < deadline, "owned child failed to drain");
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    fn stop(&mut self) {
        drop(self.child.stdin.take());
        self.wait_exit();
    }
    fn snapshot(&self) -> Result<KvAuthoritativeRead<KvSnapshot>, String> {
        let p = kv::cluster_kv_authoritative_snapshot_rpc_payload().unwrap();
        let rsp = self
            .client()
            .call_request(&ObjectRpcRequest::from_encoded_body(
                p.route, p.category, p.body,
            ))
            .map_err(|e| e.to_string())?;
        kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&rsp.body).map_err(|e| e.to_string())
    }
    fn authority(&self) -> KvAuthoritativeRead<KvSnapshot> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.snapshot() {
                Ok(s) => return s,
                Err(e) => {
                    assert!(
                        !e.contains("unauthorized") && !e.contains("auth denied"),
                        "native setup denied: {e}"
                    );
                    assert!(Instant::now() < deadline, "native authority failed: {e}");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            drop(self.child.stdin.take());
            let end = Instant::now() + Duration::from_secs(5);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < end {
                std::thread::sleep(Duration::from_millis(25));
            }
            if self.child.try_wait().ok().flatten().is_none() {
                eprintln!("native-endpoint-forced-cleanup pid={}", self.child.id());
                let _ = self.child.kill();
            }
        }
        let _ = self.child.wait();
    }
}
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn data(root: &Path, name: &str) -> SinomoDataDir {
    let d = SinomoDataDir::new(root.join(name));
    d.ensure_root_layout().unwrap();
    d
}
fn raw(n: &Node) -> ClusterBootstrapState {
    cluster::read_cluster_state_without_kv(&ClusterPaths::new(&n.data)).unwrap()
}
fn member(s: &KvSnapshot, id: &Id) -> ClusterMemberRecord {
    s.get(&sinomo_runtime::node::cluster_member_key(id))
        .unwrap()
        .decode()
        .unwrap()
}
fn published_endpoint(n: &Node) -> sinomo_runtime::core::ClusterEndpointRecord {
    let s = n.authority();
    let m = member(&s.value, &n.status.cluster.as_ref().unwrap().node_id);
    m.endpoints
        .into_iter()
        .find(|e| {
            e.transport == ClusterTransportKind::LanTcp
                && e.address.ends_with(&format!(":{}", n.status.port))
                && network::cluster_endpoint_can_carry_main_daemon_rpc(e)
        })
        .expect("published native main TCP endpoint")
}
fn join(seed: &Node, d: &SinomoDataDir, port: u16, label: &str) -> (Node, String) {
    let endpoint = published_endpoint(seed);
    let invite = seed
        .client()
        .cluster_invite_with_request(ClusterControlInviteRequest {
            ttl_seconds: Some(300),
            introducers: vec![endpoint.address],
        })
        .unwrap();
    let mut n = Node::start(d, port, &format!("{label}-join"));
    n.client()
        .cluster_join(ClusterControlJoinRequest {
            invite: invite.invite.clone(),
            node_display_name: Some(label.into()),
            node_role: None,
        })
        .unwrap();
    n.wait_exit();
    drop(n);
    (
        Node::start(d, port, &format!("{label}-joined")),
        invite.invite,
    )
}

fn capture_timeline(node: &Node, label: &str, root: &Path, start: bool) {
    use sinomo_runtime::observe::*;
    let request = ObserveNetworkTimelineRequest {
        action: if start {
            ObserveNetworkTimelineAction::Start
        } else {
            ObserveNetworkTimelineAction::Stop
        },
        capture_id: Some("endpoint-recovery".into()),
        duration_seconds: start.then_some(60),
        max_events: start.then_some(32768),
    };
    let call = ObjectRpcRequest::encode_body(
        OBSERVE_NETWORK_TIMELINE_ROUTE,
        OBSERVE_RPC_CATEGORY,
        &request,
    )
    .unwrap();
    let result = node.client().call_request(&call).and_then(|r| {
        r.decode_body::<ObserveNetworkTimelineResponse>()
            .map_err(Into::into)
    });
    let value = match result {
        Ok(r) => {
            assert_eq!(r.process_id, node.child.id());
            serde_json::to_value(r).unwrap()
        }
        Err(e) => serde_json::json!({"error":e.to_string()}),
    };
    fs::write(
        root.join(format!(
            "{label}-timeline-{}.json",
            if start { "start" } else { "stop" }
        )),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
}

fn record_scope_views(node: &Node, label: &str, root: &Path) {
    let raw = raw(node);
    let proof = node.authority();
    let mut projected = raw.clone();
    projected.members = cluster::read_cluster_members_from_snapshot(&proof.value).unwrap();
    let a = network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&raw).unwrap();
    let b =
        network::cluster_routed_stream_pool_scopes_from_fenced_bootstrap_state(&projected).unwrap();
    fs::write(root.join(format!("{label}-scope-views.json")),serde_json::to_vec_pretty(&serde_json::json!({"node":raw.local_node_id,"raw_scope":a,"projected_scope":b,"authority":proof.authority})).unwrap()).unwrap();
}

fn native_case(root: &Path) {
    let seed_data = data(root, "seed");
    let seed_port = port();
    let mut first = Node::start(&seed_data, seed_port, "seed-init");
    first
        .client()
        .cluster_init(ClusterControlInitRequest {
            cluster_name: "endpoint recovery native".into(),
            node_display_name: Some("seed".into()),
            node_role: None,
        })
        .unwrap();
    first.wait_exit();
    drop(first);
    let mut seed = Node::start(&seed_data, seed_port, "seed");
    seed.authority();
    let second_data = data(root, "second");
    let (mut second, shared_invite) = join(&seed, &second_data, port(), "second");
    second.authority();
    let learner_data = data(root, "learner");
    let learner_port = port();
    // Stop at the accepted identity transition: no new-identity process has
    // opened local Raft. Reproduce the retained projection missing seed routes.
    let endpoint = published_endpoint(&seed);
    // The accepting voter retains a real main TCP candidate for this learner,
    // but inbound LAN dialing cannot reach its loopback-only listener. The
    // authenticated outbound carrier must supply the ordinary reverse path.
    let advertised = std::net::SocketAddr::new(
        endpoint
            .address
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .ip(),
        learner_port,
    );
    let config_dir = learner_data.root().join("configs/cluster");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("endpoints.json"),
        serde_json::to_vec(
            &serde_json::json!({"main":[{"address":format!("tcp://{advertised}"),"scope":"lan"}]}),
        )
        .unwrap(),
    )
    .unwrap();
    // Portable invitations remain active for multiple accepted members. Reuse
    // the verified invitation instead of exercising unrelated repeated invite
    // publication in this endpoint-recovery fixture.
    let invite_id = cluster::decode_cluster_join_invite(&shared_invite)
        .unwrap()
        .invite_id;
    let invite_proof = seed.authority();
    assert!(
        invite_proof
            .value
            .get(&sinomo_runtime::node::cluster_invitation_key(&invite_id))
            .is_some(),
        "shared invitation must remain authoritatively present"
    );
    eprintln!(
        "native-shared-invitation id={} revision={:?}",
        invite_id, invite_proof.authority.revision
    );
    let mut joining = Node::start(&learner_data, learner_port, "learner-join");
    joining
        .client()
        .cluster_join(ClusterControlJoinRequest {
            invite: shared_invite,
            node_display_name: Some("learner".into()),
            node_role: None,
        })
        .unwrap();
    joining.wait_exit();
    drop(joining);
    // Establish the actual two-voter quorum after both supported joins.
    seed.client()
        .cluster_state_authority(ClusterControlStateAuthorityRequest {
            node_id: second.status.cluster.as_ref().unwrap().node_id.clone(),
            role: ClusterMemberRole::Voter,
        })
        .unwrap();
    let quorum = seed.authority();
    assert!(matches!(
        quorum.authority.basis,
        kv::KvAuthorityBasis::CurrentTermQuorum {
            voter_acks: 2,
            required_quorum: 2
        }
    ));
    // Match the rollout gate: actual current-term quorum and voter identity
    // must remain stable before an existing learner's recovery is attempted.
    let mut baseline = None;
    for sample in 0..6 {
        if sample > 0 {
            std::thread::sleep(Duration::from_secs(1));
        }
        let proof = seed.snapshot().expect("stable voter preflight");
        let config: kv::ClusterKvRaftConfigRecord = proof
            .value
            .get(&kv::cluster_kv_raft_config_key())
            .unwrap()
            .decode()
            .unwrap();
        assert!(matches!(
            proof.authority.basis,
            kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2
            }
        ));
        let current = (proof.authority.term, config.voters);
        if let Some(expected) = &baseline {
            assert_eq!(expected, &current);
        } else {
            baseline = Some(current);
        }
        eprintln!(
            "native-stable-quorum sample={sample} authority={:?}",
            proof.authority
        );
    }
    let paths = ClusterPaths::new(&learner_data);
    let mut lost = cluster::read_cluster_state_without_kv(&paths).unwrap();
    let seed_id = seed.status.cluster.as_ref().unwrap().node_id.clone();
    let local_id = lost.local_node_id.clone();
    for m in &mut lost.members {
        if m.node_id != local_id {
            m.endpoints.clear();
        }
    }
    // This is fixture setup on the stopped private learner's observational
    // projection only. The real voters' committed membership/keys remain intact.
    cluster::write_cluster_state(&paths, &lost).unwrap();
    network::write_object_file(
        &paths.connectivity_local,
        &Vec::<network::ClusterConnectivityRecord>::new(),
    )
    .unwrap();
    let actual_snapshot = kv::ClusterKvPaths::new(&paths.root).snapshot;
    if actual_snapshot.exists() {
        fs::copy(
            &actual_snapshot,
            learner_data
                .root()
                .join("fixture-original-materialized-snapshot.sobj"),
        )
        .unwrap();
        let kv_paths = kv::ClusterKvPaths::new(&paths.root);
        let mut snapshot = kv::read_cluster_kv_cached_snapshot(&kv_paths).unwrap();
        let authority_before = snapshot
            .entries
            .iter()
            .filter(|e| {
                e.key.starts_with("/cluster/config/") || e.key.starts_with("/cluster/policy/")
            })
            .cloned()
            .collect::<Vec<_>>();
        let local_prefix = format!("/cluster/endpoints/{local_id}/");
        snapshot.entries.retain(|e| {
            !e.key.starts_with("/cluster/network/")
                && (!e.key.starts_with("/cluster/endpoints/") || e.key.starts_with(&local_prefix))
        });
        for e in &mut snapshot.entries {
            if e.key.starts_with("/cluster/members/") {
                let mut member: ClusterMemberRecord = e.decode().unwrap();
                if member.node_id != local_id {
                    member.endpoints.clear();
                    e.value = kv::encode_kv_value(&member).unwrap();
                }
            }
        }
        kv::write_cluster_kv_materialized_snapshot_if_changed(&kv_paths, &snapshot).unwrap();
        let verified = kv::read_cluster_kv_cached_snapshot(&kv_paths).unwrap();
        assert_eq!(
            verified
                .entries
                .iter()
                .filter(|e| e.key.starts_with("/cluster/config/")
                    || e.key.starts_with("/cluster/policy/"))
                .cloned()
                .collect::<Vec<_>>(),
            authority_before
        );
        assert!(
            cluster::read_cluster_members_from_snapshot(&verified)
                .unwrap()
                .iter()
                .filter(|m| m.node_id != local_id)
                .all(|m| m.endpoints.is_empty())
        );
    }
    let stopped: ClusterBootstrapState = network::read_object_file(&paths.state).unwrap();
    assert!(
        stopped
            .members
            .iter()
            .filter(|m| m.node_id != local_id)
            .all(|m| m.endpoints.is_empty())
    );
    let mut learner = Node::start(&learner_data, learner_port, "learner-lost-route");
    let diagnostics = std::env::var_os("SINOMO_ENDPOINT_RECOVERY_DIAGNOSTICS").is_some();
    if diagnostics {
        capture_timeline(&seed, "seed", root, true);
        capture_timeline(&learner, "learner", root, true);
        record_scope_views(&seed, "seed-before", root);
    }
    let state = raw(&learner);
    eprintln!(
        "native-lost-route-observation {}",
        serde_json::to_string(
            &state
                .members
                .iter()
                .map(|m| (&m.node_id, &m.endpoints))
                .collect::<Vec<_>>()
        )
        .unwrap()
    );
    assert!(
        state
            .members
            .iter()
            .filter(|m| m.node_id != local_id)
            .all(|m| m.endpoints.is_empty()),
        "fixture must preserve the actual missing-route precondition"
    );
    let failed_before = learner
        .snapshot()
        .expect_err("lost route unexpectedly reached authority");
    eprintln!("native-endpoint-failed-before {failed_before}");
    let peer = state.members.iter().find(|m| m.node_id == seed_id).unwrap();
    let mut request = EndpointRecoveryRequest {
        spec: EndpointRecoverySpec {
            operation_id: Id::generate(),
            cluster_id: state.cluster_id.clone(),
            local_node_id: local_id.clone(),
            peer_node_id: seed_id.clone(),
            peer_public_key: peer.node_public_key.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            address: endpoint.address.clone(),
            expected_state_digest: cluster::endpoint_recovery_state_digest(&state).unwrap(),
        },
        action: EndpointRecoveryAction::Status,
    };
    let before = learner
        .client()
        .cluster_recover_endpoint(request.clone())
        .unwrap();
    assert_eq!(before.outcome, EndpointRecoveryOutcome::NotSubmitted);
    // Wrong-key negative uses the real local control boundary, before dial/write.
    let mut wrong = request.clone();
    wrong.action = EndpointRecoveryAction::Apply;
    wrong.spec.peer_public_key = "wrong-native-key".into();
    assert!(learner.client().cluster_recover_endpoint(wrong).is_err());
    assert_eq!(raw(&learner), state);
    // A different real receiver cannot answer for the pinned seed identity.
    let mut wrong_receiver = request.clone();
    wrong_receiver.action = EndpointRecoveryAction::Apply;
    wrong_receiver.spec.address = published_endpoint(&second).address;
    assert!(
        learner
            .client()
            .cluster_recover_endpoint(wrong_receiver)
            .is_err()
    );
    assert_eq!(raw(&learner), state);
    request.action = EndpointRecoveryAction::Apply;
    let request_file = root.join("endpoint-request.json");
    let request_bytes = serde_json::to_vec_pretty(&request.spec).unwrap();
    fs::write(&request_file, &request_bytes).unwrap();
    let command = Command::new(candidate_binary())
        .args(["cluster", "recover-endpoint", "apply"])
        .arg(&request_file)
        .env("SINOMO_DATA_DIR", learner.data.root())
        .output()
        .unwrap();
    eprintln!(
        "native-cli-apply exit={} operation={} stderr={}",
        command.status,
        request.spec.operation_id,
        String::from_utf8_lossy(&command.stderr)
    );
    assert!(command.status.success());
    let applied: cluster::EndpointRecoveryResponse =
        serde_json::from_slice(&command.stdout).unwrap();
    assert_eq!(applied.outcome, EndpointRecoveryOutcome::Applied);
    let repeated = learner
        .client()
        .cluster_recover_endpoint(request.clone())
        .unwrap();
    assert_eq!(repeated.outcome, EndpointRecoveryOutcome::Applied);
    eprintln!(
        "native-endpoint-applied-boundary {}",
        serde_json::json!({"operation":request.spec.operation_id,"applied":applied,"repeated":repeated})
    );
    let payload = kv::cluster_kv_authoritative_snapshot_rpc_payload().unwrap();
    let routed = learner
        .client()
        .object_rpc_to_target_from_data_dir_with_affinity(
            &learner.data,
            &seed_id,
            ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body),
            Duration::from_secs(10),
            sinomo_runtime::api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
        );
    eprintln!("native-ordinary-route error={:?}", routed.as_ref().err());
    let routed = kv::decode_cluster_kv_authoritative_snapshot_rpc_body(
        &routed
            .expect("recovered ordinary Main route must work")
            .body,
    )
    .unwrap();
    eprintln!("native-ordinary-route-authority {:?}", routed.authority);
    if std::env::var_os("SINOMO_ENDPOINT_RECOVERY_PEER_REFRESH").is_some() {
        // Endpoint recovery is local. Explicitly reconcile the established leader's
        // existing network projections so its normal reachability-owned catch-up
        // can use the newly authenticated reverse carrier; no promotion is replayed.
        let refresh = ObjectRpcRequest::encode_body(
            sinomo_runtime::api::DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
            sinomo_runtime::api::DAEMON_RPC_CATEGORY,
            &sinomo_runtime::api::DaemonClusterMaintenanceRefreshRequest,
        )
        .unwrap()
        .with_operation_id(format!("{}:peer-refresh", request.spec.operation_id));
        eprintln!(
            "native-peer-refresh target={} operation={}",
            seed_id, request.spec.operation_id
        );
        seed.client()
            .call_request(&refresh)
            .expect("existing local peer maintenance refresh");
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| learner.authority()));
    if diagnostics {
        if result.is_err() {
            // A separately identified ordinary write is a diagnostic of normal
            // fanout, never a replacement verdict for the failed idle check.
            let key = format!(
                "/verification/endpoint-recovery/{}",
                request.spec.operation_id
            );
            let txn = kv::KvTxnRequest::new(
                kv::KvRequestId::derived(
                    "endpoint-recovery-canary",
                    request.spec.operation_id.as_str(),
                ),
                kv::KvTxn {
                    compares: vec![kv::KvCompare::Missing { key: key.clone() }],
                    success: vec![kv::KvOp::Put {
                        key: key.clone(),
                        value: b"isolated endpoint canary".to_vec(),
                        lease_id: None,
                    }],
                    failure: vec![],
                },
            );
            fs::write(
                root.join("diagnostic-canary-request.json"),
                serde_json::to_vec_pretty(&txn).unwrap(),
            )
            .unwrap();
            let payload = kv::cluster_kv_txn_rpc_payload(&txn).unwrap();
            let response = seed.client().call_request(
                &ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body)
                    .with_operation_id(txn.request_id.as_str()),
            );
            let value = match response {
                Ok(r) => serde_json::to_value(kv::decode_cluster_kv_txn_rpc_body(&r.body).unwrap())
                    .unwrap(),
                Err(e) => serde_json::json!({"error":e.to_string(),"request_id":txn.request_id}),
            };
            fs::write(
                root.join("diagnostic-canary-response.json"),
                serde_json::to_vec_pretty(&value).unwrap(),
            )
            .unwrap();
            std::thread::sleep(Duration::from_secs(3));
            fs::write(root.join("diagnostic-learner-after-canary.json"),serde_json::to_vec_pretty(&match learner.snapshot(){Ok(r)=>serde_json::json!({"authority":r.authority,"canary_present":r.value.get(&key).is_some()}),Err(e)=>serde_json::json!({"error":e})}).unwrap()).unwrap();
        }
        record_scope_views(&seed, "seed-after", root);
        capture_timeline(&seed, "seed", root, false);
        capture_timeline(&learner, "learner", root, false);
        for (label, node) in [("seed", &seed), ("second", &second), ("learner", &learner)] {
            fs::write(
                root.join(format!("{label}-final-status.json")),
                serde_json::to_vec_pretty(&node.client().status().unwrap()).unwrap(),
            )
            .unwrap();
        }
    }
    let after = match result {
        Ok(r) => r,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(after.authority.revision.0 > 0);
    assert!(matches!(
        after.authority.basis,
        kv::KvAuthorityBasis::CurrentTermQuorum {
            voter_acks: 2,
            required_quorum: 2
        }
    ));
    assert_eq!(learner.status.cluster.as_ref().unwrap().node_id, local_id);
    assert_eq!(
        member(&after.value, &local_id).node_public_key,
        member(&seed.authority().value, &local_id).node_public_key
    );
    // A genuine authenticated remote voter still has no local recovery grant.
    let runtime = tokio_runtime();
    let remote_state = raw(&second);
    let mut forbidden = request.clone();
    forbidden.action = EndpointRecoveryAction::Apply;
    let control_endpoint = endpoint.clone();
    let remote = runtime.block_on(async {
        let paths = ClusterPaths::new(&second.data);
        let mut host =
            sinomo_runtime::daemon::DaemonObjectRpcAsyncHost::new(runtime.handle(), false)
                .with_paths(&DaemonPaths::new(&second.data));
        let mut client = network::ClusterObjectRpcClient::routed_from_fenced_bootstrap_state(
            &mut host,
            &paths,
            &remote_state,
        )
        .unwrap();
        let call = ObjectRpcRequest::encode_body(
            cluster::CLUSTER_CONTROL_RECOVER_ENDPOINT_ROUTE,
            cluster::CLUSTER_CONTROL_RPC_CATEGORY,
            &forbidden,
        )
        .unwrap();
        client
            .call_endpoint(
                &remote_state,
                &control_endpoint,
                network::ObjectRpcCall::value(call),
                network::ClusterObjectRpcOptions::new(
                    network::DaemonRpcTimeouts::connect_and_idle(Duration::from_secs(5)),
                )
                .with_overall_timeout(Duration::from_secs(5)),
            )
            .await
    });
    eprintln!("native-remote-negative result={:?}", remote.as_ref().err());
    assert!(
        matches!(remote, Err(network::NetworkError::AuthDenied)),
        "authenticated remote recovery must fail local-only admission: {:?}",
        remote.as_ref().err()
    );
    eprintln!(
        "native-endpoint-recovery {}",
        serde_json::json!({"operation":request.spec.operation_id,"before":before,"applied":applied,"repeat":repeated,"authority":after.authority,"cluster":state.cluster_id,"node":local_id,"seed":seed_id})
    );
    let rollback = Command::new(candidate_binary())
        .args(["cluster", "recover-endpoint", "rollback"])
        .arg(&request_file)
        .env("SINOMO_DATA_DIR", learner.data.root())
        .output()
        .unwrap();
    assert!(!rollback.status.success());
    assert!(String::from_utf8_lossy(&rollback.stderr).contains("rollback conflicts"));
    assert_eq!(fs::read(&request_file).unwrap(), request_bytes);
    eprintln!(
        "native-rollback-conflict {}",
        String::from_utf8_lossy(&rollback.stderr)
    );
    assert!(learner.authority().authority.revision >= after.authority.revision);
    learner.stop();
    drop(learner);
    let mut restarted = Node::start(&learner_data, learner_port, "learner-restarted");
    assert_eq!(restarted.status.cluster.as_ref().unwrap().node_id, local_id);
    restarted.authority();
    request.action = EndpointRecoveryAction::Status;
    assert_eq!(
        restarted
            .client()
            .cluster_recover_endpoint(request)
            .unwrap()
            .outcome,
        EndpointRecoveryOutcome::Applied
    );
    restarted.stop();
    second.stop();
    seed.stop();
}

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn native_lost_endpoint_recovery_preserves_identity_and_restores_current_term_catchup() {
    if let Ok(root) = std::env::var("SINOMO_ENDPOINT_RECOVERY_CHILD_ROOT") {
        native_case(Path::new(&root));
        return;
    }
    let root = sinomo_testdir::TestDir::new("native-endpoint-recovery").keep();
    eprintln!("native-endpoint-retained-root={}", root.display());
    let operator = data(&root, "operator");
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "native_lost_endpoint_recovery_preserves_identity_and_restores_current_term_catchup",
            "--nocapture",
        ])
        .env("SINOMO_ENDPOINT_RECOVERY_CHILD_ROOT", &root)
        .env("SINOMO_DATA_DIR", operator.root())
        .status()
        .unwrap();
    assert!(
        status.success(),
        "isolated native endpoint recovery failed; root={}",
        root.display()
    );
}
