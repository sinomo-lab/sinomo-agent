//! Owner-side metadata contribution fences across real routed namespace moves.
use super::*;

type Service = NetworkedDfsService<sinomo_core::network::InProcessTransport>;

struct ContributionCluster {
    _root: sinomo_testdir::TestDir,
    nodes: [NodeId; 3],
    services: Vec<Option<Service>>,
}

impl ContributionCluster {
    fn new(label: &str) -> Self {
        let root = temp_root(label);
        let nodes = ["users", "services", "requester"]
            .map(|suffix| NodeId::from_legacy_named(format!("{label}-{suffix}")).unwrap());
        let network = InProcessNetwork::new();
        let genesis = networked_sifs_with_backend(&root.join("genesis"), nodes[0].clone());
        let backend = genesis.metadata_backend().unwrap();
        let file_system = backend
            .get_file_system(&sifs_file_system_id())
            .unwrap()
            .unwrap();
        let page = backend
            .objects_for_file_system_page(&sifs_file_system_id(), None, 128)
            .unwrap();
        assert!(page.next_cursor.is_none());
        assert!(
            page.objects
                .iter()
                .all(|object| object.kind == DfsObjectKind::Directory)
        );
        let mut shards =
            sifs_two_primary_shard_configs(&nodes[0], &nodes[1], 1, Vec::new(), Vec::new());
        for shard in &mut shards {
            shard.namespace_prepare_codec = DfsMetadataNamespacePrepareCodec::PackedV6;
        }
        assert!(shards.iter().all(|shard| shard.replica_node_ids.is_empty()));
        let mut kv = InProcessKvStore::new();
        let cluster_id = sinomo_core::Id::from_legacy_named(label).unwrap();
        for node_id in &nodes {
            let mut member = sinomo_core::ClusterMemberRecord::local_non_voting(
                cluster_id.clone(),
                node_id.clone(),
                "contribution authority fixture",
                format!("{node_id}-key"),
                OffsetDateTime::UNIX_EPOCH,
            );
            member.state = sinomo_core::ClusterMemberState::Active;
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
            kv.put_serde(
                sinomo_core::node::cluster_member_key(node_id),
                &member,
                None,
            )
            .unwrap();
            kv.put_serde(
                sinomo_core::node::cluster_capability_policy_key(node_id),
                &sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member),
                None,
            )
            .unwrap();
        }
        let control_plane = SharedDfsControlPlaneKvClient::new(kv);
        control_plane.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&file_system))
                .unwrap();
            for shard in &shards {
                control.put_metadata_shard(shard).unwrap();
            }
        });
        let services = nodes
            .iter()
            .enumerate()
            .map(|(index, node_id)| {
                let node_root = root.join(format!("node-{index}"));
                let mut backend =
                    open_local_sharded_metadata_backend(node_root.join("metadata"), shards.clone())
                        .unwrap();
                backend.put_file_system(&file_system).unwrap();
                // Seed only the bootstrap directories. Files, moves and
                // contributions all use the normal service authority paths.
                for directory in &page.objects {
                    backend.put_object(directory).unwrap();
                }
                Some(
                    NetworkedDfsService::new(
                        network.add_node(node_id.clone()),
                        LocalContentStore::new(node_root.join("content")).unwrap(),
                    )
                    .with_control_plane(control_plane.clone())
                    .with_metadata_backend(backend)
                    .unwrap(),
                )
            })
            .collect();
        let mut cluster = Self {
            _root: root,
            nodes,
            services,
        };
        cluster.run(2, |requester| {
            requester.bootstrap_distributed_sifs().unwrap()
        });
        cluster
    }

    fn run<R>(&mut self, active: usize, operation: impl FnOnce(&mut Service) -> R) -> R {
        let mut service = self.services[active].take().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let pumps = self
            .services
            .iter_mut()
            .enumerate()
            .filter_map(|(index, service)| {
                service.take().map(|service| {
                    let stop = stop.clone();
                    (
                        index,
                        std::thread::spawn(move || pump_sifs_service_until_stop(service, stop)),
                    )
                })
            })
            .collect::<Vec<_>>();
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&mut service)));
        stop.store(true, Ordering::SeqCst);
        self.services[active] = Some(service);
        for (index, pump) in pumps {
            self.services[index] = Some(pump.join().unwrap());
        }
        match outcome {
            Ok(outcome) => outcome,
            Err(error) => std::panic::resume_unwind(error),
        }
    }

    fn write(&mut self, path: &str) -> crate::posix_model::DfsStat {
        let owner = self.nodes[0].clone();
        self.run(2, |requester| {
            requester
                .posix_write_file(
                    path,
                    b"contribution source",
                    DfsWriteOptions {
                        policy: DfsReplicationPolicy::new(1, 1).unwrap(),
                        replica_candidates: vec![owner],
                        wait_for_commit: true,
                        foreground_replica_repair: false,
                    },
                )
                .unwrap()
        })
    }

    fn version(&mut self, owner: usize, identity: &DfsRevisionReference) -> DfsObjectVersionRecord {
        self.run(owner, |service| {
            service
                .metadata_backend()
                .unwrap()
                .get_version(&identity.object_id, &identity.version)
                .unwrap()
                .unwrap()
        })
    }
}

fn value(generation: u32) -> DfsModuleMetadataValue {
    DfsModuleMetadataValue {
        schema_version: 1,
        payload: serde_json::json!({"generation":generation}),
    }
}

fn generated_operation() -> (String, OffsetDateTime) {
    let operation = sinomo_core::Id::generate();
    let created_at = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(operation.timestamp_millis()) * 1_000_000,
    )
    .unwrap();
    (operation.to_string(), created_at)
}

fn request(
    identity: DfsRevisionReference,
    head: &crate::posix_model::DfsStat,
    owner: NodeId,
    shard: &str,
    expected: Option<DfsModuleMetadataValue>,
    metadata: Option<DfsModuleMetadataValue>,
) -> DfsAtomicContribution {
    let (operation_id, created_at) = generated_operation();
    DfsAtomicContribution {
        namespace: "org.example.contribution-authority".into(),
        file_system_id: identity.file_system_id.clone(),
        operation_id,
        created_at,
        expected_heads: vec![DfsContributionHeadExpectation {
            object_id: head.object_id.clone(),
            version: head.version.clone(),
            path: head.path.clone(),
        }],
        authority: Some(DfsContributionAuthorityExpectation {
            shard_id: DfsMetadataShardId::new(shard),
            primary_node_id: owner,
            owner_epoch: 1,
        }),
        updates: vec![DfsRevisionMetadataContribution {
            identity,
            expected_metadata: expected,
            metadata,
        }],
    }
}

#[test]
fn distributed_contribution_checks_authority_and_replays_before_stale_head() {
    let mut cluster = ContributionCluster::new("contribution-authority-proof");
    let head = cluster.write("/users/contribution-source");
    let identity = DfsRevisionReference {
        file_system_id: head.file_system_id.clone(),
        object_id: head.object_id.clone(),
        version: head.version.clone(),
    };
    let original = request(
        identity.clone(),
        &head,
        cluster.nodes[0].clone(),
        "users",
        None,
        Some(value(1)),
    );
    cluster.run(0, |owner| {
        owner.apply_atomic_contribution(original.clone()).unwrap()
    });
    let mut update = original.clone();
    (update.operation_id, update.created_at) = generated_operation();
    update.updates[0].expected_metadata = Some(value(1));
    update.updates[0].metadata = Some(value(2));
    cluster.run(0, |owner| {
        owner.apply_atomic_contribution(update.clone()).unwrap()
    });
    let before = cluster.version(0, &identity);
    assert_eq!(
        before.module_metadata.get(&original.namespace),
        Some(&value(2))
    );
    for altered_authority in [
        None,
        Some(DfsContributionAuthorityExpectation {
            primary_node_id: cluster.nodes[1].clone(),
            ..original.authority.clone().unwrap()
        }),
        Some(DfsContributionAuthorityExpectation {
            owner_epoch: 2,
            ..original.authority.clone().unwrap()
        }),
    ] {
        let mut changed = update.clone();
        (changed.operation_id, changed.created_at) = generated_operation();
        changed.authority = altered_authority;
        changed.updates[0].expected_metadata = Some(value(2));
        changed.updates[0].metadata = Some(value(3));
        let error = cluster.run(0, |owner| {
            owner.apply_atomic_contribution(changed).unwrap_err()
        });
        assert!(error.to_string().contains("authority"), "{error}");
        assert_eq!(cluster.version(0, &identity), before);
    }
    cluster.run(2, |requester| {
        requester
            .posix_rename("/users/contribution-source", "/users/contribution-renamed")
            .unwrap();
    });
    // The stored full outcome takes precedence over the original head/path
    // fence and preserves a later metadata contribution to the same revision.
    cluster.run(0, |owner| {
        owner.apply_atomic_contribution(original.clone()).unwrap()
    });
    assert_eq!(cluster.version(0, &identity), before);
    let mut changed_time = original;
    changed_time.created_at += time::Duration::nanoseconds(1);
    let error = cluster.run(0, |owner| {
        owner.apply_atomic_contribution(changed_time).unwrap_err()
    });
    assert!(error.to_string().contains("timestamp"), "{error}");
    assert_eq!(cluster.version(0, &identity), before);
}

#[test]
fn admitted_contribution_retry_rejects_disjoint_owner_without_original_outcome() {
    let mut cluster = ContributionCluster::new("contribution-admitted-disjoint-move");
    let head = cluster.write("/users/admitted-contribution-a");
    let identity = DfsRevisionReference {
        file_system_id: head.file_system_id.clone(),
        object_id: head.object_id.clone(),
        version: head.version.clone(),
    };
    let original = request(
        identity.clone(),
        &head,
        cluster.nodes[0].clone(),
        "users",
        None,
        Some(value(1)),
    );
    cluster.run(0, |owner| {
        owner.apply_atomic_contribution(original.clone()).unwrap()
    });
    cluster.run(2, |requester| {
        requester
            .posix_rename(
                "/users/admitted-contribution-a",
                "/services/admitted-contribution-b",
            )
            .unwrap();
    });
    let moved = cluster.run(2, |requester| {
        requester
            .posix_stat("/services/admitted-contribution-b")
            .unwrap()
    });
    assert_eq!(
        cluster
            .version(1, &identity)
            .module_metadata
            .get(&original.namespace),
        Some(&value(1))
    );
    let reset = request(
        identity.clone(),
        &moved,
        cluster.nodes[1].clone(),
        "services",
        Some(value(1)),
        None,
    );
    cluster.run(1, |owner| owner.apply_atomic_contribution(reset).unwrap());
    let before = cluster.version(1, &identity);
    assert!(!before.module_metadata.contains_key(&original.namespace));
    let transaction_id = format!(
        "metadata-contribution:{}:{}",
        original.namespace, original.operation_id
    );
    cluster.run(1, |owner| {
        let backend = owner.metadata_backend().unwrap();
        assert!(
            backend
                .get_namespace_transaction(&transaction_id)
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .get_namespace_transaction_receipt(&transaction_id)
                .unwrap()
                .is_none()
        );
    });
    let error = cluster.run(1, |owner| {
        owner.apply_atomic_contribution(original).unwrap_err()
    });
    assert!(error.to_string().contains("authority changed"), "{error}");
    assert_eq!(cluster.version(1, &identity), before);
}

#[test]
fn distributed_contribution_original_head_rejects_disjoint_move_aba_without_outcome() {
    let mut cluster = ContributionCluster::new("contribution-disjoint-aba");
    let head = cluster.write("/users/contribution-a");
    let identity = DfsRevisionReference {
        file_system_id: head.file_system_id.clone(),
        object_id: head.object_id.clone(),
        version: head.version.clone(),
    };
    // This request has not been admitted anywhere. Returning to A cannot
    // turn its original head observation into a new current observation.
    let original = request(
        identity.clone(),
        &head,
        cluster.nodes[0].clone(),
        "users",
        None,
        Some(value(1)),
    );
    cluster.run(2, |requester| {
        requester
            .posix_rename("/users/contribution-a", "/services/contribution-b")
            .unwrap();
    });
    let error = cluster.run(1, |owner| {
        owner
            .apply_atomic_contribution(original.clone())
            .unwrap_err()
    });
    assert!(error.to_string().contains("authority changed"), "{error}");
    let b_head = cluster.run(2, |requester| {
        requester.posix_stat("/services/contribution-b").unwrap()
    });
    assert_ne!(b_head.version, head.version);
    let fresh = request(
        identity.clone(),
        &b_head,
        cluster.nodes[1].clone(),
        "services",
        None,
        Some(value(2)),
    );
    cluster.run(1, |owner| {
        owner.apply_atomic_contribution(fresh.clone()).unwrap()
    });
    let mut reset = fresh;
    (reset.operation_id, reset.created_at) = generated_operation();
    reset.updates[0].expected_metadata = Some(value(2));
    reset.updates[0].metadata = None;
    cluster.run(1, |owner| owner.apply_atomic_contribution(reset).unwrap());
    cluster.run(2, |requester| {
        requester
            .posix_rename("/services/contribution-b", "/users/contribution-a")
            .unwrap();
    });
    let a_head = cluster.run(2, |requester| {
        requester.posix_stat("/users/contribution-a").unwrap()
    });
    assert_eq!(a_head.path, head.path);
    assert_ne!(a_head.version, head.version);
    let before = cluster.version(0, &identity);
    assert!(!before.module_metadata.contains_key(&original.namespace));
    let transaction_id = format!(
        "metadata-contribution:{}:{}",
        original.namespace, original.operation_id
    );
    cluster.run(0, |owner| {
        let backend = owner.metadata_backend().unwrap();
        assert!(
            backend
                .get_namespace_transaction(&transaction_id)
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .get_namespace_transaction_receipt(&transaction_id)
                .unwrap()
                .is_none()
        );
    });
    let error = cluster.run(0, |owner| {
        owner.apply_atomic_contribution(original).unwrap_err()
    });
    assert!(error.to_string().contains("source head changed"), "{error}");
    assert_eq!(cluster.version(0, &identity), before);
}
