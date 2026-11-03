//! Owner-side metadata contribution fences across real routed namespace moves.
use super::*;

type Service = NetworkedDfsService<DfsNodeTransport>;

struct ContributionCluster {
    _runtime: crate::test_runtime::DfsTestRuntime,
    nodes: [NodeId; 3],
    services: Vec<Option<Service>>,
}

impl ContributionCluster {
    fn new(label: &str) -> Self {
        let cluster = sifs_two_primary_cluster(label);
        // Owner-side contribution checks and outcome lookups must use that
        // physical shard, not a facade also exposing replicated source shards.
        let users_owner = cluster._runtime.metadata_primary_service("/users").unwrap();
        let services_owner = cluster
            ._runtime
            .metadata_primary_service("/services")
            .unwrap();
        drop(cluster.users_primary);
        drop(cluster.services_primary);
        Self {
            nodes: [
                cluster.users_primary_node,
                cluster.services_primary_node,
                cluster.replica_node,
            ],
            services: vec![
                Some(users_owner),
                Some(services_owner),
                Some(cluster.replica),
            ],
            _runtime: cluster._runtime,
        }
    }

    fn authority(&self, shard_id: &str) -> DfsContributionAuthorityExpectation {
        let shard = self
            ._runtime
            .metadata_shards()
            .unwrap()
            .into_iter()
            .find(|shard| shard.shard_id == shard_id)
            .unwrap();
        let expected_owner = match shard_id {
            "users" => &self.nodes[0],
            "services" => &self.nodes[1],
            _ => unreachable!(),
        };
        assert_eq!(&shard.primary_node_id, expected_owner);
        DfsContributionAuthorityExpectation {
            shard_id: DfsMetadataShardId::new(shard.shard_id),
            primary_node_id: shard.primary_node_id,
            owner_epoch: shard.epoch,
        }
    }

    fn run<R>(&mut self, active: usize, operation: impl FnOnce(&mut Service) -> R) -> R {
        operation(self.services[active].as_mut().unwrap())
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
    authority: DfsContributionAuthorityExpectation,
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
        authority: Some(authority),
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
        cluster.authority("users"),
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
            owner_epoch: original.authority.as_ref().unwrap().owner_epoch + 1,
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
        cluster.authority("users"),
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
            .posix_stat_distributed("/services/admitted-contribution-b")
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
        cluster.authority("services"),
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
                .prepared_operation_record(&transaction_id)
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
        cluster.authority("users"),
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
        requester
            .posix_stat_distributed("/services/contribution-b")
            .unwrap()
    });
    assert_ne!(b_head.version, head.version);
    let fresh = request(
        identity.clone(),
        &b_head,
        cluster.authority("services"),
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
        requester
            .posix_stat_distributed("/users/contribution-a")
            .unwrap()
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
                .prepared_operation_record(&transaction_id)
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
