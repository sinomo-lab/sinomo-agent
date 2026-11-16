//! Exact command DATA for a retained native receipt. None of these observations
//! establish an outcome or permission: the caller supplies the freshly read
//! authoritative physical digest, and the whole original command must match it.
use super::*;

const MAX_WITNESS_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CANDIDATE_PAIRS: usize = 64;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Witness {
    version: u16,
    request_id: KvRequestId,
    logical_digest: sinomo_core::kv::KvCommandDigest,
    physical_digest: sinomo_core::kv::KvCommandDigest,
    authorization: Vec<KvCompare>,
}

#[derive(Clone, PartialEq, Eq)]
struct Candidate {
    revision: KvRevision,
    value: Arc<Vec<u8>>,
}

fn permission_keys(subject: &sinomo_core::node::NodeId) -> [String; 2] {
    [
        sinomo_core::node::cluster_member_key(subject),
        sinomo_core::node::cluster_capability_policy_key(subject),
    ]
}

fn suffix(keys: &[String; 2], member: &Candidate, policy: &Candidate) -> Vec<KvCompare> {
    [member, policy]
        .into_iter()
        .zip(keys)
        .flat_map(|(row, key)| {
            [
                KvCompare::ModifyRevisionEquals {
                    key: key.clone(),
                    revision: row.revision,
                },
                KvCompare::ValueEquals {
                    key: key.clone(),
                    value: row.value.as_ref().clone(),
                },
            ]
        })
        .collect()
}

fn matches_physical(
    request: &KvTxnRequest,
    subject: &sinomo_core::node::NodeId,
    authorization: &[KvCompare],
    physical: &sinomo_core::kv::KvCommandDigest,
) -> Result<bool> {
    if !request.txn.failure.is_empty() {
        return Err(error(
            "remote authorization preimage cannot contain failure-branch mutations",
        ));
    }
    let keys = permission_keys(subject);
    let [
        KvCompare::ModifyRevisionEquals {
            key: member_revision,
            ..
        },
        KvCompare::ValueEquals {
            key: member_value, ..
        },
        KvCompare::ModifyRevisionEquals {
            key: policy_revision,
            ..
        },
        KvCompare::ValueEquals {
            key: policy_value, ..
        },
    ] = authorization
    else {
        return Err(error(
            "retained authorization preimage is not the released four-compare suffix",
        ));
    };
    if member_revision != &keys[0]
        || member_value != &keys[0]
        || policy_revision != &keys[1]
        || policy_value != &keys[1]
    {
        return Err(error(
            "retained authorization preimage changes its original subject",
        ));
    }
    let mut command = request.txn.clone();
    command.compares.extend_from_slice(authorization);
    Ok(sinomo_core::kv::kv_command_digest(&KvCommand::Txn(command)).map_err(error)? == *physical)
}

fn add_entry(
    candidates: &mut [Vec<Candidate>; 2],
    keys: &[String; 2],
    row: &KvEntry,
    through: KvRevision,
) {
    if row.modify_revision > through {
        return;
    }
    for (i, key) in keys.iter().enumerate() {
        if &row.key == key {
            candidates[i].push(Candidate {
                revision: row.modify_revision,
                value: Arc::new(row.value.clone()),
            });
        }
    }
}

fn find_suffix(
    request: &KvTxnRequest,
    subject: &sinomo_core::node::NodeId,
    candidates: &mut [Vec<Candidate>; 2],
    physical: &sinomo_core::kv::KvCommandDigest,
    tried: &mut usize,
) -> Result<Option<Vec<KvCompare>>> {
    for rows in candidates.iter_mut() {
        rows.sort_by(|a, b| {
            b.revision
                .cmp(&a.revision)
                .then_with(|| a.value.cmp(&b.value))
        });
        rows.dedup();
        rows.truncate(MAX_CANDIDATE_PAIRS);
    }
    let keys = permission_keys(subject);
    for member in &candidates[0] {
        for policy in &candidates[1] {
            if *tried == MAX_CANDIDATE_PAIRS {
                return Err(error(
                    "original command preimage exceeds its bounded candidate search",
                ));
            }
            *tried += 1;
            let authorization = suffix(&keys, member, policy);
            if matches_physical(request, subject, &authorization, physical)? {
                return Ok(Some(authorization));
            }
        }
    }
    Ok(None)
}

impl NativeImportAuthority {
    pub(super) fn verify_original_command_preimage(
        &self,
        request: &KvTxnRequest,
        subject: &sinomo_core::node::NodeId,
        captured: &[MetadataCapturedKvEntry],
        physical: &sinomo_core::kv::KvCommandDigest,
        through: KvRevision,
    ) -> Result<()> {
        if Self::validate_original_command_preimage(request, subject, captured, physical).is_ok() {
            return Ok(());
        }
        let directory = self
            .data_root
            .join("dfs/migration-owner/authorization-preimages");
        let path = directory.join(format!("{}.json", request.request_id));
        let logical = sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone()))
            .map_err(error)?;
        if path.try_exists().map_err(error)? {
            self.validate_source_root(&path)?;
            let bytes = sinomo_core::storage::durable_file::read_object_file_bytes(
                &path,
                MAX_WITNESS_BYTES,
            )
            .map_err(error)?;
            let witness: Witness = serde_json::from_slice(&bytes).map_err(error)?;
            if witness.version != 1
                || witness.request_id != request.request_id
                || witness.logical_digest != logical
                || witness.physical_digest != *physical
                || !matches_physical(request, subject, &witness.authorization, physical)?
            {
                return Err(error(
                    "retained command preimage differs from its original request or native receipt",
                ));
            }
            return Ok(());
        }
        let keys = permission_keys(subject);
        let mut candidates: [Vec<Candidate>; 2] = Default::default();
        for entry in captured.iter().filter_map(|row| row.entry.as_ref()) {
            add_entry(&mut candidates, &keys, entry, through);
        }
        let (current, _) = self.read_exact(&keys)?;
        for entry in current.iter().filter_map(|row| row.entry.as_ref()) {
            add_entry(&mut candidates, &keys, entry, through);
        }
        let mut tried = 0;
        let authorization =
            match find_suffix(request, subject, &mut candidates, physical, &mut tried)? {
                Some(authorization) => authorization,
                None => {
                    self.add_retained_preimage_candidates(
                        request,
                        &keys,
                        through,
                        &mut candidates,
                    )?;
                    find_suffix(request, subject, &mut candidates, physical, &mut tried)?
                        .ok_or_else(|| {
                            error("original receipt has no exact physical command preimage")
                        })?
                }
            };
        // Retain verified DATA, not a completion flag. Every reuse still needs
        // the actual authoritative receipt and hashes the whole original body.
        sinomo_core::storage::durable_file::ensure_directory(&directory).map_err(error)?;
        self.validate_source_root(&directory)?;
        Self::save_blueprint(
            &directory,
            &format!("{}.json", request.request_id),
            &Witness {
                version: 1,
                request_id: request.request_id.clone(),
                logical_digest: logical,
                physical_digest: *physical,
                authorization,
            },
        )
    }

    /// sinomo:kv-configuration: bounded legacy receipt recovery reads local KV
    /// history only as candidate DATA. Neither freshness nor permission comes
    /// from this view; the complete command must match the authoritative digest.
    fn add_retained_preimage_candidates(
        &self,
        request: &KvTxnRequest,
        keys: &[String; 2],
        through: KvRevision,
        candidates: &mut [Vec<Candidate>; 2],
    ) -> Result<()> {
        let paths = sinomo_core::network::ClusterPaths::new(self.component_context.data_dir());
        // Use the canonical local owner's existing observation boundary.
        // The independent connectivity checkpoint may deliberately lag forever
        // after non-connectivity writes; it cannot supply this recovery view.
        let snapshot =
            sinomo_core::cluster::read_cluster_kv_latest_observed_snapshot_arc_if_present(&paths)
                .map_err(|failure| self.native_read_error(ServiceError::Network(failure)))?
                .ok_or_else(|| MetadataBackendError::StoragePreparationPending {
                    root: self.data_root.clone(),
                    processed_records: 0,
                })?;
        if snapshot.revision < through {
            return Err(MetadataBackendError::StoragePreparationPending {
                root: self.data_root.clone(),
                processed_records: 0,
            });
        }
        for entry in &snapshot.entries {
            add_entry(candidates, keys, entry, through);
        }
        for event in &snapshot.events {
            if event.revision > through {
                continue;
            }
            if let sinomo_core::kv::KvEventKind::Put { value } = &event.kind {
                for (i, key) in keys.iter().enumerate() {
                    if &event.key == key {
                        candidates[i].push(Candidate {
                            revision: event.revision,
                            value: Arc::new(value.clone()),
                        });
                    }
                }
            }
        }
        // A successful original ValueEquals binds these exact bytes at commit.
        // Historical Put receipts supply candidate modification revisions only;
        // hash equality, never a guessed combination, qualifies the preimage.
        for (i, key) in keys.iter().enumerate() {
            let values = request
                .txn
                .compares
                .iter()
                .filter_map(|compare| match compare {
                    KvCompare::ValueEquals { key: actual, value } if actual == key => {
                        Some(Arc::new(value.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            for record in &snapshot.committed_requests {
                if record.revision > through {
                    continue;
                }
                let Some(response) = record
                    .txn_response
                    .as_ref()
                    .filter(|response| response.succeeded)
                else {
                    continue;
                };
                for result in &response.responses {
                    if let sinomo_core::kv::KvOpResponse::Put {
                        key: actual,
                        revision,
                    } = result
                    {
                        if actual == key && *revision <= through {
                            for value in &values {
                                candidates[i].push(Candidate {
                                    revision: *revision,
                                    value: Arc::clone(value),
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_remote_receipt_keeps_exact_preimage_across_permission_revision_and_restart() {
        let (mut native, nodes) =
            crate::authority::placement_work::native_tests::native_migration_cluster(
                "native-original-permission-preimage",
                2,
            );
        let observer_context = native.component_context(&nodes[0]).unwrap();
        let observer = crate::authority::placement_work::native_tests::canonical_migration_runtime(
            &observer_context,
        );
        let issuer_context = native.component_context(&nodes[1]).unwrap();
        let issuer = crate::authority::placement_work::native_tests::canonical_migration_runtime(
            &issuer_context,
        );
        let keys = permission_keys(&nodes[1]);
        let (captured, _) = observer.authority.read_exact(&keys).unwrap();
        let member = captured[0].entry.as_ref().unwrap().clone();
        let bump = |owner: &NativeImportAuthority| {
            let before = owner.read_exact(&keys[..1]).unwrap().0[0]
                .entry
                .clone()
                .unwrap();
            let mut publication: sinomo_core::cluster::ClusterMemberRecord =
                before.decode().unwrap();
            publication.updated_at += time::Duration::seconds(1);
            let value = sinomo_core::kv::encode_kv_value(&publication).unwrap();
            let commit = owner
                .commit(KvTxnRequest::new(
                    KvRequestId::generate(),
                    KvTxn {
                        compares: vec![KvCompare::ValueEquals {
                            key: before.key.clone(),
                            value: before.value.clone(),
                        }],
                        success: vec![KvOp::Put {
                            key: before.key.clone(),
                            value: value.clone(),
                            lease_id: before.lease_id,
                        }],
                        failure: Vec::new(),
                    },
                ))
                .unwrap();
            NativeImportAuthority::require_committed(&commit).unwrap();
            let after = owner.read_exact(&keys[..1]).unwrap().0[0]
                .entry
                .clone()
                .unwrap();
            assert_eq!(after.value, value);
            assert!(after.modify_revision > before.modify_revision);
        };
        bump(&observer.authority);
        let submitted_rows = observer.authority.read_exact(&keys).unwrap().0;
        let request = KvTxnRequest::new(
            KvRequestId::generate(),
            KvTxn {
                compares: submitted_rows
                    .iter()
                    .map(|row| {
                        let entry = row.entry.as_ref().unwrap();
                        KvCompare::ValueEquals {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                        }
                    })
                    .collect(),
                success: vec![KvOp::Put {
                    key: "/native-preimage-test/value".into(),
                    value: b"original".to_vec(),
                    lease_id: None,
                }],
                failure: Vec::new(),
            },
        );
        let original = serde_json::to_vec(&request).unwrap();
        let commit = issuer.authority.commit(request.clone()).unwrap();
        NativeImportAuthority::require_committed(&commit).unwrap();
        assert_ne!(
            commit.result.command_digest,
            sinomo_core::kv::kv_command_digest(&KvCommand::Txn(request.txn.clone())).unwrap(),
            "actual Remote ingress must append its authorization suffix"
        );
        // Change it again before observation: neither original plan rows nor
        // current permission revisions alone can reconstruct the real receipt.
        bump(&observer.authority);
        let observe = |owner: &NativeImportAuthority| {
            block_native(&owner.runtime, async {
                let deadline =
                    tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
                loop {
                    match owner.observe_original_native_commit(
                        request.clone(),
                        &nodes[1],
                        &captured,
                    ) {
                        Ok(commit) => return Ok(commit),
                        Err(MetadataBackendError::StoragePreparationPending { .. })
                            if tokio::time::Instant::now() < deadline =>
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        Err(failure) => return Err(failure),
                    }
                }
            })
            .unwrap()
        };
        let recovered = observe(&observer.authority);
        assert_eq!(
            recovered.result.command_digest,
            commit.result.command_digest
        );
        let witness_path = observer
            .authority
            .data_root
            .join("dfs/migration-owner/authorization-preimages")
            .join(format!("{}.json", request.request_id));
        let witness = std::fs::read(&witness_path).unwrap();
        assert_eq!(serde_json::to_vec(&recovered.request).unwrap(), original);
        assert_eq!(captured[0].entry.as_ref().unwrap(), &member);
        bump(&observer.authority);
        drop(issuer);
        drop(issuer_context);
        drop(observer);
        drop(observer_context);
        native.stop_node(&nodes[0]).unwrap();
        native.reopen_node(nodes[0].clone()).unwrap();
        let context = native.component_context(&nodes[0]).unwrap();
        let owner =
            crate::authority::placement_work::native_tests::canonical_migration_runtime(&context);
        let reopened = observe(&owner.authority);
        assert_eq!(reopened.result.command_digest, commit.result.command_digest);
        assert_eq!(std::fs::read(&witness_path).unwrap(), witness);
        let mut changed = request.clone();
        changed.txn.success = vec![KvOp::Put {
            key: "/native-preimage-test/value".into(),
            value: b"changed".to_vec(),
            lease_id: None,
        }];
        assert!(
            owner
                .authority
                .observe_original_native_commit(changed, &nodes[1], &captured)
                .is_err()
        );
        let mut corrupt: Witness = serde_json::from_slice(&witness).unwrap();
        corrupt.authorization.pop();
        std::fs::write(&witness_path, serde_json::to_vec(&corrupt).unwrap()).unwrap();
        assert!(
            owner
                .authority
                .observe_original_native_commit(request, &nodes[1], &captured)
                .is_err()
        );
        std::fs::write(&witness_path, &witness).unwrap();
        let value = owner
            .authority
            .read_exact(&["/native-preimage-test/value".into()])
            .unwrap()
            .0[0]
            .entry
            .clone()
            .unwrap();
        assert_eq!(value.value, b"original");
        drop(owner);
        drop(context);
        native.close().unwrap();
    }
}
