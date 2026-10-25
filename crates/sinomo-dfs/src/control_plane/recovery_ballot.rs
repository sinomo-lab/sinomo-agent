//! Test-only, non-activating control-plane reservation. This sibling key is
//! neither a handoff nor an authority/Promise/ChosenFrame/serving certificate.
use super::*;

const RESERVATION_DOMAIN: &[u8] = b"sinomo-dfs-metadata-recovery-ballot-reservation-v1\0";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DfsMetadataShardRecoveryBallot {
    pub(crate) epoch: u64,
    pub(crate) attempt: u64,
    pub(crate) candidate_node_id: NodeId,
}

/// Pure request DATA. Identical callers intentionally derive the same request;
/// neither Reserved nor Existing establishes proposer uniqueness or authority.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DfsMetadataShardRecoveryBallotRequest {
    pub(crate) reservation_id: String,
    pub(crate) serving_shard: DfsMetadataShardConfig,
    pub(crate) expected_attempt_floor: u64,
    pub(crate) ballot: DfsMetadataShardRecoveryBallot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsRecoveryBallotRefusal {
    InvalidScope,
    NotFixedThreeMember,
    CandidateNotReplica,
    EpochOverflow,
    AttemptOverflow,
    MalformedId,
    NoAuthority,
    AuthorityChanged,
    HandoffPending,
    EpochRegressed,
    FloorAhead,
    Spent,
    PendingHeldByOther,
    NotPending,
    NotMember,
    Busy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsRecoveryBallotBegin {
    Reserved(DfsMetadataShardRecoveryBallotRequest),
    Existing(DfsMetadataShardRecoveryBallotRequest),
    Refused(DfsRecoveryBallotRefusal),
}

/// Necessary authoritative DATA verification only; deliberately not a lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DfsRecoveryBallotVerify {
    Verified(DfsMetadataShardRecoveryBallotRequest),
    Refused(DfsRecoveryBallotRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsRecoveryBallotFloorView {
    pub(crate) attempt_floor: u64,
    pub(crate) floor_epoch: Option<u64>,
    pub(crate) pending: Option<DfsMetadataShardRecoveryBallotRequest>,
    pub(crate) serving_shard: Option<DfsMetadataShardConfig>,
    pub(crate) handoff_pending: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RecoveryBallotFloor {
    attempt_floor: u64,
    floor_epoch: u64,
    // Absence of the field is malformed, not an implicit cancellation.
    #[serde(deserialize_with = "required_pending")]
    pending: Option<DfsMetadataShardRecoveryBallotRequest>,
}

fn required_pending<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DfsMetadataShardRecoveryBallotRequest>, D::Error> {
    Option::deserialize(deserializer)
}

pub(crate) fn metadata_shard_recovery_ballot_key(fs: &DfsFileSystemId, shard: &str) -> String {
    format!(
        "{DFS_KV_PREFIX}/filesystems/{fs}/metadata-recovery-ballots/{}",
        normalize_segment(shard)
    )
}

fn malformed(detail: impl Into<String>) -> KvError {
    KvError::Serialization {
        detail: format!("metadata recovery ballot: {}", detail.into()),
    }
}

fn validate_scope(shard: &str) -> Result<(), DfsRecoveryBallotRefusal> {
    if shard.is_empty()
        || shard.trim() != shard
        || shard.contains('\0')
        || normalize_segment(shard) != shard
    {
        return Err(DfsRecoveryBallotRefusal::InvalidScope);
    }
    Ok(())
}

fn validate_members(
    serving: &DfsMetadataShardConfig,
    candidate: &NodeId,
) -> Result<(), DfsRecoveryBallotRefusal> {
    validate_scope(&serving.shard_id)?;
    let members = std::iter::once(&serving.primary_node_id)
        .chain(&serving.replica_node_ids)
        .collect::<BTreeSet<_>>();
    if serving.replica_node_ids.len() != 2 || members.len() != 3 {
        return Err(DfsRecoveryBallotRefusal::NotFixedThreeMember);
    }
    if !serving.replica_node_ids.contains(candidate) {
        return Err(DfsRecoveryBallotRefusal::CandidateNotReplica);
    }
    Ok(())
}

fn derived_id(
    serving: &DfsMetadataShardConfig,
    ballot: &DfsMetadataShardRecoveryBallot,
) -> Result<String, DfsRecoveryBallotRefusal> {
    #[derive(Serialize)]
    struct Identity<'a> {
        serving: &'a DfsMetadataShardConfig,
        candidate: &'a NodeId,
        ballot: &'a DfsMetadataShardRecoveryBallot,
    }
    let encoded = serde_json::to_vec(&Identity {
        serving,
        candidate: &ballot.candidate_node_id,
        ballot,
    })
    .map_err(|_| DfsRecoveryBallotRefusal::MalformedId)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(RESERVATION_DOMAIN);
    hasher.update(&encoded);
    Ok(hasher.finalize().to_hex().to_string())
}

impl DfsMetadataShardRecoveryBallotRequest {
    pub(crate) fn prepare(
        serving: &DfsMetadataShardConfig,
        candidate: &NodeId,
        expected_attempt_floor: u64,
    ) -> Result<Self, DfsRecoveryBallotRefusal> {
        let epoch = serving
            .epoch
            .checked_add(1)
            .ok_or(DfsRecoveryBallotRefusal::EpochOverflow)?;
        let attempt = expected_attempt_floor
            .checked_add(1)
            .ok_or(DfsRecoveryBallotRefusal::AttemptOverflow)?;
        validate_members(serving, candidate)?;
        let ballot = DfsMetadataShardRecoveryBallot {
            epoch,
            attempt,
            candidate_node_id: candidate.clone(),
        };
        Ok(Self {
            reservation_id: derived_id(serving, &ballot)?,
            serving_shard: serving.clone(),
            expected_attempt_floor,
            ballot,
        })
    }

    fn validate(&self) -> Result<(), DfsRecoveryBallotRefusal> {
        let expected = Self::prepare(
            &self.serving_shard,
            &self.ballot.candidate_node_id,
            self.expected_attempt_floor,
        )?;
        if self != &expected {
            return Err(DfsRecoveryBallotRefusal::MalformedId);
        }
        Ok(())
    }
}

impl RecoveryBallotFloor {
    fn validate(&self, fs: &DfsFileSystemId, shard: &str) -> Result<(), KvError> {
        if self.attempt_floor == 0 || self.floor_epoch.checked_add(1).is_none() {
            return Err(malformed("invalid retained attempt/epoch floor"));
        }
        if let Some(pending) = &self.pending {
            pending
                .validate()
                .map_err(|reason| malformed(format!("invalid pending request: {reason:?}")))?;
            if pending.serving_shard.file_system_id != *fs
                || pending.serving_shard.shard_id != shard
                || pending.serving_shard.epoch != self.floor_epoch
                || pending.ballot.attempt != self.attempt_floor
                || pending.expected_attempt_floor != self.attempt_floor - 1
            {
                return Err(malformed(
                    "pending request differs from retained floor or key scope",
                ));
            }
        }
        Ok(())
    }
}

struct ReservationRead {
    entries: Vec<Option<KvEntry>>,
    floor: Option<RecoveryBallotFloor>,
}

fn deadline_before(deadline: Instant) -> Result<(), KvError> {
    if Instant::now() >= deadline {
        return Err(metadata_shard_authority_deadline_error());
    }
    Ok(())
}

fn exact_entries(keys: &[String], entries: &[Option<KvEntry>]) -> Result<(), KvError> {
    if entries.len() != keys.len()
        || entries
            .iter()
            .zip(keys)
            .any(|(entry, key)| entry.as_ref().is_some_and(|entry| &entry.key != key))
    {
        return Err(malformed(
            "authoritative read returned a differently scoped key set",
        ));
    }
    Ok(())
}

fn decode_floor(
    entry: Option<&KvEntry>,
    fs: &DfsFileSystemId,
    shard: &str,
) -> Result<Option<RecoveryBallotFloor>, KvError> {
    if entry.is_some_and(|entry| entry.lease_id.is_some()) {
        return Err(malformed(
            "reservation floor must not expire through a lease",
        ));
    }
    let floor = entry
        .map(KvEntry::decode::<RecoveryBallotFloor>)
        .transpose()?;
    if let Some(floor) = &floor {
        floor.validate(fs, shard)?;
    }
    Ok(floor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Structural/CAS harness only. Native Raft execution has separate tests.
    #[derive(Debug)]
    struct ObservedKv {
        inner: InProcessKvStore,
        reads: Vec<Vec<String>>,
        writes: Vec<KvTxn>,
        wrong_key: bool,
        compare_miss: bool,
        lose_reply_once: bool,
        late_read: Option<Instant>,
        late_commit: Option<Instant>,
    }

    impl ObservedKv {
        fn seeded() -> (Self, DfsMetadataShardConfig) {
            let serving = DfsMetadataShardConfig {
                file_system_id: DfsFileSystemId::generate(),
                shard_id: "reservation-structural".into(),
                primary_node_id: NodeId::generate(),
                replica_node_ids: vec![NodeId::generate(), NodeId::generate()],
                epoch: 7,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            };
            let mut inner = InProcessKvStore::new();
            inner
                .put_serde(
                    metadata_shard_key(&serving.file_system_id, &serving.shard_id),
                    &serving,
                    None,
                )
                .unwrap();
            (
                Self {
                    inner,
                    reads: Vec::new(),
                    writes: Vec::new(),
                    wrong_key: false,
                    compare_miss: false,
                    lose_reply_once: false,
                    late_read: None,
                    late_commit: None,
                },
                serving,
            )
        }
    }

    impl DfsControlPlaneKvClient for ObservedKv {
        fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
            Ok(self.inner.get(key).cloned())
        }
        fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            self.inner.range_prefix(prefix)
        }
        fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
            self.inner.put(key, value, None)
        }
        fn get_authoritative_consistent_before(
            &mut self,
            keys: &[String],
            deadline: Instant,
        ) -> Result<Vec<Option<KvEntry>>, KvError> {
            deadline_before(deadline)?;
            self.reads.push(keys.to_vec());
            let mut entries = keys
                .iter()
                .map(|key| self.inner.get(key).cloned())
                .collect::<Vec<_>>();
            if self.wrong_key
                && let Some(entry) = entries.iter_mut().flatten().next()
            {
                entry.key.push_str("-foreign");
            }
            if let Some(until) = self.late_read {
                std::thread::sleep(
                    until.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
                );
            }
            Ok(entries)
        }
        fn txn(&mut self, mut txn: KvTxn) -> Result<KvTxnResponse, KvError> {
            self.writes.push(txn.clone());
            if self.compare_miss {
                txn.compares.push(KvCompare::Exists {
                    key: "/structural-missing".into(),
                });
            }
            if let Some(until) = self.late_commit {
                assert!(
                    Instant::now() < until,
                    "the actual transaction must start before its deadline"
                );
                std::thread::sleep(
                    until.saturating_duration_since(Instant::now()) + Duration::from_millis(2),
                );
            }
            let result = self.inner.txn(txn)?;
            if self.lose_reply_once {
                self.lose_reply_once = false;
                return Err(KvError::AuthorityUnavailable {
                    detail: "structural cut after actual local CAS, before reply".into(),
                });
            }
            Ok(result)
        }
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(2)
    }
    fn request(
        serving: &DfsMetadataShardConfig,
        floor: u64,
    ) -> DfsMetadataShardRecoveryBallotRequest {
        DfsMetadataShardRecoveryBallotRequest::prepare(serving, &serving.replica_node_ids[0], floor)
            .unwrap()
    }

    #[test]
    fn recovery_reservation_request_binds_full_identity_and_refuses_before_io() {
        let (mut kv, serving) = ObservedKv::seeded();
        let original = request(&serving, 0);
        assert_eq!(request(&serving, 0), original);
        assert_eq!(original.ballot.epoch, 8);
        assert_eq!(original.ballot.attempt, 1);
        let mut variants = Vec::new();
        let mut changed = original.clone();
        changed.ballot.candidate_node_id = serving.replica_node_ids[1].clone();
        variants.push(changed);
        let mut changed = original.clone();
        changed.serving_shard.epoch += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.serving_shard.default_policy = DfsObjectPolicy::ReplicatedConfirmed;
        variants.push(changed);
        let mut changed = original.clone();
        changed.expected_attempt_floor += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.ballot.attempt += 1;
        variants.push(changed);
        for malformed in variants {
            let mut store = DfsControlPlaneStore::new(&mut kv);
            assert_eq!(
                store
                    .begin_metadata_shard_recovery_ballot_reservation(&malformed, deadline())
                    .unwrap(),
                DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::MalformedId)
            );
            assert_eq!(
                store
                    .verify_metadata_shard_recovery_ballot_before(
                        &malformed,
                        &serving.primary_node_id,
                        deadline()
                    )
                    .unwrap(),
                DfsRecoveryBallotVerify::Refused(DfsRecoveryBallotRefusal::MalformedId)
            );
            assert!(
                store
                    .cancel_metadata_shard_recovery_ballot_reservation(&malformed, deadline())
                    .is_err()
            );
        }
        let mut overflow = serving.clone();
        overflow.epoch = u64::MAX;
        assert_eq!(
            DfsMetadataShardRecoveryBallotRequest::prepare(
                &overflow,
                &overflow.replica_node_ids[0],
                0
            ),
            Err(DfsRecoveryBallotRefusal::EpochOverflow)
        );
        assert_eq!(
            DfsMetadataShardRecoveryBallotRequest::prepare(
                &serving,
                &serving.replica_node_ids[0],
                u64::MAX
            ),
            Err(DfsRecoveryBallotRefusal::AttemptOverflow)
        );
        let mut bad_group = serving.clone();
        bad_group.replica_node_ids[1] = bad_group.primary_node_id.clone();
        assert_eq!(
            DfsMetadataShardRecoveryBallotRequest::prepare(
                &bad_group,
                &bad_group.replica_node_ids[0],
                0
            ),
            Err(DfsRecoveryBallotRefusal::NotFixedThreeMember)
        );
        assert_eq!(
            DfsMetadataShardRecoveryBallotRequest::prepare(&serving, &serving.primary_node_id, 0),
            Err(DfsRecoveryBallotRefusal::CandidateNotReplica)
        );
        assert!(kv.reads.is_empty() && kv.writes.is_empty());
    }

    #[test]
    fn recovery_reservation_exact_retry_cancellation_and_stale_floor_never_remint() {
        let (mut kv, serving) = ObservedKv::seeded();
        let first = request(&serving, 0);
        kv.lose_reply_once = true;
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .is_err()
        );
        assert_eq!(kv.writes.len(), 1);
        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert_eq!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Existing(first.clone())
        );
        let other = DfsMetadataShardRecoveryBallotRequest::prepare(
            &serving,
            &serving.replica_node_ids[1],
            0,
        )
        .unwrap();
        assert_eq!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&other, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::Spent)
        );
        let second = request(&serving, 1);
        assert_eq!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&second, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::PendingHeldByOther)
        );
        assert!(
            store
                .cancel_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap()
        );
        assert_eq!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&second, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Reserved(second.clone())
        );
        assert!(
            !store
                .cancel_metadata_shard_recovery_ballot_reservation(&first, deadline())
                .unwrap()
        );
        assert!(
            store
                .cancel_metadata_shard_recovery_ballot_reservation(&second, deadline())
                .unwrap()
        );
        for old in [&first, &second, &other] {
            assert_eq!(
                store
                    .begin_metadata_shard_recovery_ballot_reservation(old, deadline())
                    .unwrap(),
                DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::Spent)
            );
            assert_eq!(
                store
                    .verify_metadata_shard_recovery_ballot_before(
                        old,
                        &serving.primary_node_id,
                        deadline()
                    )
                    .unwrap(),
                DfsRecoveryBallotVerify::Refused(DfsRecoveryBallotRefusal::NotPending)
            );
        }
        assert_eq!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&request(&serving, 5), deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::FloorAhead)
        );
        let view = store
            .metadata_shard_recovery_ballot_floor_before(
                &serving.file_system_id,
                &serving.shard_id,
                deadline(),
            )
            .unwrap();
        assert_eq!(
            (view.attempt_floor, view.floor_epoch, view.pending),
            (2, Some(serving.epoch), None)
        );
        assert_eq!(kv.writes.len(), 4);
        let keys = keys(&first);
        assert_eq!(kv.writes[0].compares.len(), 3);
        assert!(
            matches!(&kv.writes[0].compares[0], KvCompare::ValueEquals { key, .. } if key == &keys[0])
        );
        assert!(matches!(&kv.writes[0].compares[1], KvCompare::Missing { key } if key == &keys[1]));
        assert!(matches!(&kv.writes[0].compares[2], KvCompare::Missing { key } if key == &keys[2]));
        for txn in &kv.writes {
            assert!(txn.failure.is_empty());
            assert!(
                matches!(txn.success.as_slice(), [KvOp::Put { key, lease_id: None, .. }] if key == &keys[2])
            );
        }
        assert_eq!(
            kv.inner
                .get(&keys[0])
                .unwrap()
                .decode::<DfsMetadataShardConfig>()
                .unwrap(),
            serving
        );
        assert!(kv.inner.get(&keys[1]).is_none());
    }

    #[test]
    fn recovery_reservation_stored_codec_and_invariants_fail_without_mutation() {
        let (mut kv, serving) = ObservedKv::seeded();
        let req = request(&serving, 0);
        let valid = RecoveryBallotFloor {
            attempt_floor: 1,
            floor_epoch: serving.epoch,
            pending: Some(req.clone()),
        };
        let encoded = sinomo_core::kv::encode_kv_value(&valid).unwrap();
        assert_eq!(
            sinomo_core::kv::decode_kv_value::<RecoveryBallotFloor>(&encoded).unwrap(),
            valid
        );
        let cancelled = RecoveryBallotFloor {
            pending: None,
            ..valid.clone()
        };
        assert_eq!(
            sinomo_core::kv::decode_kv_value::<RecoveryBallotFloor>(
                &sinomo_core::kv::encode_kv_value(&cancelled).unwrap()
            )
            .unwrap(),
            cancelled
        );
        #[derive(Serialize)]
        struct MissingPending {
            attempt_floor: u64,
            floor_epoch: u64,
        }
        #[derive(Serialize)]
        struct UnknownField {
            attempt_floor: u64,
            floor_epoch: u64,
            pending: Option<DfsMetadataShardRecoveryBallotRequest>,
            ignored: bool,
        }
        let mut bytes = vec![
            sinomo_core::kv::encode_kv_value(&MissingPending {
                attempt_floor: 1,
                floor_epoch: serving.epoch,
            })
            .unwrap(),
            sinomo_core::kv::encode_kv_value(&UnknownField {
                attempt_floor: 1,
                floor_epoch: serving.epoch,
                pending: None,
                ignored: true,
            })
            .unwrap(),
        ];
        for field in 0..7 {
            let mut changed = valid.clone();
            match field {
                0 => changed.attempt_floor = 0,
                1 => changed.attempt_floor = 2,
                2 => changed.floor_epoch += 1,
                3 => changed.pending.as_mut().unwrap().reservation_id.push('x'),
                4 => {
                    changed.pending.as_mut().unwrap().ballot.candidate_node_id = NodeId::generate()
                }
                5 => changed.pending.as_mut().unwrap().serving_shard.shard_id = "other-key".into(),
                _ => {
                    changed.pending = None;
                    changed.floor_epoch = u64::MAX;
                }
            }
            bytes.push(sinomo_core::kv::encode_kv_value(&changed).unwrap());
        }
        let key = metadata_shard_recovery_ballot_key(&serving.file_system_id, &serving.shard_id);
        for value in bytes {
            kv.inner.put(key.clone(), value.clone(), None).unwrap();
            let mut store = DfsControlPlaneStore::new(&mut kv);
            assert!(
                store
                    .metadata_shard_recovery_ballot_floor_before(
                        &serving.file_system_id,
                        &serving.shard_id,
                        deadline()
                    )
                    .is_err()
            );
            assert!(
                store
                    .begin_metadata_shard_recovery_ballot_reservation(&req, deadline())
                    .is_err()
            );
            assert!(
                store
                    .cancel_metadata_shard_recovery_ballot_reservation(&req, deadline())
                    .is_err()
            );
            assert!(
                store
                    .verify_metadata_shard_recovery_ballot_before(
                        &req,
                        &serving.primary_node_id,
                        deadline()
                    )
                    .is_err()
            );
            assert_eq!(kv.inner.get(&key).unwrap().value, value);
        }
        assert!(kv.writes.is_empty());
    }

    #[test]
    fn recovery_reservation_exact_read_set_deadlines_and_cas_bound_are_preserved() {
        let (mut kv, serving) = ObservedKv::seeded();
        let req = request(&serving, 0);
        let expired = Instant::now();
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_recovery_ballot_reservation(&req, expired)
                .is_err()
        );
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .cancel_metadata_shard_recovery_ballot_reservation(&req, expired)
                .is_err()
        );
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .verify_metadata_shard_recovery_ballot_before(
                    &req,
                    &serving.primary_node_id,
                    expired
                )
                .is_err()
        );
        assert!(kv.reads.is_empty() && kv.writes.is_empty());
        kv.wrong_key = true;
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_recovery_ballot_reservation(&req, deadline())
                .is_err()
        );
        assert!(kv.writes.is_empty());
        kv.wrong_key = false;
        kv.compare_miss = true;
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_recovery_ballot_reservation(&req, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::Busy)
        );
        assert_eq!(kv.writes.len(), METADATA_MEMBERSHIP_HANDOFF_RETRIES);
        assert!(kv.reads.iter().all(|read| read == &keys(&req)));
        kv.compare_miss = false;
        let until = Instant::now() + Duration::from_millis(100);
        kv.late_read = Some(until);
        let count = kv.writes.len();
        assert!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_recovery_ballot_reservation(&req, until)
                .is_err()
        );
        assert_eq!(kv.writes.len(), count);
        kv.late_read = None;
        let until = Instant::now() + Duration::from_millis(100);
        kv.late_commit = Some(until);
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .begin_metadata_shard_recovery_ballot_reservation(&req, until)
                .unwrap(),
            DfsRecoveryBallotBegin::Reserved(req.clone())
        );
        assert!(Instant::now() > until);
        kv.late_commit = None;
        assert_eq!(
            DfsControlPlaneStore::new(&mut kv)
                .verify_metadata_shard_recovery_ballot_before(
                    &req,
                    &serving.primary_node_id,
                    deadline()
                )
                .unwrap(),
            DfsRecoveryBallotVerify::Verified(req)
        );
    }

    #[test]
    fn recovery_reservation_sibling_key_does_not_match_serving_or_handoff_owners() {
        let (mut kv, serving) = ObservedKv::seeded();
        let req = request(&serving, 0);
        let key = metadata_shard_recovery_ballot_key(&serving.file_system_id, &serving.shard_id);
        assert!(!crate::dfs_metadata_authority_key_is_exact(&key));
        assert!(!crate::dfs_metadata_shard_membership_handoff_key_is_exact(
            &key
        ));
        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert!(matches!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&req, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Reserved(_)
        ));
        assert_eq!(
            store
                .verify_metadata_shard_recovery_ballot_before(&req, &NodeId::generate(), deadline())
                .unwrap(),
            DfsRecoveryBallotVerify::Refused(DfsRecoveryBallotRefusal::NotMember)
        );
        let entry = kv.inner.get(&key).unwrap();
        assert!(
            !crate::metadata_authority_projection::metadata_storage_configuration_changed(
                &key,
                None,
                Some(entry)
            )
        );
        let mut changed = serving.clone();
        changed.epoch += 1;
        kv.inner
            .put_serde(
                metadata_shard_key(&serving.file_system_id, &serving.shard_id),
                &changed,
                None,
            )
            .unwrap();
        let mut store = DfsControlPlaneStore::new(&mut kv);
        assert_eq!(
            store
                .verify_metadata_shard_recovery_ballot_before(
                    &req,
                    &serving.primary_node_id,
                    deadline()
                )
                .unwrap(),
            DfsRecoveryBallotVerify::Refused(DfsRecoveryBallotRefusal::AuthorityChanged)
        );
        assert_eq!(
            store
                .begin_metadata_shard_recovery_ballot_reservation(&req, deadline())
                .unwrap(),
            DfsRecoveryBallotBegin::Refused(DfsRecoveryBallotRefusal::AuthorityChanged)
        );
        assert!(
            store
                .cancel_metadata_shard_recovery_ballot_reservation(&req, deadline())
                .unwrap(),
            "stale pending DATA remains cancellable without restoring old authority"
        );
    }
}

fn keys(request: &DfsMetadataShardRecoveryBallotRequest) -> [String; 3] {
    scoped_keys(
        &request.serving_shard.file_system_id,
        &request.serving_shard.shard_id,
    )
}

fn scoped_keys(fs: &DfsFileSystemId, shard: &str) -> [String; 3] {
    [
        metadata_shard_key(fs, shard),
        metadata_shard_membership_handoff_key(fs, shard),
        metadata_shard_recovery_ballot_key(fs, shard),
    ]
}

fn authority_refusal(
    read: &ReservationRead,
    request: &DfsMetadataShardRecoveryBallotRequest,
) -> Result<Option<DfsRecoveryBallotRefusal>, KvError> {
    let Some(entry) = &read.entries[0] else {
        return Ok(Some(DfsRecoveryBallotRefusal::NoAuthority));
    };
    if entry.decode::<DfsMetadataShardConfig>()? != request.serving_shard {
        return Ok(Some(DfsRecoveryBallotRefusal::AuthorityChanged));
    }
    if read.entries[1].is_some() {
        return Ok(Some(DfsRecoveryBallotRefusal::HandoffPending));
    }
    if read
        .floor
        .as_ref()
        .is_some_and(|floor| request.serving_shard.epoch < floor.floor_epoch)
    {
        return Ok(Some(DfsRecoveryBallotRefusal::EpochRegressed));
    }
    Ok(None)
}

impl DfsControlPlaneStore<'_> {
    fn read_recovery_ballot_before(
        &mut self,
        fs: &DfsFileSystemId,
        shard: &str,
        deadline: Instant,
    ) -> Result<ReservationRead, KvError> {
        validate_scope(shard).map_err(|reason| malformed(format!("{reason:?}")))?;
        deadline_before(deadline)?;
        let keys = scoped_keys(fs, shard);
        let entries = self
            .kv
            .get_authoritative_consistent_before(&keys, deadline)?;
        deadline_before(deadline)?;
        exact_entries(&keys, &entries)?;
        let floor = decode_floor(entries[2].as_ref(), fs, shard)?;
        Ok(ReservationRead { entries, floor })
    }

    /// sinomo:kv-configuration: Read one compact non-activating reservation floor from its exact authoritative control-plane key set.
    pub(crate) fn metadata_shard_recovery_ballot_floor_before(
        &mut self,
        fs: &DfsFileSystemId,
        shard: &str,
        deadline: Instant,
    ) -> Result<DfsRecoveryBallotFloorView, KvError> {
        let read = self.read_recovery_ballot_before(fs, shard, deadline)?;
        let serving_shard = read.entries[0]
            .as_ref()
            .map(KvEntry::decode::<DfsMetadataShardConfig>)
            .transpose()?;
        deadline_before(deadline)?;
        Ok(DfsRecoveryBallotFloorView {
            attempt_floor: read.floor.as_ref().map_or(0, |floor| floor.attempt_floor),
            floor_epoch: read.floor.as_ref().map(|floor| floor.floor_epoch),
            pending: read.floor.and_then(|floor| floor.pending),
            serving_shard,
            handoff_pending: read.entries[1].is_some(),
        })
    }

    /// sinomo:kv-consistency: Reserve one bounded recovery proposal by exact serving bytes, absent cooperative handoff, and monotonic floor CAS; grant no serving authority.
    pub(crate) fn begin_metadata_shard_recovery_ballot_reservation(
        &mut self,
        request: &DfsMetadataShardRecoveryBallotRequest,
        deadline: Instant,
    ) -> Result<DfsRecoveryBallotBegin, KvError> {
        if let Err(reason) = request.validate() {
            return Ok(DfsRecoveryBallotBegin::Refused(reason));
        }
        let keys = keys(request);
        for _ in 0..METADATA_MEMBERSHIP_HANDOFF_RETRIES {
            let read = self.read_recovery_ballot_before(
                &request.serving_shard.file_system_id,
                &request.serving_shard.shard_id,
                deadline,
            )?;
            let floor = read.floor.as_ref().map_or(0, |floor| floor.attempt_floor);
            let pending = read.floor.as_ref().and_then(|floor| floor.pending.as_ref());
            if request.expected_attempt_floor > floor {
                return Ok(DfsRecoveryBallotBegin::Refused(
                    DfsRecoveryBallotRefusal::FloorAhead,
                ));
            }
            let existing = request.expected_attempt_floor < floor && pending == Some(request);
            if request.expected_attempt_floor < floor && !existing {
                return Ok(DfsRecoveryBallotBegin::Refused(
                    DfsRecoveryBallotRefusal::Spent,
                ));
            }
            if request.expected_attempt_floor == floor && pending.is_some() {
                return Ok(DfsRecoveryBallotBegin::Refused(
                    DfsRecoveryBallotRefusal::PendingHeldByOther,
                ));
            }
            if let Some(reason) = authority_refusal(&read, request)? {
                return Ok(DfsRecoveryBallotBegin::Refused(reason));
            }
            deadline_before(deadline)?;
            if existing {
                return Ok(DfsRecoveryBallotBegin::Existing(request.clone()));
            }
            let next = RecoveryBallotFloor {
                attempt_floor: request.ballot.attempt,
                floor_epoch: request.serving_shard.epoch,
                pending: Some(request.clone()),
            };
            let floor_compare = match &read.entries[2] {
                Some(entry) => KvCompare::ValueEquals {
                    key: keys[2].clone(),
                    value: entry.value.clone(),
                },
                None => KvCompare::Missing {
                    key: keys[2].clone(),
                },
            };
            let txn = KvTxn {
                compares: vec![
                    KvCompare::ValueEquals {
                        key: keys[0].clone(),
                        value: read.entries[0]
                            .as_ref()
                            .expect("authority checked")
                            .value
                            .clone(),
                    },
                    KvCompare::Missing {
                        key: keys[1].clone(),
                    },
                    floor_compare,
                ],
                success: vec![KvOp::put_serde(keys[2].clone(), &next, None)?],
                failure: Vec::new(),
            };
            deadline_before(deadline)?;
            // The existing txn API has no deadline override. A submission
            // before the deadline may commit later; report that real commit.
            if self.kv.txn(txn)?.succeeded {
                return Ok(DfsRecoveryBallotBegin::Reserved(request.clone()));
            }
        }
        Ok(DfsRecoveryBallotBegin::Refused(
            DfsRecoveryBallotRefusal::Busy,
        ))
    }

    /// sinomo:kv-consistency: Cancel only the exact pending recovery proposal by value CAS, preserving its monotonic floor even if serving authority has changed.
    pub(crate) fn cancel_metadata_shard_recovery_ballot_reservation(
        &mut self,
        request: &DfsMetadataShardRecoveryBallotRequest,
        deadline: Instant,
    ) -> Result<bool, KvError> {
        request
            .validate()
            .map_err(|reason| malformed(format!("invalid cancellation request: {reason:?}")))?;
        deadline_before(deadline)?;
        let key = metadata_shard_recovery_ballot_key(
            &request.serving_shard.file_system_id,
            &request.serving_shard.shard_id,
        );
        let entries = self
            .kv
            .get_authoritative_consistent_before(std::slice::from_ref(&key), deadline)?;
        deadline_before(deadline)?;
        exact_entries(std::slice::from_ref(&key), &entries)?;
        let Some(mut floor) = decode_floor(
            entries[0].as_ref(),
            &request.serving_shard.file_system_id,
            &request.serving_shard.shard_id,
        )?
        else {
            return Ok(false);
        };
        if floor.pending.as_ref() != Some(request) {
            return Ok(false);
        }
        floor.pending = None;
        let txn = KvTxn {
            compares: vec![KvCompare::ValueEquals {
                key: key.clone(),
                value: entries[0].as_ref().expect("floor present").value.clone(),
            }],
            success: vec![KvOp::put_serde(key, &floor, None)?],
            failure: Vec::new(),
        };
        deadline_before(deadline)?;
        Ok(self.kv.txn(txn)?.succeeded)
    }

    /// sinomo:kv-consistency: Verify the exact pending proposal against the acceptor's own current authoritative serving and handoff view; this is DATA, not a promise or lease.
    pub(crate) fn verify_metadata_shard_recovery_ballot_before(
        &mut self,
        request: &DfsMetadataShardRecoveryBallotRequest,
        local: &NodeId,
        deadline: Instant,
    ) -> Result<DfsRecoveryBallotVerify, KvError> {
        if let Err(reason) = request.validate() {
            return Ok(DfsRecoveryBallotVerify::Refused(reason));
        }
        let read = self.read_recovery_ballot_before(
            &request.serving_shard.file_system_id,
            &request.serving_shard.shard_id,
            deadline,
        )?;
        if read.floor.as_ref().is_none_or(|floor| {
            floor.attempt_floor != request.ballot.attempt || floor.pending.as_ref() != Some(request)
        }) {
            return Ok(DfsRecoveryBallotVerify::Refused(
                DfsRecoveryBallotRefusal::NotPending,
            ));
        }
        if let Some(reason) = authority_refusal(&read, request)? {
            return Ok(DfsRecoveryBallotVerify::Refused(reason));
        }
        if local != &request.serving_shard.primary_node_id
            && !request.serving_shard.replica_node_ids.contains(local)
        {
            return Ok(DfsRecoveryBallotVerify::Refused(
                DfsRecoveryBallotRefusal::NotMember,
            ));
        }
        deadline_before(deadline)?;
        Ok(DfsRecoveryBallotVerify::Verified(request.clone()))
    }
}
