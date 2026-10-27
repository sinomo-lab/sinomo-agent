//! Pure consumers of the existing coherent write configuration projection.

use crate::{
    DfsFileSystemConfig, DfsFileSystemId, DfsNodeAdvertisement, DfsWriteConfigurationProjection,
};
use sinomo_core::kv::{ClusterKvRaftConfigRecord, KvEntry, KvError, KvRevision};
use sinomo_core::node::NodeId;
use sinomo_core::service::{
    ServiceDiscoveryHealth, ServiceDiscoveryLocality, ServiceInstanceRecord,
};
use std::collections::{BTreeMap, BTreeSet};

/// One target's configured admission and separate operational observation.
/// This is neither a holder/receipt claim nor permission to send an effect.
#[derive(Clone, Debug)]
pub(crate) struct DfsAdmittedContentTarget {
    pub(crate) advertisement: DfsNodeAdvertisement,
    pub(crate) provider_observed_ready: bool,
}

/// Pure input from one complete, floor-checked configuration. The caller still
/// owns all native admission, policy, physical-store and publication checks.
#[derive(Clone, Debug)]
pub(crate) struct DfsAdmittedContentTargets {
    pub(crate) configuration_revision: KvRevision,
    pub(crate) targets: Vec<DfsAdmittedContentTarget>,
}

impl DfsWriteConfigurationProjection {
    /// Join configured storage targets from this same retained cut, with no
    /// local, historical-location or legacy empty-provider fallback. Service
    /// health only describes current operation readiness; temporary failure
    /// does not revoke a compatible provider's desired-target eligibility.
    pub(crate) fn admitted_content_targets(&self) -> Result<DfsAdmittedContentTargets, KvError> {
        fn invalid(detail: impl Into<String>) -> KvError {
            KvError::Serialization {
                detail: detail.into(),
            }
        }
        let floor = self.floor.value.as_ref().ok_or_else(|| {
            invalid("content target inventory has no retained native configuration floor")
        })?;
        let config: ClusterKvRaftConfigRecord = floor.decode()?;
        let members = self.entries("/cluster/members/").ok_or_else(|| {
            invalid("content target inventory lacks its complete membership scope")
        })?;
        let mut authorized = BTreeSet::new();
        for entry in members {
            let member: sinomo_core::ClusterMemberRecord = entry.decode()?;
            if entry.key != sinomo_core::node::cluster_member_key(&member.node_id) {
                return Err(invalid(
                    "content target membership key differs from its node",
                ));
            }
            if member.cluster_id == config.cluster_id && member.state.is_authorized() {
                authorized.insert(member.node_id);
            }
        }
        let services = self
            .entries(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
            .ok_or_else(|| invalid("content target inventory lacks its complete provider scope"))?;
        let current_interface = format!("sinomo.object-rpc/{}", crate::DFS_RPC_CATEGORY);
        let mut providers = BTreeMap::<NodeId, bool>::new();
        for entry in services {
            let provider: ServiceInstanceRecord = entry.decode()?;
            if provider.service_id.is_empty()
                || entry.key
                    != sinomo_core::service::cluster_service_instance_key(
                        &provider.service_id,
                        &provider.node_id,
                    )
            {
                return Err(invalid(
                    "content target provider key differs from its identity",
                ));
            }
            if provider.interface_id == current_interface
                && provider.interface_version == crate::DFS_PROTOCOL
                && provider.locality == ServiceDiscoveryLocality::Cluster
                && provider
                    .provider_capabilities
                    .iter()
                    .any(|capability| capability == "dfs.object.transfer")
            {
                let ready = matches!(
                    provider.health,
                    ServiceDiscoveryHealth::Ready | ServiceDiscoveryHealth::Degraded
                );
                providers
                    .entry(provider.node_id)
                    .and_modify(|current| *current |= ready)
                    .or_insert(ready);
            }
        }
        let content = self
            .entries(crate::content_services_prefix())
            .ok_or_else(|| invalid("content target inventory lacks its complete content scope"))?;
        let mut targets = Vec::new();
        for entry in content {
            let advertisement: DfsNodeAdvertisement = entry.decode()?;
            if entry.key != crate::content_service_key(&advertisement.node_id) {
                return Err(invalid(
                    "content target advertisement key differs from its node",
                ));
            }
            if advertisement.accepts_content
                && authorized.contains(&advertisement.node_id)
                && let Some(provider_observed_ready) = providers.get(&advertisement.node_id)
            {
                targets.push(DfsAdmittedContentTarget {
                    advertisement,
                    provider_observed_ready: *provider_observed_ready,
                });
            }
        }
        Ok(DfsAdmittedContentTargets {
            configuration_revision: self.revision(),
            targets,
        })
    }

    pub(crate) fn file_system_config(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystemConfig>, KvError> {
        let key = crate::file_system_config_key(file_system_id);
        self.entries(&key)
            .and_then(|entries| entries.iter().find(|entry| entry.key == key))
            .map(KvEntry::decode)
            .transpose()
    }

    pub(crate) fn content_advertisements(&self) -> Result<Vec<DfsNodeAdvertisement>, KvError> {
        self.entries(crate::content_services_prefix())
            .expect("write projection owns the complete content-service scope")
            .iter()
            .map(KvEntry::decode)
            .collect()
    }

    pub(crate) fn content_replica_candidates(
        &self,
    ) -> Result<Vec<sinomo_core::node::NodeId>, crate::DfsError> {
        crate::mounted_sifs::content_replica_candidates_from_configuration_entries(
            self.entries(crate::content_services_prefix())
                .expect("write projection owns the complete content-service scope"),
            self.entries(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
                .expect("write projection owns the complete provider scope"),
        )
    }
}

#[cfg(test)]
mod admitted_target_tests {
    use super::*;
    use crate::write_configuration_projection::{
        DFS_WRITE_CONFIGURATION_SCOPE_COUNT, DfsWriteConfigurationState,
    };
    use sinomo_core::kv::{
        ClusterKvCommittedDelta, ClusterKvCommittedKeyState, KvAuthoritativeRead, KvAuthorityBasis,
        KvAuthorityProof, KvEvent, KvEventKind, KvLocalRead, KvVersion, cluster_kv_raft_config_key,
        encode_kv_value,
    };
    use std::sync::Arc;

    // Exercise the actual complete-prefix/floor/delta state and pure decoder.
    // These supplied native-proof values are not process/consensus evidence.
    struct InventoryFixture {
        node: NodeId,
        cluster: sinomo_core::Id,
        reads: [KvAuthoritativeRead<Vec<KvEntry>>; DFS_WRITE_CONFIGURATION_SCOPE_COUNT],
        floor: KvLocalRead<Option<KvEntry>>,
    }

    fn entry<T: serde::Serialize>(key: String, value: &T) -> KvEntry {
        KvEntry {
            key,
            value: encode_kv_value(value).unwrap(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(1),
            version: KvVersion(1),
            lease_id: None,
        }
    }

    impl InventoryFixture {
        fn new() -> Self {
            let cluster = sinomo_core::Id::derived("content-target-inventory", b"cluster");
            let node = sinomo_core::Id::derived("content-target-inventory", b"target");
            let member = sinomo_core::ClusterMemberRecord::local_voter(
                cluster.clone(),
                node.clone(),
                "target",
                "target-key",
                time::OffsetDateTime::UNIX_EPOCH,
            );
            let advertisement = DfsNodeAdvertisement::content(node.clone());
            let mut provider = ServiceInstanceRecord::new("dfs", node.clone());
            provider.interface_id = format!("sinomo.object-rpc/{}", crate::DFS_RPC_CATEGORY);
            provider.interface_version = crate::DFS_PROTOCOL.into();
            provider.locality = ServiceDiscoveryLocality::Cluster;
            provider.provider_capabilities = vec!["dfs.object.transfer".into()];
            let scopes = [
                Vec::new(),
                vec![entry(crate::content_service_key(&node), &advertisement)],
                vec![entry(
                    sinomo_core::service::cluster_service_instance_key("dfs", &node),
                    &provider,
                )],
                vec![entry(sinomo_core::node::cluster_member_key(&node), &member)],
            ];
            let config = ClusterKvRaftConfigRecord {
                cluster_id: cluster.clone(),
                term: 7,
                kv_revision: 1,
                voters: vec![node.clone()],
                learners: Vec::new(),
                non_voting: Vec::new(),
                updated_at: time::OffsetDateTime::UNIX_EPOCH,
            };
            Self {
                node,
                cluster,
                reads: scopes.map(|value| KvAuthoritativeRead {
                    value,
                    authority: KvAuthorityProof {
                        term: 7,
                        commit_index: 110,
                        revision: KvRevision(10),
                        basis: KvAuthorityBasis::CurrentTermQuorum {
                            voter_acks: 1,
                            required_quorum: 1,
                        },
                    },
                }),
                floor: KvLocalRead {
                    value: Some(entry(cluster_kv_raft_config_key(), &config)),
                    revision: KvRevision(10),
                    observed_term: 7,
                    applied_index: 110,
                    known_leader: None,
                },
            }
        }

        fn at_revision(&mut self, revision: u64) {
            for read in &mut self.reads {
                read.authority.revision = KvRevision(revision);
                read.authority.commit_index = revision + 100;
            }
            self.floor.revision = KvRevision(revision);
            self.floor.applied_index = revision + 100;
        }

        fn state_with_token(&self) -> (DfsWriteConfigurationState, u64) {
            let mut state = DfsWriteConfigurationState::new();
            let token = state.begin_refresh();
            assert!(
                state
                    .install_bootstrap(token, self.reads.clone(), self.floor.clone())
                    .unwrap()
            );
            (state, token)
        }

        fn state(&self) -> DfsWriteConfigurationState {
            self.state_with_token().0
        }

        fn view(&self) -> Arc<DfsWriteConfigurationProjection> {
            self.state().snapshot().unwrap()
        }

        fn provider(&self) -> ServiceInstanceRecord {
            self.reads[2].value[0].decode().unwrap()
        }

        fn member(&self) -> sinomo_core::ClusterMemberRecord {
            self.reads[3].value[0].decode().unwrap()
        }

        fn set_provider(&mut self, provider: &ServiceInstanceRecord) {
            self.reads[2].value[0].value = encode_kv_value(provider).unwrap();
        }

        fn set_member(&mut self, member: &sinomo_core::ClusterMemberRecord) {
            self.reads[3].value[0].value = encode_kv_value(member).unwrap();
        }
    }

    fn change(revision: u64, key: String, mut value: Option<KvEntry>) -> ClusterKvCommittedDelta {
        if let Some(entry) = &mut value {
            entry.modify_revision = KvRevision(revision);
            entry.version = KvVersion(revision);
        }
        ClusterKvCommittedDelta {
            revision: KvRevision(revision),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(revision),
                key: key.clone(),
                kind: value
                    .as_ref()
                    .map_or(KvEventKind::Delete, |entry| KvEventKind::Put {
                        value: entry.value.clone(),
                    }),
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState { key, entry: value }].into(),
        }
    }

    #[test]
    fn admitted_content_targets_join_four_proven_scopes_without_legacy_fallback() {
        let fixture = InventoryFixture::new();
        let inventory = fixture.view().admitted_content_targets().unwrap();
        assert_eq!(inventory.configuration_revision, KvRevision(10));
        assert_eq!(inventory.targets.len(), 1);
        assert_eq!(inventory.targets[0].advertisement.node_id, fixture.node);
        assert!(inventory.targets[0].provider_observed_ready);
        for scope in [1, 2, 3] {
            let mut empty = InventoryFixture::new();
            empty.reads[scope].value.clear();
            assert!(
                empty
                    .view()
                    .admitted_content_targets()
                    .unwrap()
                    .targets
                    .is_empty(),
                "complete but empty scope {scope} cannot grant legacy/local admission"
            );
        }
    }

    #[test]
    fn admitted_content_targets_keep_offline_and_unavailable_provider_desired_only() {
        for (health, ready) in [
            (ServiceDiscoveryHealth::Starting, false),
            (ServiceDiscoveryHealth::Unavailable, false),
            (ServiceDiscoveryHealth::Degraded, true),
        ] {
            let mut fixture = InventoryFixture::new();
            let mut member = fixture.member();
            member.state = sinomo_core::ClusterMemberState::Offline;
            fixture.set_member(&member);
            let mut provider = fixture.provider();
            provider.health = health;
            fixture.set_provider(&provider);
            let inventory = fixture.view().admitted_content_targets().unwrap();
            assert_eq!(
                inventory.targets.len(),
                1,
                "temporary {health:?} does not revoke the target"
            );
            assert_eq!(inventory.targets[0].advertisement.node_id, fixture.node);
            assert_eq!(inventory.targets[0].provider_observed_ready, ready);
        }
    }

    #[test]
    fn admitted_content_targets_exclude_removed_foreign_incompatible_and_refusing_nodes() {
        for failure in [
            "removed",
            "pending",
            "foreign-cluster",
            "interface",
            "protocol",
            "locality",
            "capability",
            "content-refusal",
        ] {
            let mut fixture = InventoryFixture::new();
            match failure {
                "removed" | "pending" | "foreign-cluster" => {
                    let mut member = fixture.member();
                    match failure {
                        "removed" => member.state = sinomo_core::ClusterMemberState::Removed,
                        "pending" => member.state = sinomo_core::ClusterMemberState::Pending,
                        _ => {
                            member.cluster_id =
                                sinomo_core::Id::derived("content-target-inventory", b"foreign")
                        }
                    }
                    fixture.set_member(&member);
                }
                "content-refusal" => {
                    let mut advertisement: DfsNodeAdvertisement =
                        fixture.reads[1].value[0].decode().unwrap();
                    advertisement.accepts_content = false;
                    fixture.reads[1].value[0].value = encode_kv_value(&advertisement).unwrap();
                }
                _ => {
                    let mut provider = fixture.provider();
                    match failure {
                        "interface" => provider.interface_id.push_str("-different"),
                        "protocol" => provider.interface_version = "legacy".into(),
                        "locality" => provider.locality = ServiceDiscoveryLocality::Local,
                        _ => provider.provider_capabilities.clear(),
                    }
                    fixture.set_provider(&provider);
                }
            }
            assert!(
                fixture
                    .view()
                    .admitted_content_targets()
                    .unwrap()
                    .targets
                    .is_empty(),
                "{failure}"
            );
        }
    }

    #[test]
    fn admitted_content_targets_reject_malformed_or_identity_aliased_scope_records() {
        for scope in [1, 2, 3] {
            let mut malformed = InventoryFixture::new();
            malformed.reads[scope].value[0].value = b"not a KV object".to_vec();
            assert!(
                malformed.view().admitted_content_targets().is_err(),
                "scope {scope}"
            );
            let mut aliased = InventoryFixture::new();
            aliased.reads[scope].value[0]
                .key
                .push_str("-another-identity");
            assert!(
                aliased.view().admitted_content_targets().is_err(),
                "scope {scope}"
            );
        }
    }

    #[test]
    fn admitted_content_targets_provider_and_content_deltas_retain_one_current_cut() {
        let fixture = InventoryFixture::new();
        let mut state = fixture.state();
        let before = state.snapshot().unwrap();
        let provider_key = fixture.reads[2].value[0].key.clone();
        assert!(state.observe_delta(&change(11, provider_key.clone(), None)));
        assert!(
            state
                .snapshot()
                .unwrap()
                .admitted_content_targets()
                .unwrap()
                .targets
                .is_empty()
        );
        let mut unavailable = fixture.reads[2].value[0].clone();
        let mut provider = fixture.provider();
        provider.health = ServiceDiscoveryHealth::Unavailable;
        unavailable.value = encode_kv_value(&provider).unwrap();
        assert!(state.observe_delta(&change(12, provider_key, Some(unavailable))));
        let current = state
            .snapshot()
            .unwrap()
            .admitted_content_targets()
            .unwrap();
        assert_eq!(current.configuration_revision, KvRevision(12));
        assert_eq!(current.targets.len(), 1);
        assert!(!current.targets[0].provider_observed_ready);
        let mut refused = fixture.reads[1].value[0].clone();
        let mut advertisement: DfsNodeAdvertisement = refused.decode().unwrap();
        advertisement.accepts_content = false;
        refused.value = encode_kv_value(&advertisement).unwrap();
        assert!(state.observe_delta(&change(13, refused.key.clone(), Some(refused))));
        assert!(
            state
                .snapshot()
                .unwrap()
                .admitted_content_targets()
                .unwrap()
                .targets
                .is_empty()
        );
        assert_eq!(before.revision(), KvRevision(10));
        assert_eq!(
            before.admitted_content_targets().unwrap().targets.len(),
            1,
            "retained old DATA is immutable, not renewed current admission"
        );
    }

    #[test]
    fn admitted_content_targets_membership_revocation_rebuild_cannot_reuse_old_refresh() {
        let mut fixture = InventoryFixture::new();
        let (mut state, old_token) = fixture.state_with_token();
        let mut member = fixture.member();
        member.state = sinomo_core::ClusterMemberState::Removed;
        fixture.set_member(&member);
        let removed = fixture.reads[3].value[0].clone();
        assert!(!state.observe_delta(&change(11, removed.key.clone(), Some(removed))));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        assert!(
            !state
                .install_bootstrap(old_token, fixture.reads.clone(), fixture.floor.clone())
                .unwrap()
        );
        assert!(state.snapshot().is_none());
        fixture.at_revision(11);
        let fresh = state.begin_refresh();
        assert!(
            state
                .install_bootstrap(fresh, fixture.reads.clone(), fixture.floor.clone())
                .unwrap()
        );
        assert!(
            state
                .snapshot()
                .unwrap()
                .admitted_content_targets()
                .unwrap()
                .targets
                .is_empty()
        );
    }

    #[test]
    fn admitted_content_targets_gap_and_changed_floor_withhold_current_inventory() {
        let fixture = InventoryFixture::new();
        let mut state = fixture.state();
        assert!(!state.observe_delta(&change(12, fixture.reads[2].value[0].key.clone(), None)));
        assert!(state.snapshot().is_none() && state.needs_refresh());
        let view = fixture.view();
        let mut changed = fixture.floor.clone();
        let mut config: ClusterKvRaftConfigRecord =
            changed.value.as_ref().unwrap().decode().unwrap();
        assert_eq!(config.cluster_id, fixture.cluster);
        config.cluster_id = sinomo_core::Id::derived("content-target-inventory", b"replacement");
        changed.value.as_mut().unwrap().value = encode_kv_value(&config).unwrap();
        assert!(!view.matches_floor(&changed));
        changed = fixture.floor.clone();
        changed.observed_term += 1;
        assert!(!view.matches_floor(&changed));
    }
}
