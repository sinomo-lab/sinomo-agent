//! Pure consumers of the existing coherent write configuration projection.

use crate::{
    DfsFileSystemConfig, DfsFileSystemId, DfsNodeAdvertisement, DfsWriteConfigurationProjection,
};
use sinomo_core::kv::{KvEntry, KvError};

impl DfsWriteConfigurationProjection {
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
