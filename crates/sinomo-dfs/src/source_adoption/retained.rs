//! Recovery shares only the actual retained owner and opaque native captures.
//! A fresh component must establish its own current authority before using them.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsSourceAdoptionCloseDisposition {
    Drained,
    Retained,
}

/// Storage cleanup cannot remove a root still owned by any actual source
/// custody entry. Poison is unavailable evidence, never a drained result.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn retained_custody_blocks_cleanup(root: &std::path::Path) -> Result<bool> {
    let Some(retained) = RETAINED.get() else {
        return Ok(false);
    };
    let retained = retained
        .lock()
        .map_err(|_| error("source custody registry poisoned"))?;
    Ok(retained.keys().any(|(actual_root, _)| actual_root == root))
}

pub(super) fn predecessors(
    root: &std::path::Path,
    node: &NodeId,
    cluster_id: &sinomo_core::Id,
    owner: &Arc<DfsDataOwnerLock>,
) -> Result<Vec<Arc<DfsSourceAdoptionActor>>> {
    let retained = RETAINED
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| error("source custody registry poisoned"))?;
    let mut actors = Vec::new();
    for ((actual_root, _), actor) in retained.iter() {
        if actual_root != root {
            continue;
        }
        if actor.node != *node
            || &actor.cluster_id != cluster_id
            || !Arc::ptr_eq(&actor._owner, owner)
            || (!*actor.shutdown.borrow() && actor.shutdown.has_changed().is_ok())
        {
            return Err(error(
                "retained source owner still belongs to another live component",
            ));
        }
        if !actors.iter().any(|previous| Arc::ptr_eq(previous, actor)) {
            actors.push(actor.clone());
        }
    }
    Ok(actors)
}

pub(crate) fn retained_owner_for_mount(
    context: &sinomo_module::ComponentContext,
) -> std::result::Result<Option<Arc<DfsDataOwnerLock>>, sinomo_module::ComponentError> {
    let resolve = || -> Result<Option<Arc<DfsDataOwnerLock>>> {
        if !matches!(
            context.host_role(),
            sinomo_module::ComponentHostRole::Main | sinomo_module::ComponentHostRole::Embedded
        ) {
            return Err(error("Shadow cannot inherit source custody"));
        }
        let root = context.data_dir().root();
        let actor = ACTORS
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source actor registry poisoned"))?
            .get(&root)
            .and_then(Weak::upgrade);
        let Some(actor) = actor else {
            return Ok(None);
        };
        let has_custody = RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .keys()
            .any(|(actual_root, _)| actual_root == &root);
        if !has_custody {
            return Ok(None);
        }
        if !*actor.shutdown.borrow() && actor.shutdown.has_changed().is_ok() {
            return Err(error("retained source actor is still mounted"));
        }
        if crate::runtime_host::registered_dfs_runtime_host(context.data_dir())
            .map_err(error)?
            .is_some()
        {
            return Err(error(
                "previous native host has not released its actual lifecycle",
            ));
        }
        context
            .cluster_access()
            .ok_or_else(|| error("current mount has no native cluster principal"))?;
        let state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(
            &sinomo_core::network::ClusterPaths::new(context.data_dir()),
        )
        .map_err(error)?;
        let paths = crate::runtime_host::DfsRuntimePaths::for_host_role(
            context.data_dir(),
            context.host_role(),
        );
        let registered =
            crate::runtime_host::require_daemon_dfs_data_owner_lock(&paths).map_err(error)?;
        if state.local_node_id != actor.node
            || state.cluster_id != actor.cluster_id
            || !Arc::ptr_eq(&registered, &actor._owner)
        {
            return Err(error(
                "retained owner does not match actual current principal/registered FD",
            ));
        }
        Ok(Some(actor._owner.clone()))
    };
    resolve().map_err(|cause| {
        sinomo_module::ComponentError::component(crate::SINOMO_DFS_MODULE_LEGACY_ID, cause)
    })
}

impl DfsSourceAdoptionActor {
    /// The normal host already joined its native children. Pending effects may
    /// outlive that host only in the existing exact actor/FD custody registry.
    pub(crate) fn closed_custody_disposition(&self) -> Result<DfsSourceAdoptionCloseDisposition> {
        let mut pending = Vec::new();
        for actor in std::iter::once(self).chain(self.predecessors.iter().map(Arc::as_ref)) {
            let mut keys = actor
                .sessions
                .lock()
                .map_err(|_| error("source sessions poisoned"))?
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            keys.extend(
                actor
                    .job_sessions
                    .lock()
                    .map_err(|_| error("original jobs poisoned"))?
                    .keys()
                    .map(|key| format!("job:{key}")),
            );
            for (key, preparation) in actor
                .source_preparations
                .lock()
                .map_err(|_| error("source preparations poisoned"))?
                .iter()
            {
                if preparation.holds_unbound()? {
                    keys.push(format!("preparation:{key}"));
                }
            }
            if !keys.is_empty() {
                if actor.data_root != self.data_root
                    || actor.node != self.node
                    || actor.cluster_id != self.cluster_id
                    || !Arc::ptr_eq(&actor._owner, &self._owner)
                    || (!*actor.shutdown.borrow() && actor.shutdown.has_changed().is_ok())
                {
                    return Err(error(
                        "source close does not own the same closed native actor/FD",
                    ));
                }
                pending.push((actor, keys));
            }
        }
        if pending.is_empty() {
            return Ok(DfsSourceAdoptionCloseDisposition::Drained);
        }
        if !*self.shutdown.borrow() && self.shutdown.has_changed().is_ok() {
            return Err(error(
                "source close still belongs to a live native component",
            ));
        }
        let paths = crate::runtime_host::DfsRuntimePaths::new(
            &sinomo_core::local::SinomoDataDir::new(self.data_root.clone()),
        );
        let registered =
            crate::runtime_host::require_daemon_dfs_data_owner_lock(&paths).map_err(error)?;
        if !Arc::ptr_eq(&registered, &self._owner) {
            return Err(error("source close lost its actual registered native FD"));
        }
        let retained = RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?;
        for (actor, keys) in pending {
            for key in keys {
                let Some(held) = retained.get(&(actor.data_root.clone(), key)) else {
                    return Err(error(
                        "pending source effect lacks retained native actor custody",
                    ));
                };
                if !std::ptr::eq(held.as_ref(), actor) || !Arc::ptr_eq(&held._owner, &registered) {
                    return Err(error(
                        "pending source effect retained another actor or native FD",
                    ));
                }
            }
        }
        Ok(DfsSourceAdoptionCloseDisposition::Retained)
    }

    pub(super) fn retain_current_session(&self, key: String) -> Result<()> {
        let current = actor_for_root(&self.data_root)?;
        if !std::ptr::eq(current.as_ref(), self)
            || *self.shutdown.borrow()
            || self.shutdown.has_changed().is_err()
        {
            return Err(error(
                "fresh capture owner is not the actual current mounted actor",
            ));
        }
        RETAINED
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| error("source custody registry poisoned"))?
            .insert((self.data_root.clone(), key), current);
        Ok(())
    }
    pub(super) fn source_session(
        &self,
        offer: &DfsSourceAdoptionOffer,
        effect: bool,
    ) -> Result<Arc<SourceSession>> {
        if let Some(session) = self
            .sessions
            .lock()
            .map_err(|_| error("source sessions poisoned"))?
            .get(&offer.session_id)
            .cloned()
        {
            if session.offer != *offer {
                return Err(error("current source session offer changed"));
            }
            return Ok(session);
        }
        for previous in &self.predecessors {
            let mut old = previous
                .sessions
                .lock()
                .map_err(|_| error("retained source sessions poisoned"))?;
            let Some(session) = old.get(&offer.session_id).cloned() else {
                continue;
            };
            if session.offer != *offer {
                return Err(error("retained source offer changed"));
            }
            if !effect {
                return Ok(session);
            }
            self.require_current(offer)?;
            self.verify_original_job(offer, &session.capture.family().source_object.path)?;
            let generation = self.authority.shard_projection_state(
                &offer.source_config.file_system_id,
                &crate::DfsMetadataShardId::new(&offer.source_config.shard_id),
            );
            if !generation.1 {
                return Err(error(
                    "fresh source scope is not ready for retained custody",
                ));
            }
            let fresh = Arc::new(SourceSession {
                capture: session.capture.clone(),
                generation: generation.0,
                offer: offer.clone(),
                claim: session.claim.clone(),
            });
            self.retain_current_session(offer.session_id.clone())?;
            old.remove(&offer.session_id);
            self.sessions
                .lock()
                .map_err(|_| error("source sessions poisoned"))?
                .insert(offer.session_id.clone(), fresh.clone());
            return Ok(fresh);
        }
        Err(error("source session no longer exists"))
    }
    pub(super) fn original_job_session(
        &self,
        offer: &DfsSourceAdoptionJobOffer,
        effect: bool,
    ) -> Result<Arc<job_custody::OriginalJobSession>> {
        if let Some(session) = self
            .job_sessions
            .lock()
            .map_err(|_| error("original jobs poisoned"))?
            .get(&offer.session_id)
            .cloned()
        {
            if session.offer != *offer {
                return Err(error("current original job session offer changed"));
            }
            return Ok(session);
        }
        for previous in &self.predecessors {
            let mut old = previous
                .job_sessions
                .lock()
                .map_err(|_| error("retained original jobs poisoned"))?;
            let Some(session) = old.get(&offer.session_id).cloned() else {
                continue;
            };
            if session.offer != *offer {
                return Err(error("retained original job offer changed"));
            }
            if !effect {
                return Ok(session);
            }
            if *self.shutdown.borrow() || self.shutdown.has_changed().is_err() {
                return Err(error("fresh job owner is closed"));
            }
            let configs = self
                .authority
                .cached_metadata_shards(
                    &offer.job_config.file_system_id,
                    self.authority.projection_generation(),
                )
                .map_err(error)?
                .ok_or_else(|| error("fresh original job configuration unavailable"))?;
            let generation = self.authority.shard_projection_state(
                &offer.job_config.file_system_id,
                &crate::DfsMetadataShardId::new(&offer.job_config.shard_id),
            );
            if !generation.1
                || offer.job_config.primary_node_id != self.node
                || !configs.contains(&offer.job_config)
                || !configs.contains(&session.source)
                || !configs.contains(&session.target)
            {
                return Err(error(
                    "fresh original job authority does not match retained custody",
                ));
            }
            let fresh = Arc::new(job_custody::OriginalJobSession {
                capture: session.capture.clone(),
                offer: offer.clone(),
                generation: generation.0,
                source: session.source.clone(),
                target: session.target.clone(),
                destination: session.destination.clone(),
                source_path: session.source_path.clone(),
            });
            self.retain_current_session(format!("job:{}", offer.session_id))?;
            old.remove(&offer.session_id);
            self.job_sessions
                .lock()
                .map_err(|_| error("original jobs poisoned"))?
                .insert(offer.session_id.clone(), fresh.clone());
            return Ok(fresh);
        }
        Err(error("original job custody no longer exists"))
    }
}
