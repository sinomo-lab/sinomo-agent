use super::*;
use sinomo_core::storage::migration::{
    MigrationAdvance, MigrationBatch, MigrationBudget, MigrationDriver, MigrationIdentity,
    MigrationJob, MigrationJobStore, MigrationPhase, MigrationPublication, MigrationStep,
};

struct ActivationDriver<'a> {
    store: &'a LocalContentStore,
    incarnation: String,
}

impl ActivationDriver<'_> {
    fn sentinel(&self) -> Result<ContentStoreReservationBatchSentinel> {
        let sentinel = decode_content_reservation_batch_sentinel(&read_file(
            &self.store.content_reservation_batch_sentinel_path(),
        )?)?;
        validate_content_reservation_batch_sentinel(&sentinel)?;
        if sentinel.store_incarnation != self.incarnation {
            return Err(inline_error("activation store incarnation changed"));
        }
        Ok(sentinel)
    }

    fn initial_catalog(&self) -> InlineCatalog {
        InlineCatalog {
            format_version: INLINE_FORMAT_VERSION,
            store_incarnation: self.incarnation.clone(),
            generation: 1,
            legacy_files_are_authoritative: true,
            segments: vec![InlineSegment {
                id: 0,
                sealed_end: None,
                sealed_digest: None,
            }],
        }
    }
}

impl MigrationDriver for ActivationDriver<'_> {
    fn source_is_current(&mut self, _: &MigrationJob) -> std::result::Result<bool, String> {
        self.sentinel()
            .map(|sentinel| {
                sentinel.content_binding_layout == CONTENT_BINDING_LAYOUT
                    || sentinel.content_binding_layout == INLINE_BINDING_LAYOUT
            })
            .map_err(|error| error.to_string())
    }

    fn copy_batch(
        &mut self,
        _: &MigrationJob,
        _: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        let result = (|| -> Result<MigrationBatch> {
            let root = &self.store.inline_store.owner.root;
            let bindings = root.join(INLINE_BINDING_DIR);
            durable_file::ensure_directory(&bindings).map_err(content_durable_file_error)?;
            durable_file::ensure_directory(&bindings.join("generations"))
                .map_err(content_durable_file_error)?;
            let generation = generation_root(root, 1);
            durable_file::ensure_directory(&generation).map_err(content_durable_file_error)?;
            let lease_path = generation.join("readers.lock");
            let mut lease = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lease_path)
                .map_err(|source| ContentStoreError::Io {
                    path: lease_path.clone(),
                    source,
                })?;
            durable_file::sync_open_file(
                &mut lease,
                &lease_path,
                durable_file::DurableFileSync::All,
            )
            .map_err(content_durable_file_error)?;
            sync_dir(&generation)?;
            let writer = FrameAppender::open(&segment_path(root, 1, 0), frame_limits())
                .map_err(inline_error)?;
            if writer.durable_prefix().end() != 0 {
                return Err(inline_error(
                    "legacy-gated activation output contains unexpected binding records",
                ));
            }
            let catalog = self.initial_catalog();
            let bytes = encode_catalog(&catalog)?;
            let path = catalog_path(root);
            if path.exists() {
                if read_catalog(root, &self.incarnation)? != catalog {
                    return Err(inline_error(
                        "partial activation has a different generation catalog",
                    ));
                }
            } else {
                publish_catalog(root, &catalog)?;
            }
            certify_catalog(root, &catalog)?;
            Ok(MigrationBatch {
                cursor: Vec::new(),
                records: 1,
                bytes: bytes.len(),
                complete: true,
            })
        })();
        result.map_err(|error| error.to_string())
    }

    fn verify_batch(
        &mut self,
        _: &MigrationJob,
        _: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        let result = (|| -> Result<MigrationBatch> {
            let root = &self.store.inline_store.owner.root;
            let catalog = read_catalog(root, &self.incarnation)?;
            if catalog != self.initial_catalog() {
                return Err(inline_error("activation catalog verification failed"));
            }
            certify_catalog(root, &catalog)?;
            let (view, end) = replay_catalog(root, &catalog)?;
            if view.sequence != 0 || end != 0 {
                return Err(inline_error(
                    "activation candidate is not the expected empty overlay",
                ));
            }
            let sentinel = self.sentinel()?;
            if sentinel.content_binding_layout != CONTENT_BINDING_LAYOUT {
                return Err(inline_error(
                    "activation source gate changed before publication",
                ));
            }
            // No legacy payload/manifest is rewritten or removed. The candidate
            // explicitly preserves the same owner-fenced authoritative tree,
            // so every old write carried into the drain remains reachable.
            Ok(MigrationBatch {
                cursor: Vec::new(),
                records: 1,
                bytes: 0,
                complete: true,
            })
        })();
        result.map_err(|error| error.to_string())
    }

    fn publish(&mut self, _: &MigrationJob) -> std::result::Result<MigrationPublication, String> {
        let result = (|| -> Result<MigrationPublication> {
            let mut sentinel = self.sentinel()?;
            let catalog = read_catalog(&self.store.inline_store.owner.root, &self.incarnation)?;
            certify_catalog(&self.store.inline_store.owner.root, &catalog)?;
            if sentinel.content_binding_layout == INLINE_BINDING_LAYOUT {
                return Ok(MigrationPublication::Published);
            }
            if sentinel.content_binding_layout != CONTENT_BINDING_LAYOUT {
                return Err(inline_error("unsupported activation source"));
            }
            sentinel.content_binding_layout = INLINE_BINDING_LAYOUT.to_owned();
            // This mandatory, old-reader-rejected sentinel is the last activation
            // edge. Keep the original incarnation and current catalog scan epoch.
            write_file_atomic(
                &self.store.content_reservation_batch_sentinel_path(),
                &encode_content_reservation_batch_sentinel(&sentinel)?,
            )?;
            sync_dir(&self.store.root.join(CONTENT_RESERVATION_DIR))?;
            sync_dir(&self.store.root)?;
            Ok(MigrationPublication::Published)
        })();
        result.map_err(|error| error.to_string())
    }
}

impl LocalContentStore {
    /// Only a unique mutable, newly acquired main data owner can request this.
    /// Generic library construction and shadow factory construction do not
    /// activate layouts. Live store/reader leases cause a typed refusal.
    pub(crate) fn activate_inline_bindings(
        &mut self,
        permit: &mut crate::runtime_host::DfsColdContentActivation<'_>,
    ) -> Result<()> {
        if let Some(error) = self.inline_store.identity_error.as_ref() {
            return Err(inline_error(error));
        }
        let canonical = root_identity::canonical_existing_root(&self.root).map_err(|source| {
            ContentStoreError::Io {
                path: self.root.clone(),
                source,
            }
        })?;
        let permitted =
            root_identity::canonical_existing_root(permit.content_root()).map_err(|source| {
                ContentStoreError::Io {
                    path: permit.content_root().to_owned(),
                    source,
                }
            })?;
        if canonical != permitted {
            return Err(inline_error(
                "cold activation permit belongs to another content root",
            ));
        }
        let _owner = ContentMutationGuard::acquire(&canonical, true)?;
        let _reservation = self.lock_content_reservations_for_write()?;
        let sentinel = decode_content_reservation_batch_sentinel(&read_file(
            &self.content_reservation_batch_sentinel_path(),
        )?)?;
        validate_content_reservation_batch_sentinel(&sentinel)?;
        if sentinel.content_binding_layout == INLINE_BINDING_LAYOUT {
            // A crash may publish the mandatory marker before the runner's
            // completion checkpoint. Resume that exact final step; a visible
            // marker never bypasses catalog namespace recertification.
            let job_store =
                MigrationJobStore::new(canonical.join(INLINE_BINDING_DIR).join("activation.sobj"));
            if let Some(job) = job_store.read().map_err(inline_error)?
                && job.phase != MigrationPhase::Complete
            {
                if job.phase != MigrationPhase::Publishing {
                    return Err(inline_error(
                        "active marker precedes verified activation publication",
                    ));
                }
                let mut driver = ActivationDriver {
                    store: self,
                    incarnation: sentinel.store_incarnation.clone(),
                };
                if !matches!(
                    job_store
                        .advance(
                            &job.identity,
                            MigrationBudget {
                                max_records: 1,
                                max_bytes: 1024 * 1024,
                                max_duration: Duration::from_millis(50)
                            },
                            &AtomicBool::new(false),
                            &mut driver
                        )
                        .map_err(inline_error)?,
                    MigrationAdvance::Complete(_)
                ) {
                    return Err(inline_error(
                        "activation publication checkpoint did not complete",
                    ));
                }
            }
            let mut state = self
                .inline_store
                .owner
                .state
                .lock()
                .map_err(|_| inline_error("resident binding mutex poisoned"))?;
            ensure_writer(&canonical, &sentinel.store_incarnation, &mut state)?;
            return Ok(());
        }

        // Exclude lease construction from the final liveness observation until
        // sentinel publication. Recheck aliases against the now-existing root;
        // precreation inspection cannot evade the drain using a second spelling.
        let stores = INLINE_STORES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| inline_error("content owner registry poisoned"))?;
        let owners = stores
            .values()
            .filter_map(Weak::upgrade)
            .filter(|owner| {
                root_identity::normalized_root_key(&owner.root).is_ok_and(|root| root == canonical)
            })
            .collect::<Vec<_>>();
        let mut gates = Vec::new();
        for owner in &owners {
            gates.push(
                owner
                    .handles
                    .lock()
                    .map_err(|_| inline_error("content lease gate poisoned"))?,
            );
        }
        let mut remaining = 0;
        for gate in &gates {
            if let Some(handle) = gate.upgrade() {
                let observed = Arc::strong_count(&handle).saturating_sub(1);
                remaining += if Arc::ptr_eq(&handle, &self.inline_store.handle) {
                    observed.saturating_sub(1)
                } else {
                    observed
                };
            }
        }
        if remaining != 0 {
            return Err(ContentStoreError::InlineActivationBusy {
                root: canonical,
                live_leases: remaining,
            });
        }

        durable_file::ensure_directory(&canonical.join(INLINE_BINDING_DIR))
            .map_err(content_durable_file_error)?;
        let job =
            MigrationJobStore::new(canonical.join(INLINE_BINDING_DIR).join("activation.sobj"));
        let identity = MigrationIdentity {
            owner: "sinomo-dfs".into(),
            migration: "content-inline-bindings-v1".into(),
            scope: canonical.display().to_string(),
            source_version: 1,
            target_version: 2,
            source_generation: sentinel.store_incarnation.clone(),
        };
        let mut driver = ActivationDriver {
            store: self,
            incarnation: sentinel.store_incarnation.clone(),
        };
        let cancelled = AtomicBool::new(false);
        loop {
            match job
                .advance(
                    &identity,
                    MigrationBudget {
                        max_records: 1,
                        max_bytes: 1024 * 1024,
                        max_duration: Duration::from_millis(50),
                    },
                    &cancelled,
                    &mut driver,
                )
                .map_err(inline_error)?
            {
                MigrationAdvance::Complete(_) => break,
                MigrationAdvance::Progress(_) => {}
                MigrationAdvance::Busy => {
                    return Err(inline_error("activation migration executor is busy"));
                }
                MigrationAdvance::Yielded => {
                    return Err(inline_error(
                        "activation yielded without a completed durable step",
                    ));
                }
                MigrationAdvance::Superseded(_) => {
                    return Err(inline_error("activation source was superseded"));
                }
            }
        }
        drop(driver);
        drop(gates);
        drop(stores);
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        ensure_writer(&canonical, &sentinel.store_incarnation, &mut state)?;
        Ok(())
    }
}
