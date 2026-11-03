//! Coherent module runtime generations and hot-unload invocation fencing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use sinomo_core::service::{
    ServiceError, ServiceInvocationGate, ServiceInvocationLease, ServiceInvocationMetadata,
};
use thiserror::Error;

use crate::{ExtensionRegistrySnapshot, ModuleRegistrySnapshot};

#[derive(Clone, Debug)]
pub struct ModuleRuntimeGenerationSnapshot {
    pub generation: u64,
    pub module_registry: Arc<ModuleRegistrySnapshot>,
    pub extension_registry: Arc<ExtensionRegistrySnapshot>,
    pub service_registry_generation: u64,
}

#[derive(Clone)]
pub struct ModuleRuntimeGenerationStore {
    active: Arc<ArcSwap<ModuleRuntimeGenerationSnapshot>>,
    update_lock: Arc<Mutex<()>>,
}

impl ModuleRuntimeGenerationStore {
    pub fn new(
        module_registry: Arc<ModuleRegistrySnapshot>,
        extension_registry: Arc<ExtensionRegistrySnapshot>,
        service_registry_generation: u64,
    ) -> Result<Self, ModuleRuntimeGenerationError> {
        validate_generation_pair(&module_registry, &extension_registry)?;
        Ok(Self {
            active: Arc::new(ArcSwap::from_pointee(ModuleRuntimeGenerationSnapshot {
                generation: 1,
                module_registry,
                extension_registry,
                service_registry_generation,
            })),
            update_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn active_snapshot(&self) -> Arc<ModuleRuntimeGenerationSnapshot> {
        self.active.load_full()
    }

    pub fn publish(
        &self,
        module_registry: Arc<ModuleRegistrySnapshot>,
        extension_registry: Arc<ExtensionRegistrySnapshot>,
        service_registry_generation: u64,
    ) -> Result<Arc<ModuleRuntimeGenerationSnapshot>, ModuleRuntimeGenerationError> {
        validate_generation_pair(&module_registry, &extension_registry)?;
        let _guard = self
            .update_lock
            .lock()
            .map_err(|_| ModuleRuntimeGenerationError::Lock {
                detail: "module runtime generation update lock poisoned".to_string(),
            })?;
        let active = self.active.load();
        if module_registry.version() < active.module_registry.version()
            || service_registry_generation < active.service_registry_generation
        {
            return Err(ModuleRuntimeGenerationError::Regression {
                current_module_generation: active.module_registry.version(),
                next_module_generation: module_registry.version(),
                current_service_generation: active.service_registry_generation,
                next_service_generation: service_registry_generation,
            });
        }
        let snapshot = Arc::new(ModuleRuntimeGenerationSnapshot {
            generation: active.generation.saturating_add(1),
            module_registry,
            extension_registry,
            service_registry_generation,
        });
        self.active.store(Arc::clone(&snapshot));
        Ok(snapshot)
    }
}

fn validate_generation_pair(
    module_registry: &ModuleRegistrySnapshot,
    extension_registry: &ExtensionRegistrySnapshot,
) -> Result<(), ModuleRuntimeGenerationError> {
    if module_registry.version() != extension_registry.generation.module_registry_generation {
        return Err(ModuleRuntimeGenerationError::MismatchedGeneration {
            module_generation: module_registry.version(),
            extension_generation: extension_registry.generation.module_registry_generation,
        });
    }
    Ok(())
}

#[derive(Clone)]
pub struct ModuleRuntimeHost {
    pub generations: ModuleRuntimeGenerationStore,
    pub invocations: ModuleInvocationLeaseRegistry,
    pub native_operations: crate::NativeModuleOperationCancellationRegistry,
}

impl ModuleRuntimeHost {
    pub fn new(
        module_registry: Arc<ModuleRegistrySnapshot>,
        extension_registry: Arc<ExtensionRegistrySnapshot>,
        service_registry_generation: u64,
    ) -> Result<Self, ModuleRuntimeGenerationError> {
        let invocations = ModuleInvocationLeaseRegistry::default();
        invocations.activate_registry(module_registry.as_ref())?;
        Ok(Self {
            generations: ModuleRuntimeGenerationStore::new(
                module_registry,
                extension_registry,
                service_registry_generation,
            )?,
            invocations,
            native_operations: crate::NativeModuleOperationCancellationRegistry::default(),
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ModuleRuntimeLifecycleState {
    Active,
    Draining,
    Unloaded,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleRuntimeLifecycleStatus {
    pub module_id: String,
    pub state: ModuleRuntimeLifecycleState,
    pub active_invocations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleDrainOutcome {
    pub module_id: String,
    pub drained: bool,
    pub remaining_invocations: u64,
    pub elapsed: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleHotUnloadReport {
    pub module_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cascade_modules: Vec<String>,
    pub runtime_generation_before: u64,
    pub runtime_generation_after: u64,
    pub module_registry_generation: u64,
    pub service_registry_generation: u64,
    pub invocations_at_drain_start: u64,
    pub native_operations_cancelled: u64,
    pub network_modules_retired: u64,
    pub network_module_retirement_complete: bool,
    pub drained: bool,
    pub remaining_invocations: u64,
    pub logical_unload_complete: bool,
    pub component_teardown_complete: bool,
    pub physical_unload_attempted: bool,
    pub physical_unload_succeeded: bool,
    pub restart_required: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ModuleHotUnloadRequest {
    pub module_id: String,
    pub drain_timeout_ms: u64,
    pub reason: String,
}

impl ModuleHotUnloadRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.module_id.trim().is_empty() {
            return Err("hot-unload module id is required".to_string());
        }
        if self.drain_timeout_ms == 0 || self.drain_timeout_ms > 300_000 {
            return Err("hot-unload drain timeout must be between 1 and 300000ms".to_string());
        }
        if self.reason.trim().is_empty() || self.reason.len() > 4_096 {
            return Err("hot-unload reason must contain 1 to 4096 bytes".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ModuleInvocationLeaseRegistry {
    inner: Arc<Mutex<ModuleInvocationLeaseState>>,
    changed: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct ModuleInvocationLeaseState {
    modules: BTreeMap<String, ModuleInvocationEntry>,
    component_modules: BTreeMap<String, String>,
}

struct ModuleInvocationEntry {
    state: ModuleRuntimeLifecycleState,
    active_invocations: u64,
}

impl ModuleInvocationLeaseRegistry {
    pub fn activate_registry(
        &self,
        registry: &ModuleRegistrySnapshot,
    ) -> Result<(), ModuleInvocationLeaseError> {
        let mut inner = self.lock()?;
        for registration in registry.registry().registrations() {
            let module_id = registration.module_id().to_string();
            let component_id = crate::component_descriptor_from_registration(registration)
                .map_err(|error| ModuleInvocationLeaseError::InvalidComponent {
                    module_id: module_id.clone(),
                    detail: error.to_string(),
                })?
                .component_id;
            inner
                .modules
                .entry(module_id.clone())
                .and_modify(|entry| entry.state = ModuleRuntimeLifecycleState::Active)
                .or_insert(ModuleInvocationEntry {
                    state: ModuleRuntimeLifecycleState::Active,
                    active_invocations: 0,
                });
            inner.component_modules.insert(component_id, module_id);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn activate_module(
        &self,
        module_id: impl Into<String>,
        component_id: impl Into<String>,
    ) -> Result<(), ModuleInvocationLeaseError> {
        let module_id = module_id.into();
        let component_id = component_id.into();
        let mut inner = self.lock()?;
        inner
            .modules
            .entry(module_id.clone())
            .and_modify(|entry| entry.state = ModuleRuntimeLifecycleState::Active)
            .or_insert(ModuleInvocationEntry {
                state: ModuleRuntimeLifecycleState::Active,
                active_invocations: 0,
            });
        inner.component_modules.insert(component_id, module_id);
        drop(inner);
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn acquire(
        &self,
        module_id: &str,
    ) -> Result<ModuleInvocationLease, ModuleInvocationLeaseError> {
        let mut inner = self.lock()?;
        let resolved_module_id = inner
            .component_modules
            .get(module_id)
            .cloned()
            .unwrap_or_else(|| module_id.to_string());
        let Some(entry) = inner.modules.get_mut(&resolved_module_id) else {
            return Err(ModuleInvocationLeaseError::UnknownModule {
                module_id: module_id.to_string(),
            });
        };
        if entry.state != ModuleRuntimeLifecycleState::Active {
            return Err(ModuleInvocationLeaseError::Unavailable {
                module_id: module_id.to_string(),
                state: entry.state,
            });
        }
        entry.active_invocations = entry.active_invocations.saturating_add(1);
        Ok(ModuleInvocationLease {
            inner: Arc::new(ModuleInvocationLeaseInner {
                registry: self.clone(),
                module_id: resolved_module_id,
            }),
        })
    }

    pub fn acquire_if_tracked(
        &self,
        module_or_component_id: &str,
    ) -> Result<Option<ModuleInvocationLease>, ModuleInvocationLeaseError> {
        let tracked = {
            let inner = self.lock()?;
            inner.modules.contains_key(module_or_component_id)
                || inner.component_modules.contains_key(module_or_component_id)
        };
        if tracked {
            self.acquire(module_or_component_id).map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn begin_drain(&self, module_id: &str) -> Result<u64, ModuleInvocationLeaseError> {
        let mut inner = self.lock()?;
        let Some(entry) = inner.modules.get_mut(module_id) else {
            return Err(ModuleInvocationLeaseError::UnknownModule {
                module_id: module_id.to_string(),
            });
        };
        entry.state = ModuleRuntimeLifecycleState::Draining;
        let active = entry.active_invocations;
        drop(inner);
        self.changed.notify_waiters();
        Ok(active)
    }

    pub fn complete_unload(&self, module_id: &str) -> Result<(), ModuleInvocationLeaseError> {
        let mut inner = self.lock()?;
        let Some(entry) = inner.modules.get_mut(module_id) else {
            return Err(ModuleInvocationLeaseError::UnknownModule {
                module_id: module_id.to_string(),
            });
        };
        if entry.active_invocations != 0 {
            return Err(ModuleInvocationLeaseError::StillActive {
                module_id: module_id.to_string(),
                active_invocations: entry.active_invocations,
            });
        }
        entry.state = ModuleRuntimeLifecycleState::Unloaded;
        drop(inner);
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn reactivate(&self, module_id: &str) -> Result<(), ModuleInvocationLeaseError> {
        let mut inner = self.lock()?;
        let entry = inner
            .modules
            .entry(module_id.to_string())
            .or_insert(ModuleInvocationEntry {
                state: ModuleRuntimeLifecycleState::Active,
                active_invocations: 0,
            });
        if entry.active_invocations != 0 {
            return Err(ModuleInvocationLeaseError::StillActive {
                module_id: module_id.to_string(),
                active_invocations: entry.active_invocations,
            });
        }
        entry.state = ModuleRuntimeLifecycleState::Active;
        drop(inner);
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn status(
        &self,
        module_id: &str,
    ) -> Result<ModuleRuntimeLifecycleStatus, ModuleInvocationLeaseError> {
        let inner = self.lock()?;
        let Some(entry) = inner.modules.get(module_id) else {
            return Err(ModuleInvocationLeaseError::UnknownModule {
                module_id: module_id.to_string(),
            });
        };
        Ok(ModuleRuntimeLifecycleStatus {
            module_id: module_id.to_string(),
            state: entry.state,
            active_invocations: entry.active_invocations,
        })
    }

    pub async fn wait_for_drain(
        &self,
        module_id: &str,
        timeout: Duration,
    ) -> Result<ModuleDrainOutcome, ModuleInvocationLeaseError> {
        let started = Instant::now();
        loop {
            let notified = self.changed.notified();
            let active = self.status(module_id)?.active_invocations;
            if active == 0 {
                return Ok(ModuleDrainOutcome {
                    module_id: module_id.to_string(),
                    drained: true,
                    remaining_invocations: 0,
                    elapsed: started.elapsed(),
                });
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Ok(ModuleDrainOutcome {
                    module_id: module_id.to_string(),
                    drained: false,
                    remaining_invocations: active,
                    elapsed: started.elapsed(),
                });
            };
            if tokio::time::timeout(remaining, notified).await.is_err() {
                let active = self.status(module_id)?.active_invocations;
                return Ok(ModuleDrainOutcome {
                    module_id: module_id.to_string(),
                    drained: active == 0,
                    remaining_invocations: active,
                    elapsed: started.elapsed(),
                });
            }
        }
    }

    fn release(&self, module_id: &str) {
        if let Ok(mut inner) = self.inner.lock()
            && let Some(entry) = inner.modules.get_mut(module_id)
        {
            entry.active_invocations = entry.active_invocations.saturating_sub(1);
        }
        self.changed.notify_waiters();
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ModuleInvocationLeaseState>, ModuleInvocationLeaseError>
    {
        self.inner
            .lock()
            .map_err(|_| ModuleInvocationLeaseError::Lock {
                detail: "module invocation lease lock poisoned".to_string(),
            })
    }
}

impl ServiceInvocationGate for ModuleInvocationLeaseRegistry {
    fn enter(
        &self,
        invocation: &ServiceInvocationMetadata,
    ) -> Result<Arc<dyn ServiceInvocationLease>, ServiceError> {
        let Some(module_id) = invocation.component_id.as_deref() else {
            return Ok(Arc::new(()));
        };
        self.acquire_if_tracked(module_id)
            .map(|lease| {
                lease
                    .map(|lease| Arc::new(lease) as Arc<dyn ServiceInvocationLease>)
                    .unwrap_or_else(|| Arc::new(()))
            })
            .map_err(|error| ServiceError::Service(error.to_string()))
    }
}

#[derive(Clone)]
pub struct ModuleInvocationLease {
    inner: Arc<ModuleInvocationLeaseInner>,
}

impl ModuleInvocationLease {
    pub(crate) fn ensure_active(&self) -> Result<(), ModuleInvocationLeaseError> {
        let status = self.inner.registry.status(&self.inner.module_id)?;
        if status.state == ModuleRuntimeLifecycleState::Active {
            Ok(())
        } else {
            Err(ModuleInvocationLeaseError::Unavailable {
                module_id: self.inner.module_id.clone(),
                state: status.state,
            })
        }
    }
}

struct ModuleInvocationLeaseInner {
    registry: ModuleInvocationLeaseRegistry,
    module_id: String,
}

impl std::fmt::Debug for ModuleInvocationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModuleInvocationLease")
            .field("module_id", &self.inner.module_id)
            .finish_non_exhaustive()
    }
}

impl Drop for ModuleInvocationLeaseInner {
    fn drop(&mut self) {
        self.registry.release(&self.module_id);
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ModuleInvocationLeaseError {
    #[error("module invocation lease state is unavailable: {detail}")]
    Lock { detail: String },
    #[error("module `{module_id}` has an invalid component descriptor: {detail}")]
    InvalidComponent { module_id: String, detail: String },
    #[error("module `{module_id}` is not registered with the runtime invocation gate")]
    UnknownModule { module_id: String },
    #[error("module `{module_id}` is {state:?} and cannot accept new invocations")]
    Unavailable {
        module_id: String,
        state: ModuleRuntimeLifecycleState,
    },
    #[error("module `{module_id}` still has {active_invocations} active invocations")]
    StillActive {
        module_id: String,
        active_invocations: u64,
    },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ModuleRuntimeGenerationError {
    #[error("module runtime generation lock failed: {detail}")]
    Lock { detail: String },
    #[error(
        "module generation {module_generation} does not match extension generation {extension_generation}"
    )]
    MismatchedGeneration {
        module_generation: u64,
        extension_generation: u64,
    },
    #[error(
        "module runtime generation regressed from module/service {current_module_generation}/{current_service_generation} to {next_module_generation}/{next_service_generation}"
    )]
    Regression {
        current_module_generation: u64,
        next_module_generation: u64,
        current_service_generation: u64,
        next_service_generation: u64,
    },
    #[error(transparent)]
    Invocation(#[from] ModuleInvocationLeaseError),
}

// Index the existing host-owned generation store, not a second activation
// registry. Weak entries retain withdrawal after a host has stopped.
fn process_hosts()
-> &'static Mutex<BTreeMap<std::path::PathBuf, std::sync::Weak<ModuleRuntimeHost>>> {
    static HOSTS: std::sync::OnceLock<
        Mutex<BTreeMap<std::path::PathBuf, std::sync::Weak<ModuleRuntimeHost>>>,
    > = std::sync::OnceLock::new();
    HOSTS.get_or_init(Default::default)
}
fn host_key(root: &std::path::Path) -> std::path::PathBuf {
    std::fs::canonicalize(root).unwrap_or_else(|_| {
        if root.is_absolute() {
            root.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(root)
        }
    })
}
pub(crate) fn bind_process_module_host(
    root: &std::path::Path,
    host: ModuleRuntimeHost,
) -> Arc<ModuleRuntimeHost> {
    let mut hosts = process_hosts().lock().expect("module host index poisoned");
    let key = host_key(root);
    if let Some(existing) = hosts.get(&key).and_then(std::sync::Weak::upgrade) {
        assert!(
            Arc::ptr_eq(&existing.generations.active, &host.generations.active),
            "one data directory cannot have multiple live module runtime owners"
        );
        return existing;
    }
    let host = Arc::new(host);
    hosts.insert(key, Arc::downgrade(&host));
    host
}
pub fn process_module_host(
    root: &std::path::Path,
) -> Result<Option<Arc<ModuleRuntimeHost>>, String> {
    let hosts = process_hosts()
        .lock()
        .map_err(|_| "module host index poisoned")?;
    match hosts.get(&host_key(root)) {
        None => Ok(None),
        Some(host) => host
            .upgrade()
            .map(Some)
            .ok_or_else(|| "module runtime host has stopped".into()),
    }
}
/// Resident metadata is authoritative while a daemon hosts this data directory.
/// Standalone tooling falls back to verified loader-state discovery.
pub fn module_registrations_for_host(
    root: &std::path::Path,
) -> Result<Vec<crate::ModuleRegistration>, String> {
    if let Some(host) = process_module_host(root)? {
        Ok(host
            .generations
            .active_snapshot()
            .module_registry
            .registry()
            .registrations()
            .cloned()
            .collect())
    } else {
        crate::active_native_dylib_module_registrations(root).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ModuleId, ModuleRegistration, ModuleRegistry, ModuleRegistrySnapshotStore};
    use sinomo_core::TrustRing;

    fn module_snapshot() -> Arc<ModuleRegistrySnapshot> {
        let registration = ModuleRegistration::native_rust(
            ModuleId::from_legacy_named("module.fixture").expect("module id"),
            "1.0.0",
            "fixture",
            "fixture",
            "1.0.0",
            TrustRing::R1TrustedRuntime,
            "module.fixture",
        );
        let registry = ModuleRegistry::builder()
            .register(registration)
            .expect("registration")
            .try_build()
            .expect("registry");
        ModuleRegistrySnapshotStore::new(registry)
            .expect("store")
            .active_snapshot()
            .expect("snapshot")
    }

    #[tokio::test]
    async fn draining_blocks_new_invocations_and_waits_for_existing_leases() {
        let snapshot = module_snapshot();
        let module_id = snapshot
            .registry()
            .registrations()
            .next()
            .expect("module")
            .module_id()
            .to_string();
        let leases = ModuleInvocationLeaseRegistry::default();
        leases
            .activate_registry(snapshot.as_ref())
            .expect("activate");
        let lease = leases.acquire(&module_id).expect("lease");
        assert_eq!(leases.begin_drain(&module_id).expect("drain"), 1);
        assert!(matches!(
            leases.acquire(&module_id),
            Err(ModuleInvocationLeaseError::Unavailable { .. })
        ));
        let waiting = {
            let leases = leases.clone();
            let module_id = module_id.clone();
            tokio::spawn(async move {
                leases
                    .wait_for_drain(&module_id, Duration::from_secs(1))
                    .await
            })
        };
        tokio::task::yield_now().await;
        drop(lease);
        let outcome = waiting.await.expect("join").expect("outcome");
        assert!(outcome.drained);
        leases.complete_unload(&module_id).expect("complete unload");
        assert_eq!(
            leases.status(&module_id).expect("status").state,
            ModuleRuntimeLifecycleState::Unloaded
        );
    }

    #[tokio::test]
    async fn drain_timeout_keeps_module_quarantined_until_the_lease_finishes() {
        let snapshot = module_snapshot();
        let module_id = snapshot
            .registry()
            .registrations()
            .next()
            .expect("module")
            .module_id()
            .to_string();
        let leases = ModuleInvocationLeaseRegistry::default();
        leases
            .activate_registry(snapshot.as_ref())
            .expect("activate");
        let lease = leases.acquire(&module_id).expect("lease");
        leases.begin_drain(&module_id).expect("drain");

        let outcome = leases
            .wait_for_drain(&module_id, Duration::from_millis(1))
            .await
            .expect("outcome");
        assert!(!outcome.drained);
        assert_eq!(outcome.remaining_invocations, 1);
        assert!(matches!(
            leases.complete_unload(&module_id),
            Err(ModuleInvocationLeaseError::StillActive { .. })
        ));
        drop(lease);
        assert!(
            leases
                .wait_for_drain(&module_id, Duration::from_secs(1))
                .await
                .expect("drained")
                .drained
        );
    }

    #[test]
    fn unified_runtime_generation_rejects_mismatched_and_regressing_snapshots() {
        let module_snapshot = module_snapshot();
        let extensions = Arc::new(
            crate::compile_default_extension_registry(module_snapshot.as_ref())
                .expect("extension registry"),
        );
        let store =
            ModuleRuntimeGenerationStore::new(module_snapshot.clone(), extensions.clone(), 1)
                .expect("runtime store");
        let next = store
            .publish(module_snapshot.clone(), extensions.clone(), 2)
            .expect("service generation advance");
        assert_eq!(next.generation, 2);
        assert!(matches!(
            store.publish(module_snapshot.clone(), extensions, 1),
            Err(ModuleRuntimeGenerationError::Regression { .. })
        ));

        let mut mismatched = crate::compile_default_extension_registry(module_snapshot.as_ref())
            .expect("extension registry");
        mismatched.generation.module_registry_generation += 1;
        assert!(matches!(
            store.publish(module_snapshot, Arc::new(mismatched), 3),
            Err(ModuleRuntimeGenerationError::MismatchedGeneration { .. })
        ));
    }

    #[test]
    fn hot_unload_request_requires_a_bounded_deadline_and_reason() {
        let valid = ModuleHotUnloadRequest {
            module_id: "01fixturemodule00000000000000".to_string(),
            drain_timeout_ms: 30_000,
            reason: "operator request".to_string(),
        };
        valid.validate().expect("valid request");
        assert!(
            ModuleHotUnloadRequest {
                drain_timeout_ms: 0,
                ..valid.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            ModuleHotUnloadRequest {
                reason: String::new(),
                ..valid
            }
            .validate()
            .is_err()
        );
    }
}
