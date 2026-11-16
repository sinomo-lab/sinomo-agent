//! Exact publication DATA and managed component lifecycle controls. Native
//! fixtures use the actual scoped owner; faults withhold work, never grant readiness.
use super::*;
use sinomo_dfs::{ContentStoreStagingWriter, LocalContentStore};

#[test]
fn managed_owner_preserves_current_initialization_at_every_durable_phase() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    for phase in 1..=5 {
        let name = format!("managed-current-initialization-{phase}");
        let node = DfsTestRuntime::ordered_node_ids(&name, 1).remove(0);
        let mut native = DfsTestRuntime::new(&name, node.clone()).unwrap();
        let live = native.data_dir(&node).dfs_dir().join("content");
        let original = LocalContentStore::interrupt_current_initialization_for_test(&live, phase).unwrap();
        assert!(LocalContentStore::has_current_initialization(&live).unwrap());
        native.configure_component_factories(vec![
            Arc::new(sinomo_dfs::component_factory_with_storage_preparation(Arc::new(storage_preparation_provider)).unwrap()),
            Arc::new(staged_import_component_factory().unwrap()),
        ]).unwrap();
        native.start_nodes().unwrap();
        let store = LocalContentStore::new(&live).unwrap();
        assert_eq!(store.store_incarnation().unwrap(), original);
        assert!(!LocalContentStore::has_current_initialization(&live).unwrap());
        assert!(!live.join(".content-conversion-v2").exists(), "current initialization was enrolled as a legacy conversion");
        let object = sinomo_core::Id::generate().to_string();
        let version = sinomo_core::Id::generate().to_string();
        store.put_object_version(&object, &version, b"same native incarnation").unwrap();
        assert_eq!(store.read_object_version(&object, &version).unwrap().1, b"same native incarnation");
        drop(store);
        native.close().unwrap();
    }
}

#[test]
fn partial_content_publication_preserves_source_and_replays_exact_target() {
    let data = sinomo_testdir::TestDir::new("single-owner-content-publication");
    let live = data.join("dfs/content");
    crate::content::write_original_v3_fixture_data(&live, b"original immutable bytes", false);
    let marker = std::fs::read(live.join("content-reservations/00/batch-format-v1.sobj")).unwrap();
    let catalog = std::fs::read(live.join("inline-bindings/current.sobj")).unwrap();
    let mut bound_before_effect = false;
    let first = content_exposure::prepare(data.path(), |_| { bound_before_effect = true; Ok(()) }).unwrap();
    assert!(bound_before_effect);
    assert_eq!(content_exposure::frontend_target(data.path()).unwrap(), Some(true));
    assert_eq!(LocalContentStore::open_for_inspection(&live).inventory_import_pending().unwrap().as_deref(), Some(first.inventory_id.as_str()));
    let retained = data.join(&first.retained);
    assert!(content_exposure::completed_original_job(data.path(), &first.retained).unwrap().is_none(),
        "a published partial target is not completed original evidence");
    assert_eq!(std::fs::read(retained.join("content-reservations/00/batch-format-v1.sobj")).unwrap(), marker);
    assert_eq!(std::fs::read(retained.join("inline-bindings/current.sobj")).unwrap(), catalog);
    let replay = content_exposure::prepare(data.path(), |_| Ok(())).unwrap();
    assert_eq!(replay.inventory_id, first.inventory_id);
    assert_eq!(replay.target_incarnation, first.target_incarnation);
    assert_eq!(replay.retained, first.retained);
}

#[test]
fn valid_fresh_content_cannot_replace_a_partial_publication_target() {
    let data = sinomo_testdir::TestDir::new("single-owner-content-replacement");
    let live = data.join("dfs/content");
    crate::content::write_original_v3_fixture_data(&live, b"kept source", false);
    let publication = content_exposure::prepare(data.path(), |_| Ok(())).unwrap();
    let retained = data.join(&publication.retained);
    let original = std::fs::read(retained.join("inline-bindings/current.sobj")).unwrap();
    std::fs::rename(&live, data.join("kept-original-target")).unwrap();
    drop(ContentStoreStagingWriter::open_candidate(&live).unwrap());
    assert!(crate::content::current(&live).unwrap());
    assert!(LocalContentStore::open_for_inspection(&live).inventory_import_pending().unwrap().is_none());
    assert!(content_exposure::frontend_target(data.path()).is_err());
    assert!(content_exposure::prepare(data.path(), |_| Ok(())).is_err());
    assert_eq!(std::fs::read(retained.join("inline-bindings/current.sobj")).unwrap(), original);
}

#[test]
fn managed_owner_migrates_never_requested_source3_in_background() {
    managed_background_content("managed-owner-cold-content-background", b"cold content is work without a read request");
}

#[test]
fn managed_owner_byte_progress_finishes_a_blob_larger_than_a_turn() {
    managed_background_content("managed-owner-large-content-background", &vec![0x42; 16 * 1024 * 1024]);
}

fn managed_background_content(name: &str, expected: &[u8]) {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let live = native.data_dir(&node).dfs_dir().join("content");
    let manifest = crate::content::write_original_v3_fixture_data(&live, expected, false);
    native.configure_component_factories(vec![
        Arc::new(sinomo_dfs::component_factory_with_storage_preparation(Arc::new(storage_preparation_provider)).unwrap()),
        Arc::new(staged_import_component_factory().unwrap()),
    ]).unwrap();
    native.start_nodes().unwrap();
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let mut changed = owner.binding().readiness;
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    block_native(&native.runtime_handle(), async {
        loop {
            // Inspection does not demand the version or schedule conversion.
            if crate::content::current(&live)?
                && LocalContentStore::open_for_inspection(&live).inventory_import_pending().map_err(error)?.is_none() {
                return Ok::<_, MetadataBackendError>(());
            }
            tokio::time::timeout_at(deadline, changed.changed()).await
                .map_err(|_| error("background cold content did not complete its actual owner work"))?
                .map_err(error)?;
        }
    }).unwrap_or_else(|failure| panic!("{failure}; actual owner state: {:?}", owner.work.scheduler.lock().unwrap()));
    let (actual_manifest, actual_bytes) = LocalContentStore::open_for_inspection(&live)
        .read_object_version(&manifest.object_id, &manifest.version).unwrap();
    assert_eq!(actual_manifest, manifest);
    assert_eq!(actual_bytes, expected);
    let publication = content_exposure::retained_publication(&owner.authority.data_root).unwrap().unwrap();
    assert!(owner.authority.data_root.join(&publication.retained).join("inline-bindings/current.sobj").is_file());
    let original_cursor = crate::content::OriginalV3Converter::checkpoint_path(
        &owner.authority.data_root.join(&publication.retained), &live, &publication.inventory_id).unwrap();
    let before = std::fs::read(&original_cursor).unwrap();
    let archive_job = content_exposure::completed_original_job(&owner.authority.data_root, &publication.retained).unwrap().unwrap();
    assert_eq!(archive_job.identity, publication.inventory_id);
    assert_eq!(std::fs::read(&original_cursor).unwrap(), before, "archive classification must not advance conversion");
    assert!(content_exposure::completed_original_job(&owner.authority.data_root, Path::new("dfs/.content-retained/unknown")).unwrap().is_none());
    drop(changed);
    drop(owner);
    drop(context);
    native.close().unwrap();
}

#[test]
fn missing_live_after_owned_source_rename_resumes_original_pending_target() {
    let data = sinomo_testdir::TestDir::new("single-owner-content-rename-restart");
    let live = data.join("dfs/content");
    crate::content::write_original_v3_fixture_data(&live, b"original across rename interruption", false);
    let hook = content_exposure::install_hook(data.path(), Arc::new(|point| {
        if point == "source-retained" { return Err(error("injected interruption after durable original source retention")); }
        Ok(())
    }));
    assert!(content_exposure::prepare(data.path(), |_| Ok(())).is_err());
    assert!(!live.exists());
    assert_eq!(content_exposure::frontend_target(data.path()).unwrap(), Some(false));
    let original = content_exposure::retained_publication(data.path()).unwrap().unwrap();
    drop(hook);
    let recovered = content_exposure::prepare(data.path(), |_| Ok(())).unwrap();
    assert_eq!(recovered.inventory_id, original.inventory_id);
    assert_eq!(recovered.target_incarnation, original.target_incarnation);
    assert_eq!(recovered.retained, original.retained);
    assert_eq!(LocalContentStore::open_for_inspection(&live).inventory_import_pending().unwrap().as_deref(), Some(original.inventory_id.as_str()));
}

#[test]
fn managed_waiter_deadline_does_not_cancel_owned_source_relocation() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let name = "managed-owner-caller-cancel";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let data_root = native.data_dir(&node).root().canonicalize().unwrap();
    let live = data_root.join("dfs/content");
    let manifest = crate::content::write_original_v3_fixture_data(&live, b"owned work outlives its caller", false);
    let (entered, observed) = std::sync::mpsc::sync_channel(1);
    let (release, resume) = std::sync::mpsc::sync_channel(1);
    let resume = Mutex::new(resume);
    let hook = content_exposure::install_hook(&data_root, Arc::new(move |point| {
        if point == "source-retained" {
            entered.send(()).map_err(error)?;
            resume.lock().map_err(error)?.recv().map_err(error)?;
        }
        Ok(())
    }));
    native.configure_component_factories(vec![
        Arc::new(sinomo_dfs::component_factory_with_storage_preparation(Arc::new(storage_preparation_provider)).unwrap()),
        Arc::new(staged_import_component_factory().unwrap()),
    ]).unwrap();
    let first_wait = native.start_nodes();
    observed.recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT).unwrap();
    assert!(first_wait.is_err(), "held source relocation must not satisfy ordinary startup");
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    assert!(owner.work.active.load(std::sync::atomic::Ordering::Acquire) > 0);
    assert!(!*owner.lifecycle.borrow(), "caller deadline cancelled the actual owner");
    assert!(!live.exists());
    assert!(matches!((owner.binding().provider)(&live, &DfsStoragePreparationScope::Content, 256),
        Ok(sinomo_dfs::DfsStoragePreparation::Pending { .. })));
    assert!(!live.exists(), "pending managed demand initialized an empty replacement root");
    let original = content_exposure::retained_publication(&data_root).unwrap().unwrap();
    release.send(()).unwrap();
    drop(hook);
    native.wait_startup_completion().unwrap();
    let mut changed = owner.binding().readiness;
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    block_native(&native.runtime_handle(), async {
        loop {
            if LocalContentStore::open_for_inspection(&live).inventory_import_completed(&original.inventory_id).map_err(error)? { return Ok::<_, MetadataBackendError>(()); }
            tokio::time::timeout_at(deadline, changed.changed()).await.map_err(|_| error("owned source continuation deadline"))?.map_err(error)?;
        }
    }).unwrap();
    let completed = content_exposure::retained_publication(&data_root).unwrap().unwrap();
    assert_eq!(completed.inventory_id, original.inventory_id);
    assert_eq!(completed.target_incarnation, original.target_incarnation);
    assert_eq!(LocalContentStore::open_for_inspection(&live).read_object_version(&manifest.object_id, &manifest.version).unwrap().1, b"owned work outlives its caller");
    drop(changed); drop(owner); drop(context);
    native.close().unwrap();
}

#[test]
fn managed_version_demand_completes_while_global_source_inventory_is_held() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    use std::sync::atomic::{AtomicBool, Ordering};
    let name = "managed-owner-exact-content-demand";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let data_root = native.data_dir(&node).root().canonicalize().unwrap();
    let live = data_root.join("dfs/content");
    let expected = b"demanded unit does not await unrelated inventory";
    let manifest = crate::content::write_original_v3_fixture_data(&live, expected, false);
    let held = Arc::new(AtomicBool::new(true));
    let hold = Arc::clone(&held);
    let root_for_hook = live.clone();
    let hook = content_exposure::install_hook(&data_root, Arc::new(move |point| {
        if point == "before-inventory-step" && hold.load(Ordering::Acquire) {
            return Err(MetadataBackendError::StoragePreparationPending { root: root_for_hook.clone(), processed_records: 0 });
        }
        Ok(())
    }));
    native.configure_component_factories(vec![
        Arc::new(sinomo_dfs::component_factory_with_storage_preparation(Arc::new(storage_preparation_provider)).unwrap()),
        Arc::new(staged_import_component_factory().unwrap()),
    ]).unwrap();
    native.start_nodes().unwrap();
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let mut changed = owner.binding().readiness;
    let store = LocalContentStore::new(&live).unwrap();
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let (actual, bytes) = block_native(&native.runtime_handle(), async {
        loop {
            match store.read_object_version(&manifest.object_id, &manifest.version) {
                Ok(result) => return Ok::<_, MetadataBackendError>(result),
                Err(sinomo_dfs::ContentStoreError::StoragePreparationPending { .. }) => {},
                Err(failure) => return Err(error(failure)),
            }
            tokio::time::timeout_at(deadline, changed.changed()).await.map_err(|_| error("exact demanded content unit deadline"))?.map_err(error)?;
        }
    }).unwrap_or_else(|failure| panic!("{failure}; actual demanded owner state: {:?}", owner.work.scheduler.lock().unwrap()));
    assert_eq!(actual, manifest);
    assert_eq!(bytes, expected);
    assert!(held.load(Ordering::Acquire));
    assert!(store.inventory_import_pending().unwrap().is_some(), "per-unit readiness incorrectly cleared global coverage");
    held.store(false, Ordering::Release);
    drop(hook);
    owner.work.notify();
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    block_native(&native.runtime_handle(), async {
        while store.inventory_import_pending().map_err(error)?.is_some() {
            tokio::time::timeout_at(deadline, changed.changed()).await.map_err(|_| error("released actual inventory continuation deadline"))?.map_err(error)?;
        }
        Ok::<_, MetadataBackendError>(())
    }).unwrap();
    drop(store); drop(changed); drop(owner); drop(context);
    native.close().unwrap();
}

#[test]
fn managed_owner_restart_recovers_missing_live_original_publication() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let name = "managed-owner-rename-restart";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let data_root = native.data_dir(&node).root().canonicalize().unwrap();
    let live = data_root.join("dfs/content");
    let expected = b"same original source after owner restart";
    let manifest = crate::content::write_original_v3_fixture_data(&live, expected, false);
    let hook = content_exposure::install_hook(&data_root, Arc::new(|point| {
        if point == "source-retained" { return Err(error("injected owner interruption after source rename")); }
        Ok(())
    }));
    native.configure_component_factories(vec![
        Arc::new(sinomo_dfs::component_factory_with_storage_preparation(Arc::new(storage_preparation_provider)).unwrap()),
        Arc::new(staged_import_component_factory().unwrap()),
    ]).unwrap();
    assert!(native.start_nodes().is_err());
    assert!(!live.exists());
    let original = content_exposure::retained_publication(&data_root).unwrap().unwrap();
    let source_catalog = std::fs::read(data_root.join(&original.retained).join("inline-bindings/current.sobj")).unwrap();
    native.stop_node(&node).unwrap();
    drop(hook);
    native.reopen_node(node.clone()).unwrap();
    native.wait_startup_completion().unwrap();
    let context = native.component_context(&node).unwrap();
    let owner = placement_work::native_tests::canonical_migration_runtime(&context);
    let mut changed = owner.binding().readiness;
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    block_native(&native.runtime_handle(), async {
        loop {
            if LocalContentStore::open_for_inspection(&live).inventory_import_completed(&original.inventory_id).map_err(error)? {
                return Ok::<_, MetadataBackendError>(());
            }
            tokio::time::timeout_at(deadline, changed.changed()).await.map_err(|_| error("restarted original content job did not complete"))?.map_err(error)?;
        }
    }).unwrap_or_else(|failure| panic!("{failure}; actual restarted owner state: {:?}", owner.work.scheduler.lock().unwrap()));
    let recovered = content_exposure::retained_publication(&data_root).unwrap().unwrap();
    assert_eq!(recovered.inventory_id, original.inventory_id);
    assert_eq!(recovered.target_incarnation, original.target_incarnation);
    assert_eq!(recovered.retained, original.retained);
    assert_eq!(std::fs::read(data_root.join(&original.retained).join("inline-bindings/current.sobj")).unwrap(), source_catalog);
    let (actual, bytes) = LocalContentStore::open_for_inspection(&live).read_object_version(&manifest.object_id, &manifest.version).unwrap();
    assert_eq!(actual, manifest);
    assert_eq!(bytes, expected);
    drop(changed); drop(owner); drop(context);
    native.close().unwrap();
}

#[test]
fn managed_factory_failure_does_not_initialize_missing_content_root() {
    use sinomo_dfs::test_support::DfsTestRuntime;
    let name = "managed-owner-builder-failure";
    let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
    let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
    let data_root = native.data_dir(&node).root().canonicalize().unwrap();
    let live = data_root.join("dfs/content");
    crate::content::write_original_v3_fixture_data(&live, b"preserved while provider unavailable", false);
    let hook = content_exposure::install_hook(&data_root, Arc::new(|point| {
        if point == "source-retained" { return Err(error("injected interruption before target publication")); }
        Ok(())
    }));
    assert!(content_exposure::prepare(&data_root, |_| Ok(())).is_err());
    drop(hook);
    assert!(!live.exists());
    let original = content_exposure::retained_publication(&data_root).unwrap().unwrap();
    let source_catalog = std::fs::read(data_root.join(&original.retained).join("inline-bindings/current.sobj")).unwrap();
    native.configure_component_factories(vec![
        Arc::new(sinomo_dfs::component_factory_with_storage_preparation(Arc::new(|_, _, _| {
            Err(sinomo_module::ComponentError::Mount { component_id: "dfs".into(), detail: "injected unavailable migration owner".into() })
        })).unwrap()),
        Arc::new(staged_import_component_factory().unwrap()),
    ]).unwrap();
    assert!(native.start_nodes().is_err());
    assert!(!live.exists(), "managed factory failure created an empty content root across retained publication");
    assert_eq!(std::fs::read(data_root.join(&original.retained).join("inline-bindings/current.sobj")).unwrap(), source_catalog);
    assert_eq!(content_exposure::retained_publication(&data_root).unwrap().unwrap().inventory_id, original.inventory_id);
    native.close().unwrap();
}

#[test]
fn virgin_content_requires_observed_physical_absence() {
    let data = sinomo_testdir::TestDir::new("managed-content-absence-evidence");
    let live = data.join("dfs/content");
    std::fs::create_dir_all(data.join("dfs")).unwrap();
    assert!(content_exposure::live_root_absent(&live).unwrap());
    assert!(!content_exposure::has_retained_sources(data.path()).unwrap());
    std::fs::write(data.join("dfs/.content-retained"), b"unreadable as source directory").unwrap();
    assert!(content_exposure::has_retained_sources(data.path()).is_err());
    std::fs::write(&live, b"existing DATA is not absent").unwrap();
    assert!(content_exposure::live_root_absent(&live).is_err());
}
