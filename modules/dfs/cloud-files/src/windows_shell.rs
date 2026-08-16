//! Reactive File Explorer status projection for the Windows provider.
//!
//! Explorer reads properties stored by Windows. No Sinomo COM server is loaded
//! into Explorer, and no shell query can enter DFS, the network, or the queue.
//! The provider pushes a bounded update only when an already-durable local
//! writeback state changes.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use windows::Storage::Provider::{
    StorageProviderItemProperties, StorageProviderItemProperty,
    StorageProviderItemPropertyDefinition, StorageProviderSyncRootManager,
};
use windows::Storage::{IStorageItem, StorageFile, StorageFolder};
use windows::Win32::Foundation::RPC_E_CHANGED_MODE;
use windows::Win32::System::Com::{COINIT_MULTITHREADED, CoInitializeEx, CoUninitialize};
use windows::core::{HSTRING, Interface};
use windows_collections::IIterable;

use crate::sync_executor::CloudFilesDfsMutationCommand;
use crate::windows_sync::{CloudFilesExplorerItemStatus, CloudFilesWritableSyncState};

const STATUS_ICON_RESOURCE: &str = "shell32.dll,-14";

const STATUS_DEFINITIONS: &[(CloudFilesExplorerItemStatus, i32, &str)] = &[
    (
        CloudFilesExplorerItemStatus::WaitingToUpload,
        1,
        "Waiting to upload",
    ),
    (CloudFilesExplorerItemStatus::Uploading, 2, "Uploading"),
    (CloudFilesExplorerItemStatus::Retrying, 3, "Retrying"),
    (CloudFilesExplorerItemStatus::Conflict, 4, "Conflict"),
    (CloudFilesExplorerItemStatus::Failed, 5, "Sync failed"),
];

fn shell_error(operation: &str, error: impl std::fmt::Display) -> String {
    format!("{operation}: {error}")
}

struct ComApartment(bool);

impl ComApartment {
    fn enter_mta() -> Result<Self, windows::core::HRESULT> {
        let status = unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) };
        if status.is_ok() {
            Ok(Self(true))
        } else {
            Err(status)
        }
    }
}

impl Drop for ComApartment {
    fn drop(&mut self) {
        if self.0 {
            unsafe { CoUninitialize() };
        }
    }
}

fn run_in_mta<T, F>(operation: &'static str, task: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    match ComApartment::enter_mta() {
        Ok(_apartment) => task(),
        Err(status) if status == RPC_E_CHANGED_MODE => thread::Builder::new()
            .name("sinomo-cloud-files-shell".to_string())
            .spawn(move || {
                let _apartment =
                    ComApartment::enter_mta().map_err(|status| shell_error(operation, status))?;
                task()
            })
            .map_err(|error| shell_error("spawn Cloud Files shell operation", error))?
            .join()
            .map_err(|_| format!("{operation}: shell worker panicked"))?,
        Err(status) => Err(shell_error(operation, status)),
    }
}

fn definition_tuple(
    definition: &StorageProviderItemPropertyDefinition,
) -> Result<(i32, String), String> {
    Ok((
        definition
            .Id()
            .map_err(|error| shell_error("read custom-state ID", error))?,
        definition
            .DisplayNameResource()
            .map_err(|error| shell_error("read custom-state label", error))?
            .to_string(),
    ))
}

fn configure_sync_root_status_schema_inner(sync_root: &Path) -> Result<(), String> {
    if !StorageProviderSyncRootManager::IsSupported()
        .map_err(|error| shell_error("query storage-provider support", error))?
    {
        return Err("Windows storage-provider custom properties are unavailable".to_string());
    }
    let path = HSTRING::from(sync_root.as_os_str().to_string_lossy().as_ref());
    let folder = StorageFolder::GetFolderFromPathAsync(&path)
        .map_err(|error| shell_error("open Cloud Files sync root for Explorer", error))?
        .join()
        .map_err(|error| shell_error("resolve Cloud Files sync root for Explorer", error))?;
    let info = StorageProviderSyncRootManager::GetSyncRootInformationForFolder(&folder)
        .map_err(|error| shell_error("read Cloud Files shell registration", error))?;
    let definitions = info
        .StorageProviderItemPropertyDefinitions()
        .map_err(|error| shell_error("read Cloud Files custom-state definitions", error))?;
    let mut observed = Vec::new();
    for index in 0..definitions
        .Size()
        .map_err(|error| shell_error("count Cloud Files custom-state definitions", error))?
    {
        let definition = definitions
            .GetAt(index)
            .map_err(|error| shell_error("read Cloud Files custom-state definition", error))?;
        observed.push(definition_tuple(&definition)?);
    }
    let expected = STATUS_DEFINITIONS
        .iter()
        .map(|(_, id, label)| (*id, (*label).to_string()))
        .collect::<Vec<_>>();
    if observed == expected {
        return Ok(());
    }
    definitions
        .Clear()
        .map_err(|error| shell_error("replace Cloud Files custom-state definitions", error))?;
    for (_, id, label) in STATUS_DEFINITIONS {
        let definition = StorageProviderItemPropertyDefinition::new()
            .map_err(|error| shell_error("create Cloud Files custom-state definition", error))?;
        definition
            .SetId(*id)
            .map_err(|error| shell_error("set Cloud Files custom-state ID", error))?;
        definition
            .SetDisplayNameResource(&HSTRING::from(*label))
            .map_err(|error| shell_error("set Cloud Files custom-state label", error))?;
        definitions
            .Append(&definition)
            .map_err(|error| shell_error("append Cloud Files custom-state definition", error))?;
    }
    StorageProviderSyncRootManager::Register(&info)
        .map_err(|error| shell_error("register Cloud Files Explorer status schema", error))
}

/// Add the provider-specific status vocabulary to an already-owned sync root.
/// The core mount remains usable if the shell extension surface is unavailable.
pub(crate) fn configure_sync_root_status_schema(sync_root: &Path) -> Result<(), String> {
    let sync_root = sync_root.to_path_buf();
    run_in_mta("configure Cloud Files Explorer status", move || {
        configure_sync_root_status_schema_inner(&sync_root)
    })
}

fn status_definition(status: CloudFilesExplorerItemStatus) -> (i32, &'static str) {
    STATUS_DEFINITIONS
        .iter()
        .find_map(|(candidate, id, label)| (*candidate == status).then_some((*id, *label)))
        .expect("every Explorer status has a registered definition")
}

fn storage_item(path: &Path) -> Result<Option<IStorageItem>, String> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(shell_error("inspect Explorer status path", error)),
    };
    let path = HSTRING::from(path.as_os_str().to_string_lossy().as_ref());
    if metadata.is_dir() {
        StorageFolder::GetFolderFromPathAsync(&path)
            .map_err(|error| shell_error("open Explorer status directory", error))?
            .join()
            .map_err(|error| shell_error("resolve Explorer status directory", error))?
            .cast::<IStorageItem>()
            .map(Some)
            .map_err(|error| shell_error("cast Explorer status directory", error))
    } else {
        StorageFile::GetFileFromPathAsync(&path)
            .map_err(|error| shell_error("open Explorer status file", error))?
            .join()
            .map_err(|error| shell_error("resolve Explorer status file", error))?
            .cast::<IStorageItem>()
            .map(Some)
            .map_err(|error| shell_error("cast Explorer status file", error))
    }
}

fn set_item_status(
    path: &Path,
    status: Option<CloudFilesExplorerItemStatus>,
) -> Result<(), String> {
    let Some(item) = storage_item(path)? else {
        return Ok(());
    };
    let mut properties = Vec::new();
    if let Some(status) = status {
        let (id, label) = status_definition(status);
        let property = StorageProviderItemProperty::new()
            .map_err(|error| shell_error("create Explorer item status", error))?;
        property
            .SetId(id)
            .map_err(|error| shell_error("set Explorer item status ID", error))?;
        property
            .SetValue(&HSTRING::from(label))
            .map_err(|error| shell_error("set Explorer item status text", error))?;
        property
            .SetIconResource(&HSTRING::from(STATUS_ICON_RESOURCE))
            .map_err(|error| shell_error("set Explorer item status icon", error))?;
        properties.push(property);
    }
    let properties: IIterable<StorageProviderItemProperty> =
        properties.into_iter().map(Some).collect::<Vec<_>>().into();
    StorageProviderItemProperties::SetAsync(&item, &properties)
        .map_err(|error| shell_error("start Explorer item status update", error))?
        .join()
        .map_err(|error| shell_error("complete Explorer item status update", error))
}

fn apply_item_status_changes(
    changes: Vec<(PathBuf, Option<CloudFilesExplorerItemStatus>)>,
) -> Result<(), String> {
    if changes.is_empty() {
        return Ok(());
    }
    run_in_mta("publish Cloud Files Explorer status", move || {
        let mut first_error = None;
        for (path, status) in changes {
            if let Err(error) = set_item_status(&path, status)
                && first_error.is_none()
            {
                first_error = Some(format!("{}: {error}", path.display()));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    })
}

fn sifs_to_local(sync_root: &Path, root_path: &str, path: &str) -> Result<PathBuf, String> {
    fn canonical(path: &str) -> Result<Vec<&str>, String> {
        if !path.starts_with('/') || path.contains('\\') {
            return Err(format!("SIFS path `{path}` is not canonical"));
        }
        let mut result = Vec::new();
        for component in Path::new(path).components() {
            match component {
                Component::RootDir => {}
                Component::Normal(name) => result.push(
                    name.to_str()
                        .ok_or_else(|| format!("SIFS path `{path}` is not UTF-8"))?,
                ),
                _ => return Err(format!("SIFS path `{path}` is not canonical")),
            }
        }
        Ok(result)
    }
    let root = canonical(root_path)?;
    let path = canonical(path)?;
    if !path.starts_with(&root) {
        return Err(format!(
            "SIFS path `{path:?}` is outside mounted root `{root_path}`"
        ));
    }
    let mut local = sync_root.to_path_buf();
    for component in &path[root.len()..] {
        local.push(component);
    }
    Ok(local)
}

fn mutation_status_path(command: &CloudFilesDfsMutationCommand) -> String {
    use crate::local_change_queue::CloudFilesLocalOperationKind;

    match command.kind {
        CloudFilesLocalOperationKind::DeleteFile
        | CloudFilesLocalOperationKind::DeleteDirectory => command
            .source_path
            .trim_end_matches('/')
            .rsplit_once('/')
            .map_or_else(
                || "/".to_string(),
                |(parent, _)| {
                    if parent.is_empty() {
                        "/".to_string()
                    } else {
                        parent.to_string()
                    }
                },
            ),
        CloudFilesLocalOperationKind::Rename => command
            .destination_path
            .clone()
            .unwrap_or_else(|| command.source_path.clone()),
        _ => command.source_path.clone(),
    }
}

/// Shared, mount-scoped cache. Equality suppresses every unchanged update, so
/// an idle provider performs no shell calls and no disk I/O for decoration.
pub(crate) struct CloudFilesExplorerStatusProjector {
    sync_root: PathBuf,
    root_path: String,
    enabled: bool,
    desired: BTreeMap<String, CloudFilesExplorerItemStatus>,
    published: BTreeMap<String, CloudFilesExplorerItemStatus>,
    logged_error: bool,
}

impl CloudFilesExplorerStatusProjector {
    pub(crate) fn new(sync_root: PathBuf, root_path: String) -> Self {
        Self {
            sync_root,
            root_path,
            enabled: false,
            desired: BTreeMap::new(),
            published: BTreeMap::new(),
            logged_error: false,
        }
    }

    pub(crate) fn observe_state(&mut self, state: &CloudFilesWritableSyncState) {
        self.desired = state.explorer_item_statuses();
        self.reconcile();
    }

    pub(crate) fn observe_uploading(&mut self, command: &CloudFilesDfsMutationCommand) {
        let path = mutation_status_path(command);
        self.desired
            .entry(path)
            .and_modify(|status| *status = (*status).max(CloudFilesExplorerItemStatus::Uploading))
            .or_insert(CloudFilesExplorerItemStatus::Uploading);
        self.reconcile();
    }

    pub(crate) fn enable(&mut self) {
        self.enabled = true;
        self.reconcile();
    }

    fn reconcile(&mut self) {
        if !self.enabled || self.desired == self.published {
            return;
        }
        let mut changes = Vec::new();
        for (path, status) in &self.desired {
            if self.published.get(path) != Some(status) {
                match sifs_to_local(&self.sync_root, &self.root_path, path) {
                    Ok(local) => changes.push((local, Some(*status))),
                    Err(error) if !self.logged_error => {
                        crate::host_log(&format!(
                            "Cloud Files Explorer status path was rejected: {error}"
                        ));
                        self.logged_error = true;
                    }
                    Err(_) => {}
                }
            }
        }
        for path in self.published.keys() {
            if !self.desired.contains_key(path) {
                match sifs_to_local(&self.sync_root, &self.root_path, path) {
                    Ok(local) => changes.push((local, None)),
                    Err(error) if !self.logged_error => {
                        crate::host_log(&format!(
                            "Cloud Files Explorer status clear path was rejected: {error}"
                        ));
                        self.logged_error = true;
                    }
                    Err(_) => {}
                }
            }
        }
        // Record the attempted state even on shell failure. Decoration must
        // never turn an idle mount into a retry loop or battery drain source.
        self.published = self.desired.clone();
        if let Err(error) = apply_item_status_changes(changes)
            && !self.logged_error
        {
            crate::host_log(&format!(
                "Cloud Files Explorer status update is unavailable: {error}"
            ));
            self.logged_error = true;
        }
    }
}

pub(crate) type SharedExplorerStatusProjector = Arc<Mutex<CloudFilesExplorerStatusProjector>>;

pub(crate) fn observe_explorer_state(
    projector: &SharedExplorerStatusProjector,
    state: &CloudFilesWritableSyncState,
) {
    match projector.lock() {
        Ok(mut projector) => projector.observe_state(state),
        Err(_) => crate::host_log("Cloud Files Explorer status projector is poisoned"),
    }
}

pub(crate) fn enable_explorer_status(projector: &SharedExplorerStatusProjector) {
    match projector.lock() {
        Ok(mut projector) => projector.enable(),
        Err(_) => crate::host_log("Cloud Files Explorer status projector is poisoned"),
    }
}

pub(crate) fn observe_explorer_upload(
    projector: &SharedExplorerStatusProjector,
    command: &CloudFilesDfsMutationCommand,
) {
    match projector.lock() {
        Ok(mut projector) => projector.observe_uploading(command),
        Err(_) => crate::host_log("Cloud Files Explorer status projector is poisoned"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_status_schema_is_bounded_and_stable() {
        assert_eq!(STATUS_DEFINITIONS.len(), 5);
        assert_eq!(
            STATUS_DEFINITIONS
                .iter()
                .map(|(_, id, _)| *id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(
            status_definition(CloudFilesExplorerItemStatus::Conflict),
            (4, "Conflict")
        );
    }

    #[test]
    fn mounted_sifs_paths_project_only_beneath_the_sync_root() {
        let sync_root = Path::new(r"D:\sinomo");
        assert_eq!(
            sifs_to_local(sync_root, "/services", "/services/users/human/image.jpg")
                .expect("project mounted SIFS path"),
            sync_root.join("users").join("human").join("image.jpg")
        );
        assert!(sifs_to_local(sync_root, "/services", "/users/human/image.jpg").is_err());
        assert!(sifs_to_local(sync_root, "/services", "/services/../secrets").is_err());
    }
}
