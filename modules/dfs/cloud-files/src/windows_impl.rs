use std::collections::{BTreeMap, VecDeque};
use std::ffi::{OsStr, c_void};
use std::fs;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use sinomo_dfs_cloud_files_api::{
    CloudFilesModuleError, CloudFilesMountRequest, CloudFilesMountResponse,
};
use sinomo_runtime::core::ClusterEndpointRecord;
use sinomo_runtime::daemon;
use sinomo_runtime::dfs::{
    self, DfsControlPlaneKvClient, DfsMessageCommand, DfsMessageResponse, DfsMessageTransport,
    DfsObjectKind, DfsPosixError, DfsRoutedMessage, DfsTransportIdentity, LocalContentStore,
    NetworkedDfsService,
};
use sinomo_runtime::kv::{
    KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse, read_cluster_kv_entry,
    read_cluster_kv_prefix_entries,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{
    self, InboundFrame, MeshMessage, MeshMessageBody, MeshMessageId, NetworkError, NetworkFrame,
    NetworkFrameId, ProtocolMessage,
};
use sinomo_runtime::node::NodeId;
use windows::Win32::Foundation::{
    NTSTATUS, STATUS_FILE_IS_A_DIRECTORY, STATUS_NOT_A_DIRECTORY, STATUS_OBJECT_NAME_NOT_FOUND,
    STATUS_SUCCESS, STATUS_UNSUCCESSFUL,
};
use windows::Win32::Storage::CloudFilters::{
    CF_CALLBACK_INFO, CF_CALLBACK_PARAMETERS, CF_CALLBACK_REGISTRATION,
    CF_CALLBACK_TYPE_FETCH_DATA, CF_CALLBACK_TYPE_FETCH_PLACEHOLDERS, CF_CALLBACK_TYPE_NONE,
    CF_CONNECT_FLAG_NONE, CF_CREATE_FLAG_NONE, CF_FS_METADATA, CF_HARDLINK_POLICY_NONE,
    CF_HYDRATION_POLICY, CF_HYDRATION_POLICY_MODIFIER_AUTO_DEHYDRATION_ALLOWED,
    CF_HYDRATION_POLICY_PROGRESSIVE, CF_INSYNC_POLICY_TRACK_ALL, CF_OPERATION_INFO,
    CF_OPERATION_PARAMETERS, CF_OPERATION_PARAMETERS_0, CF_OPERATION_PARAMETERS_0_0,
    CF_OPERATION_PARAMETERS_0_4, CF_OPERATION_TRANSFER_DATA_FLAG_NONE,
    CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE, CF_OPERATION_TYPE_TRANSFER_DATA,
    CF_OPERATION_TYPE_TRANSFER_PLACEHOLDERS, CF_PLACEHOLDER_CREATE_FLAG_MARK_IN_SYNC,
    CF_PLACEHOLDER_CREATE_FLAG_SUPERSEDE, CF_PLACEHOLDER_CREATE_INFO,
    CF_PLACEHOLDER_MANAGEMENT_POLICY_DEFAULT, CF_POPULATION_POLICY,
    CF_POPULATION_POLICY_MODIFIER_NONE, CF_POPULATION_POLICY_PARTIAL, CF_REGISTER_FLAG_UPDATE,
    CF_SYNC_POLICIES, CF_SYNC_REGISTRATION, CfConnectSyncRoot, CfCreatePlaceholders,
    CfDisconnectSyncRoot, CfExecute, CfRegisterSyncRoot,
};
use windows::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY, FILE_BASIC_INFO,
};
use windows::core::{GUID, PCWSTR};

use crate::{host_is_cancelled, host_log};

const DEFAULT_CLOUD_FILES_TIMEOUT: Duration = Duration::from_secs(3);
const WINDOWS_TICKS_PER_SECOND: u64 = 10_000_000;
const UNIX_EPOCH_AS_WINDOWS_FILETIME: u64 = 116_444_736 * WINDOWS_TICKS_PER_SECOND;
const CLOUD_FILES_PROVIDER_ID: GUID = GUID::from_u128(0xa318f9ce_140b_41bc_9e44_d0f154459584);

type SifsService = NetworkedDfsService<DfsCloudFilesEndpointTransport>;

pub fn mount_sifs_cloud_files(
    mut request: CloudFilesMountRequest,
) -> Result<CloudFilesMountResponse, CloudFilesModuleError> {
    request.root_path = normalize_sifs_root(&request.root_path)?;
    fs::create_dir_all(&request.sync_root).map_err(|source| CloudFilesModuleError {
        code: "sync-root-create".to_string(),
        detail: format!(
            "create Cloud Files sync root {}: {source}",
            request.sync_root.display()
        ),
    })?;

    let opened = open_sifs_service(request.data_dir_root.clone())?;
    let provider = Box::new(CloudFilesProvider::new(
        opened.service,
        request.sync_root.clone(),
        request.root_path.clone(),
        request.read_only,
    ));
    register_sync_root(&request)?;
    let placeholders_created = provider.prepopulate(request.prepopulate_depth)?;
    if !request.connect {
        return Ok(CloudFilesMountResponse {
            sync_root: request.sync_root,
            root_path: request.root_path,
            placeholders_created,
            connected: false,
            detail: "registered Cloud Files sync root without connecting callbacks".to_string(),
        });
    }

    let provider_ptr = Box::into_raw(provider);
    let callback_table = callback_table();
    let sync_root_wide = wide_path(&request.sync_root);
    let connection = unsafe {
        CfConnectSyncRoot(
            PCWSTR(sync_root_wide.as_ptr()),
            callback_table.as_ptr(),
            Some(provider_ptr.cast::<c_void>() as *const c_void),
            CF_CONNECT_FLAG_NONE,
        )
    }
    .map_err(|source| {
        unsafe {
            drop(Box::from_raw(provider_ptr));
        }
        CloudFilesModuleError {
            code: "connect-sync-root".to_string(),
            detail: format!(
                "connect Cloud Files sync root {}: {source}",
                request.sync_root.display()
            ),
        }
    })?;

    host_log(&format!(
        "Cloud Files provider connected {} -> {}",
        request.sync_root.display(),
        request.root_path
    ));
    let started = Instant::now();
    loop {
        if host_is_cancelled() {
            break;
        }
        if let Some(max_run_seconds) = request.max_run_seconds
            && started.elapsed() >= Duration::from_secs(max_run_seconds)
        {
            break;
        }
        thread::sleep(Duration::from_millis(250));
    }
    unsafe {
        let _ = CfDisconnectSyncRoot(connection);
        drop(Box::from_raw(provider_ptr));
    }
    Ok(CloudFilesMountResponse {
        sync_root: request.sync_root,
        root_path: request.root_path,
        placeholders_created,
        connected: true,
        detail: "Cloud Files provider disconnected after cancellation".to_string(),
    })
}

fn callback_table() -> [CF_CALLBACK_REGISTRATION; 3] {
    [
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_FETCH_DATA,
            Callback: Some(fetch_data_callback),
        },
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_FETCH_PLACEHOLDERS,
            Callback: Some(fetch_placeholders_callback),
        },
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_NONE,
            Callback: None,
        },
    ]
}

unsafe extern "system" fn fetch_data_callback(
    callback_info: *const CF_CALLBACK_INFO,
    callback_parameters: *const CF_CALLBACK_PARAMETERS,
) {
    let (Some(info), Some(parameters), Some(provider)) = (unsafe {
        (
            callback_info.as_ref(),
            callback_parameters.as_ref(),
            callback_info
                .as_ref()
                .and_then(|info| (info.CallbackContext as *const CloudFilesProvider).as_ref()),
        )
    }) else {
        return;
    };
    let fetch = unsafe { parameters.Anonymous.FetchData };
    let path = match unsafe { sifs_path_from_callback(info) } {
        Ok(path) => path,
        Err(status) => {
            let _ = unsafe { transfer_data(info, Vec::new(), 0, status) };
            return;
        }
    };
    let bytes = match provider.read_range(
        &path,
        fetch.RequiredFileOffset.max(0) as u64,
        fetch.RequiredLength.max(0) as usize,
    ) {
        Ok(bytes) => bytes,
        Err(status) => {
            let _ = unsafe { transfer_data(info, Vec::new(), 0, status) };
            return;
        }
    };
    let _ = unsafe { transfer_data(info, bytes, fetch.RequiredFileOffset.max(0), STATUS_SUCCESS) };
}

unsafe extern "system" fn fetch_placeholders_callback(
    callback_info: *const CF_CALLBACK_INFO,
    _callback_parameters: *const CF_CALLBACK_PARAMETERS,
) {
    let (Some(info), Some(provider)) = (unsafe {
        (
            callback_info.as_ref(),
            callback_info
                .as_ref()
                .and_then(|info| (info.CallbackContext as *const CloudFilesProvider).as_ref()),
        )
    }) else {
        return;
    };
    let path = match unsafe { sifs_path_from_callback(info) } {
        Ok(path) => path,
        Err(status) => {
            let _ = unsafe { transfer_placeholders(info, Vec::new(), status) };
            return;
        }
    };
    let placeholders = match provider.placeholder_payloads_for_dir(&path) {
        Ok(placeholders) => placeholders,
        Err(status) => {
            let _ = unsafe { transfer_placeholders(info, Vec::new(), status) };
            return;
        }
    };
    let _ = unsafe { transfer_placeholders(info, placeholders, STATUS_SUCCESS) };
}

unsafe fn transfer_data(
    info: &CF_CALLBACK_INFO,
    bytes: Vec<u8>,
    offset: i64,
    status: NTSTATUS,
) -> windows::core::Result<()> {
    let mut params = CF_OPERATION_PARAMETERS {
        ParamSize: std::mem::size_of::<CF_OPERATION_PARAMETERS>() as u32,
        Anonymous: CF_OPERATION_PARAMETERS_0 {
            TransferData: CF_OPERATION_PARAMETERS_0_0 {
                Flags: CF_OPERATION_TRANSFER_DATA_FLAG_NONE,
                CompletionStatus: status,
                Buffer: if status == STATUS_SUCCESS {
                    bytes.as_ptr().cast::<c_void>()
                } else {
                    ptr::null()
                },
                Offset: offset,
                Length: if status == STATUS_SUCCESS {
                    bytes.len() as i64
                } else {
                    0
                },
            },
        },
    };
    let op = operation_info(info, CF_OPERATION_TYPE_TRANSFER_DATA);
    unsafe { CfExecute(&op, &mut params) }
}

unsafe fn transfer_placeholders(
    info: &CF_CALLBACK_INFO,
    mut payloads: Vec<PlaceholderPayload>,
    status: NTSTATUS,
) -> windows::core::Result<()> {
    let mut infos = payloads
        .iter_mut()
        .map(PlaceholderPayload::create_info)
        .collect::<Vec<_>>();
    let mut params = CF_OPERATION_PARAMETERS {
        ParamSize: std::mem::size_of::<CF_OPERATION_PARAMETERS>() as u32,
        Anonymous: CF_OPERATION_PARAMETERS_0 {
            TransferPlaceholders: CF_OPERATION_PARAMETERS_0_4 {
                Flags: CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                CompletionStatus: status,
                PlaceholderTotalCount: infos.len() as i64,
                PlaceholderArray: if status == STATUS_SUCCESS && !infos.is_empty() {
                    infos.as_mut_ptr()
                } else {
                    ptr::null_mut()
                },
                PlaceholderCount: if status == STATUS_SUCCESS {
                    infos.len() as u32
                } else {
                    0
                },
                EntriesProcessed: 0,
            },
        },
    };
    let op = operation_info(info, CF_OPERATION_TYPE_TRANSFER_PLACEHOLDERS);
    unsafe { CfExecute(&op, &mut params) }
}

fn operation_info(
    info: &CF_CALLBACK_INFO,
    operation_type: windows::Win32::Storage::CloudFilters::CF_OPERATION_TYPE,
) -> CF_OPERATION_INFO {
    CF_OPERATION_INFO {
        StructSize: std::mem::size_of::<CF_OPERATION_INFO>() as u32,
        Type: operation_type,
        ConnectionKey: info.ConnectionKey,
        TransferKey: info.TransferKey,
        CorrelationVector: info.CorrelationVector as *const _,
        SyncStatus: ptr::null(),
        RequestKey: info.RequestKey,
    }
}

unsafe fn sifs_path_from_callback(info: &CF_CALLBACK_INFO) -> Result<String, NTSTATUS> {
    if !info.FileIdentity.is_null() && info.FileIdentityLength > 0 {
        return bytes_to_sifs_path(unsafe {
            std::slice::from_raw_parts(
                info.FileIdentity.cast::<u8>(),
                info.FileIdentityLength as usize,
            )
        });
    }
    if !info.SyncRootIdentity.is_null() && info.SyncRootIdentityLength > 0 {
        return bytes_to_sifs_path(unsafe {
            std::slice::from_raw_parts(
                info.SyncRootIdentity.cast::<u8>(),
                info.SyncRootIdentityLength as usize,
            )
        });
    }
    Err(STATUS_OBJECT_NAME_NOT_FOUND)
}

fn bytes_to_sifs_path(bytes: &[u8]) -> Result<String, NTSTATUS> {
    std::str::from_utf8(bytes)
        .map_err(|_| STATUS_OBJECT_NAME_NOT_FOUND)
        .and_then(|path| normalize_sifs_root(path).map_err(|_| STATUS_OBJECT_NAME_NOT_FOUND))
}

struct CloudFilesProvider {
    dfs: Mutex<SifsService>,
    sync_root: PathBuf,
    root_path: String,
    read_only: bool,
}

impl CloudFilesProvider {
    fn new(dfs: SifsService, sync_root: PathBuf, root_path: String, read_only: bool) -> Self {
        Self {
            dfs: Mutex::new(dfs),
            sync_root,
            root_path,
            read_only,
        }
    }

    fn lock_dfs(&self) -> Result<MutexGuard<'_, SifsService>, NTSTATUS> {
        self.dfs.lock().map_err(|_| STATUS_UNSUCCESSFUL)
    }

    fn prepopulate(&self, depth: u32) -> Result<u64, CloudFilesModuleError> {
        if depth == 0 {
            return Ok(0);
        }
        self.prepopulate_dir(&self.root_path, &self.sync_root, depth)
    }

    fn prepopulate_dir(
        &self,
        sifs_path: &str,
        local_base: &Path,
        depth: u32,
    ) -> Result<u64, CloudFilesModuleError> {
        let payloads = self
            .placeholder_payloads_for_dir(sifs_path)
            .map_err(|status| CloudFilesModuleError {
                code: "prepopulate-placeholders".to_string(),
                detail: format!("read SIFS directory `{sifs_path}` for Cloud Files: {status:?}"),
            })?;
        let mut created = create_placeholders(local_base, payloads)?;
        let Some(child_depth) = child_prepopulate_depth(depth) else {
            return Ok(created);
        };
        let children = self
            .lock_dfs()
            .and_then(|mut dfs| {
                dfs.posix_list_dir_distributed(sifs_path)
                    .map_err(ntstatus_for_posix_error)
            })
            .map_err(|status| CloudFilesModuleError {
                code: "prepopulate-list".to_string(),
                detail: format!("list SIFS directory `{sifs_path}` for Cloud Files: {status:?}"),
            })?;
        for child in children {
            if child.kind != DfsObjectKind::Directory && child.kind != DfsObjectKind::Mount {
                continue;
            }
            let child_base = local_base.join(&child.name);
            created += self.prepopulate_dir(&child.path, &child_base, child_depth)?;
        }
        Ok(created)
    }

    fn placeholder_payloads_for_dir(
        &self,
        sifs_path: &str,
    ) -> Result<Vec<PlaceholderPayload>, NTSTATUS> {
        let children = {
            let mut dfs = self.lock_dfs()?;
            dfs.posix_list_dir_distributed(sifs_path)
                .map_err(ntstatus_for_posix_error)?
        };
        let mut payloads = Vec::new();
        for child in children {
            let stat = {
                let mut dfs = self.lock_dfs()?;
                dfs.posix_stat_distributed(&child.path)
                    .map_err(ntstatus_for_posix_error)?
            };
            if !is_visible_placeholder_kind(&stat.kind) {
                continue;
            }
            payloads.push(PlaceholderPayload::new(child.name, stat, self.read_only));
        }
        Ok(payloads)
    }

    fn read_range(&self, sifs_path: &str, offset: u64, len: usize) -> Result<Vec<u8>, NTSTATUS> {
        let bytes = {
            let mut dfs = self.lock_dfs()?;
            dfs.posix_read_file_distributed(sifs_path)
                .map_err(ntstatus_for_posix_error)?
        };
        Ok(slice_bytes(&bytes, offset as usize, len))
    }
}

fn child_prepopulate_depth(depth: u32) -> Option<u32> {
    depth.checked_sub(1).filter(|child_depth| *child_depth > 0)
}

fn is_visible_placeholder_kind(kind: &DfsObjectKind) -> bool {
    *kind != DfsObjectKind::Tombstone
}

struct PlaceholderPayload {
    display_name: String,
    relative_name: Vec<u16>,
    identity: Vec<u8>,
    metadata: CF_FS_METADATA,
    flags: windows::Win32::Storage::CloudFilters::CF_PLACEHOLDER_CREATE_FLAGS,
}

impl PlaceholderPayload {
    fn new(relative_name: String, stat: dfs::DfsStat, read_only: bool) -> Self {
        let mut attributes =
            if stat.kind == DfsObjectKind::Directory || stat.kind == DfsObjectKind::Mount {
                FILE_ATTRIBUTE_DIRECTORY.0
            } else {
                FILE_ATTRIBUTE_NORMAL.0
            };
        if read_only {
            attributes |= FILE_ATTRIBUTE_READONLY.0;
        }
        let size = if stat.kind == DfsObjectKind::Directory || stat.kind == DfsObjectKind::Mount {
            0
        } else {
            stat.size_bytes.unwrap_or(0) as i64
        };
        Self {
            display_name: relative_name.clone(),
            relative_name: wide_str(&relative_name),
            identity: stat.path.into_bytes(),
            metadata: CF_FS_METADATA {
                BasicInfo: FILE_BASIC_INFO {
                    CreationTime: unix_nanos_to_filetime(stat.created_at_unix_nanos),
                    LastAccessTime: unix_nanos_to_filetime(stat.accessed_at_unix_nanos),
                    LastWriteTime: unix_nanos_to_filetime(stat.modified_at_unix_nanos),
                    ChangeTime: unix_nanos_to_filetime(stat.changed_at_unix_nanos),
                    FileAttributes: attributes,
                },
                FileSize: size,
            },
            flags: CF_PLACEHOLDER_CREATE_FLAG_MARK_IN_SYNC | CF_PLACEHOLDER_CREATE_FLAG_SUPERSEDE,
        }
    }

    fn create_info(&mut self) -> CF_PLACEHOLDER_CREATE_INFO {
        CF_PLACEHOLDER_CREATE_INFO {
            RelativeFileName: PCWSTR(self.relative_name.as_ptr()),
            FsMetadata: self.metadata,
            FileIdentity: self.identity.as_ptr().cast::<c_void>(),
            FileIdentityLength: self.identity.len() as u32,
            Flags: self.flags,
            Result: windows::core::HRESULT(0),
            CreateUsn: 0,
        }
    }
}

fn create_placeholders(
    base: &Path,
    mut payloads: Vec<PlaceholderPayload>,
) -> Result<u64, CloudFilesModuleError> {
    if payloads.is_empty() {
        return Ok(0);
    }
    let base_wide = wide_path(base);
    let mut infos = payloads
        .iter_mut()
        .map(PlaceholderPayload::create_info)
        .collect::<Vec<_>>();
    let mut entries_processed = 0;
    unsafe {
        CfCreatePlaceholders(
            PCWSTR(base_wide.as_ptr()),
            &mut infos,
            CF_CREATE_FLAG_NONE,
            Some(&mut entries_processed),
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: "create-placeholders".to_string(),
        detail: format!(
            "create Cloud Files placeholders under {}: {source}; entries_processed={}; {}",
            base.display(),
            entries_processed,
            placeholder_results_detail(&payloads, &infos)
        ),
    })?;
    Ok(entries_processed as u64)
}

fn placeholder_results_detail(
    payloads: &[PlaceholderPayload],
    infos: &[CF_PLACEHOLDER_CREATE_INFO],
) -> String {
    let results = payloads
        .iter()
        .zip(infos.iter())
        .map(|(payload, info)| format!("{}=0x{:08X}", payload.display_name, info.Result.0 as u32))
        .collect::<Vec<_>>();
    if results.is_empty() {
        "placeholder_results=[]".to_string()
    } else {
        format!("placeholder_results=[{}]", results.join(", "))
    }
}

fn register_sync_root(request: &CloudFilesMountRequest) -> Result<(), CloudFilesModuleError> {
    let sync_root_wide = wide_path(&request.sync_root);
    let provider_name = wide_str(&request.provider_name);
    let provider_version = wide_str(&request.provider_version);
    let sync_root_identity = request.root_path.as_bytes();
    let registration = CF_SYNC_REGISTRATION {
        StructSize: std::mem::size_of::<CF_SYNC_REGISTRATION>() as u32,
        ProviderName: PCWSTR(provider_name.as_ptr()),
        ProviderVersion: PCWSTR(provider_version.as_ptr()),
        SyncRootIdentity: sync_root_identity.as_ptr().cast::<c_void>(),
        SyncRootIdentityLength: sync_root_identity.len() as u32,
        FileIdentity: ptr::null(),
        FileIdentityLength: 0,
        ProviderId: CLOUD_FILES_PROVIDER_ID,
    };
    let policies = CF_SYNC_POLICIES {
        StructSize: std::mem::size_of::<CF_SYNC_POLICIES>() as u32,
        Hydration: CF_HYDRATION_POLICY {
            Primary: CF_HYDRATION_POLICY_PROGRESSIVE,
            Modifier: CF_HYDRATION_POLICY_MODIFIER_AUTO_DEHYDRATION_ALLOWED,
        },
        Population: CF_POPULATION_POLICY {
            Primary: CF_POPULATION_POLICY_PARTIAL,
            Modifier: CF_POPULATION_POLICY_MODIFIER_NONE,
        },
        InSync: CF_INSYNC_POLICY_TRACK_ALL,
        HardLink: CF_HARDLINK_POLICY_NONE,
        PlaceholderManagement: CF_PLACEHOLDER_MANAGEMENT_POLICY_DEFAULT,
    };
    unsafe {
        CfRegisterSyncRoot(
            PCWSTR(sync_root_wide.as_ptr()),
            &registration,
            &policies,
            CF_REGISTER_FLAG_UPDATE,
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: "register-sync-root".to_string(),
        detail: format!(
            "register Cloud Files sync root {}: {source}",
            request.sync_root.display()
        ),
    })
}

#[derive(Debug)]
struct OpenedSifsService {
    service: SifsService,
}

fn open_sifs_service(
    data_dir_root: Option<PathBuf>,
) -> Result<OpenedSifsService, CloudFilesModuleError> {
    let data_dir = match data_dir_root {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home().map_err(error("local-data"))?,
    };
    data_dir.ensure_root_layout().map_err(error("local-data"))?;
    let cluster_paths = network::ClusterPaths::new(&data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        network::read_cluster_state_without_kv(&cluster_paths).ok()
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(dfs::fallback_dfs_node_id);
    let endpoints = cluster_state
        .as_ref()
        .map(|state| network::cluster_dfs_protocol_targets(&cluster_paths, state))
        .unwrap_or_default();
    let content =
        LocalContentStore::new(data_dir.dfs_dir().join("content")).map_err(error("dfs"))?;
    let mut service = NetworkedDfsService::new(
        DfsCloudFilesEndpointTransport::new(local_node_id, endpoints),
        content,
    );
    if cluster_paths.kv_snapshot.exists() {
        service = service.with_control_plane(DfsCloudFilesControlPlaneKvClient {
            cluster_paths: cluster_paths.clone(),
        });
    }
    let metadata = open_metadata_backend(&data_dir, &cluster_paths)?;
    let mut service = service
        .with_boxed_metadata_backend(metadata)
        .map_err(error("dfs"))?;
    service.bootstrap_distributed_sifs().map_err(error("dfs"))?;
    Ok(OpenedSifsService { service })
}

fn open_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &network::ClusterPaths,
) -> Result<Box<dyn dfs::DfsMetadataBackend>, CloudFilesModuleError> {
    if cluster_paths.state.exists() {
        let file_system_id = dfs::sifs_file_system_id();
        let mut shards = read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            &format!("{}/", dfs::metadata_shards_prefix(&file_system_id)),
        )
        .map_err(error("network"))?
        .into_iter()
        .map(|entry| entry.decode::<dfs::DfsMetadataShardConfig>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(error("dfs"))?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        if !shards.is_empty() {
            let backend = dfs::open_local_sharded_metadata_backend(
                dfs::default_dfs_metadata_shards_root(data_dir),
                shards,
            )
            .map_err(error("dfs"))?;
            return Ok(Box::new(backend));
        }
    }
    dfs::open_root_local_metadata_backend(dfs::default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn dfs::DfsMetadataBackend>)
        .map_err(error("dfs"))
}

#[derive(Clone, Debug)]
struct DfsCloudFilesControlPlaneKvClient {
    cluster_paths: network::ClusterPaths,
}

impl DfsCloudFilesControlPlaneKvClient {
    fn daemon_error(&self, source: impl std::fmt::Display) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl DfsControlPlaneKvClient for DfsCloudFilesControlPlaneKvClient {
    fn get(&self, key: &str) -> std::result::Result<Option<KvEntry>, KvError> {
        read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> std::result::Result<Vec<KvEntry>, KvError> {
        read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> std::result::Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> std::result::Result<KvTxnResponse, KvError> {
        let mut state = network::read_cluster_state(&self.cluster_paths)
            .map_err(|source| self.daemon_error(source))?;
        let snapshot =
            daemon::commit_cluster_kv_txn_with_quorum(&self.cluster_paths, &mut state, txn)
                .map_err(|source| self.daemon_error(source))?;
        Ok(KvTxnResponse {
            succeeded: true,
            revision: snapshot.revision,
            responses: Vec::new(),
        })
    }
}

#[derive(Clone)]
struct DfsCloudFilesEndpointTransport {
    local_node_id: NodeId,
    endpoints_by_node_id: BTreeMap<NodeId, Vec<ClusterEndpointRecord>>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    timeout: Duration,
}

impl std::fmt::Debug for DfsCloudFilesEndpointTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsCloudFilesEndpointTransport")
            .field("local_node_id", &self.local_node_id)
            .field("endpoints_by_node_id", &self.endpoints_by_node_id)
            .field("incoming_messages", &self.incoming_messages)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl DfsCloudFilesEndpointTransport {
    fn new(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    ) -> Self {
        let mut endpoints_by_node_id = BTreeMap::<NodeId, Vec<ClusterEndpointRecord>>::new();
        for endpoint in endpoints {
            if endpoint.node_id != local_node_id {
                endpoints_by_node_id
                    .entry(endpoint.node_id.clone())
                    .or_default()
                    .push(endpoint);
            }
        }
        Self {
            local_node_id,
            endpoints_by_node_id,
            incoming_messages: VecDeque::new(),
            timeout: DEFAULT_CLOUD_FILES_TIMEOUT,
        }
    }

    fn deliver_returned_message(
        &mut self,
        message: DfsRoutedMessage,
    ) -> std::result::Result<(), NetworkError> {
        let target_node_id = message.target_node_id.clone();
        if target_node_id == self.local_node_id {
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.send_routed_message_to_endpoint(&target_node_id, message, false)
    }

    fn send_routed_message_to_endpoint(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
        require_local_source: bool,
    ) -> std::result::Result<(), NetworkError> {
        if require_local_source && message.source_node_id != self.local_node_id {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: message.source_node_id,
            });
        }
        if !require_local_source
            && message.source_node_id != self.local_node_id
            && !self
                .endpoints_by_node_id
                .contains_key(&message.source_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: message.source_node_id,
            });
        }
        if &message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: message.target_node_id,
            });
        }
        let endpoints = self
            .endpoints_by_node_id
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })?;
        let request = DfsMessageCommand {
            request_id: format!("cloud_files_dfs_message_{}", message.frame_id),
            routed_message: message,
        };
        let mut failures = Vec::new();
        for endpoint in &endpoints {
            match send_dfs_message(endpoint, &request, self.timeout) {
                Ok(response) => {
                    for returned_message in response.outbound_messages {
                        self.deliver_returned_message(returned_message)?;
                    }
                    return Ok(());
                }
                Err(error) => failures.push(format!("{} failed: {error}", endpoint.address)),
            }
        }
        Err(NetworkError::Io {
            operation: format!("send DFS message to {target_node_id}"),
            detail: failures.join("; "),
        })
    }
}

impl DfsTransportIdentity for DfsCloudFilesEndpointTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for DfsCloudFilesEndpointTransport {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> std::result::Result<(), NetworkError> {
        self.send_routed_message_to_endpoint(target_node_id, message, true)
    }

    fn drain_incoming(&mut self) -> std::result::Result<Vec<InboundFrame>, NetworkError> {
        self.incoming_messages
            .drain(..)
            .map(|message| {
                dfs_network_frame_from_routed_message(message).map(|frame| InboundFrame {
                    received_by: self.local_node_id.clone(),
                    frame,
                })
            })
            .collect()
    }

    fn drain_incoming_messages(
        &mut self,
    ) -> std::result::Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self.incoming_messages.drain(..).collect())
    }
}

fn send_dfs_message(
    endpoint: &ClusterEndpointRecord,
    request: &DfsMessageCommand,
    timeout: Duration,
) -> std::result::Result<DfsMessageResponse, CloudFilesModuleError> {
    let rpc_request = dfs::dfs_message_rpc_request(request).map_err(error("dfs-message"))?;
    let response = daemon::send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)
        .map_err(error("daemon"))?;
    dfs::decode_dfs_message_rpc_response(&response).map_err(error("dfs-message"))
}

fn dfs_network_frame_from_routed_message(
    message: DfsRoutedMessage,
) -> std::result::Result<NetworkFrame, NetworkError> {
    let protocol_message = ProtocolMessage::encode(dfs::DFS_PROTOCOL, &message.message)?;
    Ok(NetworkFrame {
        frame_id: NetworkFrameId(message.frame_id),
        source_node_id: message.source_node_id.clone(),
        target_node_id: message.target_node_id.clone(),
        message: MeshMessage {
            message_id: MeshMessageId(message.message_id),
            origin_node_id: message.source_node_id,
            target_node_id: message.target_node_id,
            body: MeshMessageBody::Protocol(protocol_message),
        },
    })
}

fn normalize_sifs_root(path: &str) -> Result<String, CloudFilesModuleError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(CloudFilesModuleError {
            code: "invalid-path".to_string(),
            detail: "SIFS root path cannot be empty".to_string(),
        });
    }
    if !trimmed.starts_with('/') {
        return Err(CloudFilesModuleError {
            code: "invalid-path".to_string(),
            detail: format!("SIFS root path `{path}` must be absolute"),
        });
    }
    let mut parts = Vec::new();
    for component in trimmed.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn ntstatus_for_posix_error(error: DfsPosixError) -> NTSTATUS {
    match error {
        DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
            STATUS_OBJECT_NAME_NOT_FOUND
        }
        DfsPosixError::NotDirectory { .. } => STATUS_NOT_A_DIRECTORY,
        DfsPosixError::IsDirectory { .. } => STATUS_FILE_IS_A_DIRECTORY,
        _ => STATUS_UNSUCCESSFUL,
    }
}

fn unix_nanos_to_filetime(nanos: u64) -> i64 {
    (UNIX_EPOCH_AS_WINDOWS_FILETIME + nanos / 100).min(i64::MAX as u64) as i64
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn wide_str(value: &str) -> Vec<u16> {
    OsStr::new(value).encode_wide().chain(Some(0)).collect()
}

fn error<E>(code: &'static str) -> impl FnOnce(E) -> CloudFilesModuleError
where
    E: std::fmt::Display,
{
    move |source| CloudFilesModuleError {
        code: code.to_string(),
        detail: source.to_string(),
    }
}

fn slice_bytes(bytes: &[u8], start: usize, len: usize) -> Vec<u8> {
    if start >= bytes.len() {
        return Vec::new();
    }
    let end = start.saturating_add(len).min(bytes.len());
    bytes[start..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepopulate_depth_counts_visible_levels() {
        assert_eq!(child_prepopulate_depth(0), None);
        assert_eq!(child_prepopulate_depth(1), None);
        assert_eq!(child_prepopulate_depth(2), Some(1));
        assert_eq!(child_prepopulate_depth(3), Some(2));
    }

    #[test]
    fn tombstones_are_not_cloud_files_placeholders() {
        assert!(is_visible_placeholder_kind(&DfsObjectKind::File));
        assert!(is_visible_placeholder_kind(&DfsObjectKind::Directory));
        assert!(!is_visible_placeholder_kind(&DfsObjectKind::Tombstone));
    }
}
