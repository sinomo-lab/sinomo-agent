use crate::{
    state::{Shared, Tab},
    ui::chat::ChatWorkspaceSnapshot,
};
use sinomo_runtime::{
    core::storage::durable_file::{
        DurableAtomicFileOptions, ensure_private_directory, write_file_atomic,
    },
    local::SinomoDataDir,
};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkspaceSnapshot {
    pub schema_version: u32,
    pub active_tab: Tab,
    pub chat: ChatWorkspaceSnapshot,
    #[serde(default)]
    pub files: Option<crate::ui::files::FilesWorkspaceSnapshot>,
    #[serde(default)]
    pub tabs: Vec<OpenTabSnapshot>,
    #[serde(default)]
    pub active_open_tab: Option<usize>,
    #[serde(default)]
    pub current_open_chat: Option<usize>,
    #[serde(default)]
    pub window: WindowPlacement,
}

#[derive(Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct WindowPlacement {
    pub size: Option<[f32; 2]>,
    pub position: Option<[f32; 2]>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum OpenTabSnapshot {
    Chat(Box<ChatWorkspaceSnapshot>),
    Files {
        viewer: bool,
        state: crate::ui::files::FilesWorkspaceSnapshot,
    },
}

pub(crate) fn load(data_dir: &SinomoDataDir) -> Result<Option<WorkspaceSnapshot>, String> {
    let path = data_dir.shared_configs_dir().join("app/workspace.json");
    match std::fs::metadata(&path) {
        Ok(metadata) if metadata.len() > 128 * 1024 * 1024 => {
            return Err("workspace restoration file is too large".into());
        }
        _ => {}
    }
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let snapshot: WorkspaceSnapshot = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if snapshot.schema_version != 1 {
        return Err("unsupported workspace restoration version".into());
    }
    Ok(Some(snapshot))
}

pub(crate) fn save(shared: &Shared) -> Result<(), String> {
    let app = shared.borrow();
    let ready = app.ready.as_ref().ok_or("app is not connected")?;
    let snapshot = WorkspaceSnapshot {
        schema_version: 1,
        active_tab: app.active_tab,
        chat: ready.chat.workspace_snapshot(),
        files: Some(ready.files.workspace_snapshot()),
        tabs: shared.workspace_snapshots(),
        active_open_tab: app
            .workspace_tabs
            .iter()
            .position(|tab| Some(tab.id) == app.active_workspace_tab),
        current_open_chat: app
            .workspace_tabs
            .iter()
            .position(|tab| Some(tab.id) == app.last_chat_tab),
        window: app.window_placement,
    };
    let bytes = serde_json::to_vec(&snapshot).map_err(|e| e.to_string())?;
    if bytes.len() > 128 * 1024 * 1024 {
        return Err("workspace snapshot exceeds the restoration limit".into());
    }
    std::fs::create_dir_all(app.data_dir.shared_configs_dir()).map_err(|e| e.to_string())?;
    ensure_private_directory(&app.data_dir.shared_configs_dir().join("app"))
        .map_err(|e| e.to_string())?;
    let path = app
        .data_dir
        .root()
        .join(sinomo_client::api::app::update::APP_WORKSPACE_SNAPSHOT_PATH);
    write_file_atomic(
        &path,
        &bytes,
        DurableAtomicFileOptions::new("workspace", "restore").sync_all(),
    )
    .map_err(|e| e.to_string())
}
