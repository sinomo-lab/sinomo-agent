//! UI-owned tab identities. Each retained surface captures its own state, so a late
//! worker result or widget callback can never be redirected by tab selection.
use super::*;
use crate::ui::files::FilesSourceKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceTabId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceTabKind {
    Chat,
    Files,
    Document,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkspaceSelection {
    Pinned(Tab),
    Open(WorkspaceTabId),
}

pub(crate) enum WorkspaceContent {
    Chat(RefCell<ChatState>),
    Files(RefCell<FilesState>),
}

pub(crate) struct WorkspaceTab {
    pub id: WorkspaceTabId,
    pub kind: WorkspaceTabKind,
    pub content: WorkspaceContent,
    pub signals: AppSignals,
}

impl WorkspaceTab {
    pub fn chat(&self) -> Option<&RefCell<ChatState>> {
        match &self.content {
            WorkspaceContent::Chat(chat) => Some(chat),
            _ => None,
        }
    }

    pub fn files(&self) -> Option<&RefCell<FilesState>> {
        match &self.content {
            WorkspaceContent::Files(files) => Some(files),
            _ => None,
        }
    }

    pub fn title(&self) -> String {
        match &self.content {
            WorkspaceContent::Chat(chat) => chat.borrow().tab_title(),
            WorkspaceContent::Files(files) => files.borrow().tab_title(),
        }
    }
}

impl Shared {
    pub(crate) fn chat_scopes(&self) -> Vec<Self> {
        std::iter::once(self.root_scope())
            .chain(
                self.workspace_scopes()
                    .into_iter()
                    .filter(|scope| scope.workspace_kind() == Some(WorkspaceTabKind::Chat)),
            )
            .collect()
    }

    pub(crate) fn workspace_snapshots(&self) -> Vec<crate::restoration::OpenTabSnapshot> {
        use crate::restoration::OpenTabSnapshot;
        self.workspace_scopes()
            .iter()
            .map(|scope| match scope.workspace_kind() {
                Some(WorkspaceTabKind::Chat) => {
                    OpenTabSnapshot::Chat(Box::new(scope.chat().workspace_snapshot()))
                }
                kind => OpenTabSnapshot::Files {
                    viewer: kind == Some(WorkspaceTabKind::Document),
                    state: scope.files().workspace_snapshot(),
                },
            })
            .collect()
    }

    pub(crate) fn restore_workspace_tabs(&self, snapshot: crate::restoration::WorkspaceSnapshot) {
        use crate::restoration::OpenTabSnapshot;
        let (backend, waker) = {
            let state = self.state.borrow();
            let Some(ready) = &state.ready else {
                return;
            };
            (ready.backend.clone(), state.waker.clone())
        };
        if let Some(files) = snapshot.files {
            self.files_mut().restore_workspace(files, waker.clone());
        }
        let mut ids = Vec::new();
        for tab in snapshot.tabs {
            let (kind, content) = match tab {
                OpenTabSnapshot::Chat(chat) => (
                    WorkspaceTabKind::Chat,
                    WorkspaceContent::Chat(RefCell::new(ChatState::new_tab(
                        &backend,
                        waker.clone(),
                        Some(*chat),
                    ))),
                ),
                OpenTabSnapshot::Files { viewer, state } => {
                    let mut files = FilesState::new_tab(&backend, &self.root_scope().files());
                    files.restore_workspace(state, waker.clone());
                    (
                        if viewer {
                            WorkspaceTabKind::Document
                        } else {
                            WorkspaceTabKind::Files
                        },
                        WorkspaceContent::Files(RefCell::new(files)),
                    )
                }
            };
            ids.push(self.insert_workspace_tab(kind, content));
        }
        let current_chat = snapshot
            .current_open_chat
            .and_then(|index| ids.get(index))
            .copied()
            .filter(|id| {
                self.workspace_scope(*id)
                    .is_some_and(|scope| scope.workspace_kind() == Some(WorkspaceTabKind::Chat))
            });
        self.state.borrow_mut().last_chat_tab = current_chat;
        if let Some(id) = snapshot.active_open_tab.and_then(|index| ids.get(index)) {
            self.select_workspace_tab(*id);
        } else {
            self.set_active_tab(snapshot.active_tab);
        }
    }

    pub(crate) fn root_scope(&self) -> Self {
        Self {
            state: self.state.clone(),
            signals: self.signals.clone(),
            scope: None,
        }
    }

    pub(crate) fn workspace_scope(&self, id: WorkspaceTabId) -> Option<Self> {
        let scope = self
            .state
            .borrow()
            .workspace_tabs
            .iter()
            .find(|tab| tab.id == id)?
            .clone();
        Some(Self {
            state: self.state.clone(),
            signals: self.signals.clone(),
            scope: Some(scope),
        })
    }

    pub(crate) fn workspace_scopes(&self) -> Vec<Self> {
        self.state
            .borrow()
            .workspace_tabs
            .iter()
            .map(|scope| Self {
                state: self.state.clone(),
                signals: self.signals.clone(),
                scope: Some(scope.clone()),
            })
            .collect()
    }

    pub(crate) fn active_scope(&self) -> Self {
        let id = self.state.borrow().active_workspace_tab;
        id.and_then(|id| self.workspace_scope(id))
            .unwrap_or_else(|| self.root_scope())
    }

    pub(crate) fn current_chat_scope(&self) -> Self {
        let id = self.state.borrow().last_chat_tab;
        id.and_then(|id| self.workspace_scope(id))
            .unwrap_or_else(|| self.root_scope())
    }

    pub(crate) fn workspace_kind(&self) -> Option<WorkspaceTabKind> {
        self.scope.as_ref().map(|tab| tab.kind)
    }

    pub(crate) fn workspace_id(&self) -> Option<WorkspaceTabId> {
        self.scope.as_ref().map(|tab| tab.id)
    }

    pub(crate) fn is_active_scope(&self) -> bool {
        self.state.borrow().active_workspace_tab == self.workspace_id()
    }

    pub(crate) fn publish_workspace_signals(&self) {
        let (selection, tabs) = {
            let state = self.state.borrow();
            (
                state
                    .active_workspace_tab
                    .map(WorkspaceSelection::Open)
                    .unwrap_or(WorkspaceSelection::Pinned(state.active_tab)),
                state.workspace_tabs.iter().map(|tab| tab.id).collect(),
            )
        };
        self.signals.workspace_tabs.set(tabs);
        self.signals.workspace_selection.set(selection);
        let titles = self
            .state
            .borrow()
            .workspace_tabs
            .iter()
            .map(|tab| tab.title())
            .collect();
        self.signals.workspace_titles.set(titles);
    }

    pub(crate) fn sync_chat_tab_visibility(&self) {
        let root = self.root_scope();
        for shared in std::iter::once(root).chain(self.workspace_scopes()) {
            if shared
                .workspace_kind()
                .is_none_or(|kind| kind == WorkspaceTabKind::Chat)
            {
                let visible = shared.is_active_scope() && shared.active_tab() == Tab::Sinomo;
                shared.chat_mut().sync_media_setup_visibility(visible);
            }
        }
    }

    pub fn select_workspace_tab(&self, id: WorkspaceTabId) -> bool {
        let Some(scope) = self.workspace_scope(id) else {
            return false;
        };
        {
            let mut state = self.state.borrow_mut();
            state.active_workspace_tab = Some(id);
            state.active_tab = if scope.workspace_kind() == Some(WorkspaceTabKind::Chat) {
                Tab::Sinomo
            } else {
                Tab::Files
            };
            if scope.workspace_kind() == Some(WorkspaceTabKind::Chat) {
                state.last_chat_tab = Some(id);
            }
        }
        self.sync_chat_tab_visibility();
        self.publish_shell_signals();
        true
    }

    pub fn new_workspace_tab(&self, kind: WorkspaceTabKind) -> Option<WorkspaceTabId> {
        if kind == WorkspaceTabKind::Document || self.borrow().updates.view.busy {
            return None;
        }
        let (backend, waker) = {
            let state = self.state.borrow();
            (state.ready.as_ref()?.backend.clone(), state.waker.clone())
        };
        let content = match kind {
            WorkspaceTabKind::Chat => {
                let mut chat = ChatState::new_tab(&backend, waker.clone(), None);
                chat.start_provider_list_load(backend.config.clone(), waker.clone());
                chat.start_agent_list_load(waker.clone());
                chat.start_session_list_load(waker);
                WorkspaceContent::Chat(RefCell::new(chat))
            }
            _ => WorkspaceContent::Files(RefCell::new(FilesState::new_tab(
                &backend,
                &self.root_scope().files(),
            ))),
        };
        Some(self.insert_workspace_tab(kind, content))
    }

    fn insert_workspace_tab(
        &self,
        kind: WorkspaceTabKind,
        content: WorkspaceContent,
    ) -> WorkspaceTabId {
        let mut signals = (*self.signals).clone();
        let state = self.state.borrow();
        // Only the owned surface is scoped. Cross-surface actions still publish to
        // the pinned destination rather than an unused per-tab signal.
        match kind {
            WorkspaceTabKind::Chat => {
                signals.chat_structure = Signal::named(
                    "tab chat structure",
                    ChatStructureKey::new(state.theme_mode, state.text_scale, None),
                );
            }
            _ => {
                signals.files_structure = Signal::named(
                    "tab files structure",
                    FilesStructureKey::new(state.theme_mode, state.text_scale, None),
                );
                signals.files_thumbnail_revision = Signal::named("tab thumbnails", 0);
            }
        }
        drop(state);
        let id = {
            let mut state = self.state.borrow_mut();
            let id = WorkspaceTabId(state.next_workspace_tab_id);
            state.next_workspace_tab_id += 1;
            state.workspace_tabs.push(Rc::new(WorkspaceTab {
                id,
                kind,
                content,
                signals,
            }));
            id
        };
        let scope = self.workspace_scope(id).expect("inserted tab");
        match kind {
            WorkspaceTabKind::Chat => scope.publish_chat_signals(),
            _ => scope.publish_files_signals(),
        }
        self.select_workspace_tab(id);
        id
    }

    pub(crate) fn open_files_tab(
        &self,
        source: FilesSourceKind,
        path: String,
    ) -> Option<WorkspaceTabId> {
        let id = self.new_workspace_tab(WorkspaceTabKind::Files)?;
        let scope = self.workspace_scope(id)?;
        let waker = self.borrow().waker.clone();
        scope.files_mut().open_location(source, path, waker);
        Some(id)
    }

    pub(crate) fn open_generated_report(&self, source: FilesSourceKind, base: String, html: String) {
        let Some(backend) = self.borrow().ready.as_ref().map(|r| r.backend.clone()) else {
            return;
        };
        let mut files = FilesState::new_tab(&backend, &self.root_scope().files());
        files.generated_report(source, base, html);
        self.insert_workspace_tab(
            WorkspaceTabKind::Document,
            WorkspaceContent::Files(RefCell::new(files)),
        );
    }

    pub(crate) fn open_document_tab(
        &self,
        source: FilesSourceKind,
        path: String,
        title: String,
    ) -> Option<WorkspaceTabId> {
        // Reopening an already edited document focuses its buffer instead of making
        // another editor which could overwrite it with an older snapshot.
        let existing = self.workspace_scopes().into_iter().find(|scope| {
            scope.workspace_kind() == Some(WorkspaceTabKind::Document)
                && scope.files().document_matches(source, &path)
        });
        if let Some(scope) = existing {
            let id = scope.workspace_id()?;
            self.select_workspace_tab(id);
            return Some(id);
        }
        let (backend, waker) = {
            let state = self.state.borrow();
            (state.ready.as_ref()?.backend.clone(), state.waker.clone())
        };
        let mut files = FilesState::new_tab(&backend, &self.root_scope().files());
        files.begin_document_load(source, path, title, waker);
        Some(self.insert_workspace_tab(
            WorkspaceTabKind::Document,
            WorkspaceContent::Files(RefCell::new(files)),
        ))
    }

    pub fn close_workspace_tab(&self, id: WorkspaceTabId) -> bool {
        if self.borrow().updates.view.busy {
            return false;
        }
        let Some(scope) = self.workspace_scope(id) else {
            return false;
        };
        if scope.workspace_kind() == Some(WorkspaceTabKind::Document) {
            scope.files_mut().request_close_document();
            if scope.files().has_document() {
                self.select_workspace_tab(id);
                return false;
            }
            if self.workspace_scope(id).is_none() {
                return true;
            }
        }
        self.remove_workspace_tab(id)
    }

    pub(crate) fn remove_workspace_tab(&self, id: WorkspaceTabId) -> bool {
        let Some(scope) = self.workspace_scope(id) else {
            return false;
        };
        if scope.workspace_kind() == Some(WorkspaceTabKind::Chat) {
            scope.chat_mut().close_media_call();
        }
        let next = {
            let mut state = self.state.borrow_mut();
            let Some(index) = state.workspace_tabs.iter().position(|tab| tab.id == id) else {
                return false;
            };
            let removed = state.workspace_tabs.remove(index);
            if state.last_chat_tab == Some(id) {
                state.last_chat_tab = None;
            }
            if state.active_workspace_tab == Some(id) {
                state.active_workspace_tab = None;
                state.active_tab = if removed.kind == WorkspaceTabKind::Chat {
                    Tab::Sinomo
                } else {
                    Tab::Files
                };
                state
                    .workspace_tabs
                    .get(index)
                    .or_else(|| state.workspace_tabs.last())
                    .map(|tab| tab.id)
            } else {
                None
            }
        };
        if let Some(next) = next {
            self.select_workspace_tab(next);
        } else {
            self.sync_chat_tab_visibility();
        }
        self.publish_shell_signals();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_tabs_keep_drafts_and_worker_identities_separate() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared.chat_mut().input = "pinned draft".into();
        let a = shared.new_workspace_tab(WorkspaceTabKind::Chat).unwrap();
        let first = shared.workspace_scope(a).unwrap();
        first.chat_mut().input = "first draft".into();
        let b = shared.new_workspace_tab(WorkspaceTabKind::Chat).unwrap();
        let second = shared.workspace_scope(b).unwrap();
        second.chat_mut().input = "second draft".into();
        assert_ne!(
            first.chat().handle.connection_snapshot().execution_id,
            second.chat().handle.connection_snapshot().execution_id
        );
        shared.select_workspace_tab(a);
        // A retained background callback is scoped even after selection changes.
        second.chat_mut().input.push_str(" updated");
        assert_eq!(first.chat().input, "first draft");
        assert_eq!(second.chat().input, "second draft updated");
        assert_eq!(shared.chat().input, "pinned draft");
        assert!(shared.close_workspace_tab(b));
        assert_eq!(shared.borrow().active_workspace_tab, Some(a));
        assert!(shared.close_workspace_tab(a));
        assert_eq!(shared.borrow().active_workspace_tab, None);
        assert_eq!(shared.active_tab(), Tab::Sinomo);
    }

    #[test]
    fn workspace_tabs_restore_all_drafts_and_selected_tab() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let a = shared.new_workspace_tab(WorkspaceTabKind::Chat).unwrap();
        shared.workspace_scope(a).unwrap().chat_mut().input = "restored first".into();
        let b = shared.new_workspace_tab(WorkspaceTabKind::Chat).unwrap();
        shared.workspace_scope(b).unwrap().chat_mut().input = "restored second".into();
        let snapshot = crate::restoration::WorkspaceSnapshot {
            schema_version: 1,
            active_tab: Tab::Sinomo,
            chat: shared.chat().workspace_snapshot(),
            files: None,
            tabs: shared.workspace_snapshots(),
            active_open_tab: Some(0),
            current_open_chat: Some(0),
            window: Default::default(),
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let restored = Shared::new(AppState::ready(Backend::local_fallback()));
        restored.restore_workspace_tabs(serde_json::from_slice(&bytes).unwrap());
        let scopes = restored.workspace_scopes();
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0].chat().input, "restored first");
        assert_eq!(scopes[1].chat().input, "restored second");
        assert_eq!(
            restored.active_scope().workspace_id(),
            scopes[0].workspace_id()
        );
    }

    #[test]
    fn workspace_tabs_cross_surface_actions_publish_to_the_pinned_destination() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let chat_id = shared.new_workspace_tab(WorkspaceTabKind::Chat).unwrap();
        let chat = shared.workspace_scope(chat_id).unwrap();
        let files_before = shared.signals().files_structure.get();
        chat.files_mut().path = "/linked-workspace".into();
        assert_ne!(files_before, shared.signals().files_structure.get());
        assert_eq!(
            chat.signals().files_structure.get(),
            shared.signals().files_structure.get()
        );
        let files_id = shared.new_workspace_tab(WorkspaceTabKind::Files).unwrap();
        let files = shared.workspace_scope(files_id).unwrap();
        assert_eq!(files.current_chat_scope().workspace_id(), Some(chat_id));
        files.chat_mut().input = "pinned chat action".into();
        assert_eq!(
            files.signals().chat_structure.get(),
            shared.signals().chat_structure.get()
        );
        assert_eq!(shared.chat().input, "pinned chat action");
        assert!(chat.chat().input.is_empty());
    }

    #[test]
    fn workspace_tabs_stay_fixed_during_restart_checkpoint() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let id = shared.new_workspace_tab(WorkspaceTabKind::Chat).unwrap();
        shared.borrow_mut().updates.view.busy = true;
        assert!(shared.new_workspace_tab(WorkspaceTabKind::Files).is_none());
        assert!(!shared.close_workspace_tab(id));
        assert_eq!(shared.workspace_scopes().len(), 1);
        shared.borrow_mut().updates.view.busy = false;
        assert!(shared.close_workspace_tab(id));
    }
}
