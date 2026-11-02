//! Shared target selection and isolation for node-scoped settings.
use super::*;

pub(super) struct NodeSettingsSnapshot {
    pub items: Vec<ConfigItem>,
    pub mount: SifsMountSettingsSnapshot,
}

impl SettingsState {
    pub(super) fn operation_targets_node(&self, operation: &SettingsOperation) -> bool {
        match operation {
            SettingsOperation::SetConfigValue { key, .. }
            | SettingsOperation::SetConfigDocument { key, .. }
            | SettingsOperation::SetConfigBool { key, .. } => self
                .config_items
                .iter()
                .any(|item| item.key == *key && item.node_scoped),
            SettingsOperation::RefreshNodeSettings
            | SettingsOperation::RefreshModules { .. }
            | SettingsOperation::SetNodeModuleOverride { .. }
            | SettingsOperation::RefreshSifsMount { .. }
            | SettingsOperation::SaveSifsMount { .. }
            | SettingsOperation::RefreshRemoteManagement
            | SettingsOperation::SetRemoteManagementPermission { .. }
            | SettingsOperation::SetExternalIntegrationsPermission { .. }
            | SettingsOperation::SetRemoteManagementModuleOverride { .. } => true,
            _ => false,
        }
    }

    pub(super) fn selected_settings_node_is_local(&self) -> bool {
        self.module_selected_node_id
            .as_deref()
            .is_none_or(|selected| {
                self.cluster_node_id.as_deref().unwrap_or(&self.node_id) == selected
                    || self
                        .module_snapshot
                        .nodes
                        .iter()
                        .any(|node| node.node_id == selected && node.local)
            })
    }

    pub(super) fn merge_config_scope(&mut self, items: Vec<ConfigItem>, node_scoped: bool) {
        self.config_items
            .retain(|item| item.node_scoped != node_scoped);
        self.config_items.extend(
            items
                .into_iter()
                .filter(|item| item.node_scoped == node_scoped),
        );
        self.config_items.sort_by(|a, b| a.key.cmp(&b.key));
    }

    pub(super) fn select_settings_node(&mut self, node_id: String) {
        self.node_selection_epoch.fetch_add(1, Ordering::AcqRel);
        let keys = self
            .config_items
            .iter()
            .filter(|item| item.node_scoped)
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        self.pending_operations.retain(|scope, _| match scope {
            SettingsOperationScope::Modules
            | SettingsOperationScope::SifsMount
            | SettingsOperationScope::RemoteManagementRefresh
            | SettingsOperationScope::RemoteManagementPermission
            | SettingsOperationScope::ExternalIntegrationsPermission
            | SettingsOperationScope::RemoteManagementModule => false,
            SettingsOperationScope::ConfigField(field) => !keys.iter().any(|key| {
                field == &config_field_key(key)
                    || field.starts_with(&config_schema_field_prefix(key))
            }),
            _ => true,
        });
        for domain in [
            SettingsOperationDomain::Config,
            SettingsOperationDomain::Modules,
            SettingsOperationDomain::SifsMount,
            SettingsOperationDomain::RemoteManagement,
        ] {
            self.server_generations.bump(domain);
            self.draft_generations.bump(domain);
        }
        self.server_generations.remote_management_permission = self
            .server_generations
            .remote_management_permission
            .wrapping_add(1);
        let cluster_keys = self
            .config_items
            .iter()
            .filter(|item| !item.node_scoped)
            .map(|item| item.key.clone())
            .collect::<Vec<_>>();
        let retain_field = |field: &String| {
            (!field.starts_with("config:") && !field.starts_with("config-schema:"))
                || cluster_keys.iter().any(|key| {
                    field == &config_field_key(key)
                        || field.starts_with(&config_schema_field_prefix(key))
                })
        };
        self.field_drafts.retain(|field, _| retain_field(field));
        self.field_errors.retain(|field, _| retain_field(field));
        self.config_drafts
            .retain(|key, _| cluster_keys.contains(key));
        self.config_items.retain(|item| !item.node_scoped);
        self.config_ui_revision = self.config_ui_revision.wrapping_add(1);
        self.module_selected_node_id = Some(node_id);
        self.module_snapshot.selected_node = None;
        self.module_snapshot.modules.clear();
        self.module_action_error = None;
        self.module_action_status = None;
        self.node_settings_loading = true;
        self.node_settings_error = None;
        self.sifs_mount_snapshot = SifsMountSettingsSnapshot::default();
        self.sifs_mount_form = SifsMountForm::from(&self.sifs_mount_snapshot.config);
        self.sifs_mount_action_error = None;
        self.sifs_mount_action_status = None;
        self.remote_management = RemoteManagementSettingsSnapshot::default();
        self.remote_management_enable_confirming = false;
        self.external_integrations_enable_confirming = false;
    }
}

fn reload_node(shared: &Shared, node_id: String) {
    shared.settings_mut().select_settings_node(node_id);
    queue_settings_operation(
        shared,
        SettingsOperationScope::Modules,
        SettingsOperation::RefreshNodeSettings,
    );
}

pub(super) fn node_settings_page<W: Widget + 'static>(
    shared: &Shared,
    reader: &ThemeReader,
    body: W,
) -> impl Widget + use<W> {
    // The option list has no reader form, so the picker follows the node list; picking a node
    // only moves its selection.
    let nodes = settings_projection(shared, "settings node picker", |snapshot| {
        snapshot.modules.nodes.clone()
    });
    let s = shared.clone();
    let r = Rc::clone(reader);
    let picker = RebuildOnChange::key_from(nodes, move |_| {
        let nodes = s.settings().module_snapshot.nodes.clone();
        let s_change = s.clone();
        let select = Select::new("settings-node-selector")
            .options(nodes.iter().map(module_node_option).collect::<Vec<_>>())
            .selected_when({
                let s = s.clone();
                move || s.settings().selected_module_node_index()
            })
            .theme_when(clone_reader(&r))
            .on_change(move |index, _| {
                if let Some(node) = nodes.get(index) {
                    reload_node(&s_change, node.node_id.clone());
                }
            });
        WidgetPod::new(
            SettingRow::new("Node")
                .help("Settings on this page apply to the node you pick here.")
                .control(select)
                .build(&s),
        )
    });
    let gate = settings_projection(shared, "settings node loading gate", |snapshot| {
        usize::from(snapshot.node_target.1 || snapshot.node_target.2.is_some())
    });
    let message = settings_projection(shared, "settings selected node availability", |snapshot| {
        let (_, loading, error) = &snapshot.node_target;
        if *loading {
            "Loading the selected node…".to_string()
        } else {
            error.clone().unwrap_or_default()
        }
    });
    let refresh = {
        let s = shared.clone();
        sui::Button::new("Refresh node")
            .theme_when(clone_reader(reader))
            .on_press(move || {
                let node = s.settings().module_selected_node_id.clone();
                if let Some(node) = node {
                    reload_node(&s, node);
                }
            })
    };
    let unavailable = SettingsCard::new("Node settings")
        .description_from(message)
        .header_action(refresh)
        .build(shared, reader);
    Stack::vertical()
        .gap(14.0)
        .alignment(Alignment::Stretch)
        .with_child(picker)
        .with_child(
            SwitchView::new()
                .with_child(body)
                .with_child(unavailable)
                .selected_from(gate),
        )
}

pub(super) fn remote_host_access_panel(
    shared: &Shared,
    reader: &ThemeReader,
) -> impl Widget + use<> {
    let s = shared.clone();
    let module_override = Select::new("selected-node-remote-management-module")
        .options(MODULE_OVERRIDE_OPTIONS.iter().copied())
        .selected_when({
            let s = s.clone();
            move || {
                Some(match s.settings().remote_management.module_override {
                    None => 0,
                    Some(true) => 1,
                    Some(false) => 2,
                })
            }
        })
        .theme_when(clone_reader(reader))
        .on_change(move |index, _| {
            queue_settings_operation(
                &s,
                SettingsOperationScope::RemoteManagementModule,
                SettingsOperation::SetRemoteManagementModuleOverride {
                    enabled: match index {
                        1 => Some(true),
                        2 => Some(false),
                        _ => None,
                    },
                },
            );
        });
    let card = SettingsCard::new("Host access on the selected node")
        .description("Host-access consent can only be changed on that node itself. Ordinary Sinomo settings do not need it.")
        .row(
            SettingRow::new("Allow host access there")
                .help("Run this in a trusted shell on the selected node.")
                .control(copy_control(
                    reader,
                    "selected-node-host-consent-command",
                    "Run on the selected node",
                    REMOTE_MANAGEMENT_ALLOW_COMMAND.to_string(),
                ))
                .build(shared),
        )
        .row(
            SettingRow::new("Allow integrations there")
                .control(copy_control(
                    reader,
                    "selected-node-integrations-consent-command",
                    "Allow integrations on the selected node",
                    EXTERNAL_INTEGRATIONS_ALLOW_COMMAND.to_string(),
                ))
                .build(shared),
        )
        .row(
            SettingRow::new("Module override")
                .help_from(settings_projection(
                    shared,
                    "settings remote node module detail",
                    |snapshot| {
                        snapshot
                            .remote_management
                            .snapshot
                            .module_observed_detail
                            .clone()
                    },
                ))
                .control(module_override)
                .build(shared),
        );
    let feedback = settings_projection(shared, "settings remote node module status", |snapshot| {
        let remote = &snapshot.remote_management;
        remote
            .module_error
            .clone()
            .or(remote.module_status.clone())
            .or(remote.snapshot.module_error.clone())
            .unwrap_or_default()
    });
    let has_feedback = derived(
        feedback.clone(),
        "settings remote node has status",
        |feedback: &String| !feedback.is_empty(),
    );
    let card = card.row(revealed(
        SettingRow::new("Status")
            .value_from(shared, feedback)
            .build(shared),
        has_feedback,
    ));
    card.build(shared, reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changing_nodes_discards_stale_results_and_queued_node_writes() {
        let backend = Backend::local_fallback();
        let mut settings = SettingsState::new(&backend);
        let cluster_key = settings
            .config_items
            .iter()
            .find(|item| !item.node_scoped)
            .unwrap()
            .key
            .clone();
        settings
            .field_drafts
            .insert(config_field_key(&cluster_key), "cluster draft".into());
        settings
            .config_drafts
            .insert("old-node-definition".into(), Value::Bool(true));
        let epoch = settings.node_selection_epoch.load(Ordering::Acquire);
        let request = SettingsOperationRequest {
            request_id: 10,
            scope: SettingsOperationScope::SifsMount,
            draft_generation: 0,
            operation_epoch: settings.operation_epoch.load(Ordering::Acquire),
            operation_epoch_source: Arc::clone(&settings.operation_epoch),
            node_epoch: Some((Arc::clone(&settings.node_selection_epoch), epoch)),
            backend,
            operation: SettingsOperation::SaveSifsMount {
                config: SifsMountConfig::default(),
            },
        };
        settings
            .pending_operations
            .insert(SettingsOperationScope::SifsMount, 10);
        assert!(settings_operation_request_is_current(&request));
        settings.select_settings_node("another-node".into());
        assert_eq!(
            settings
                .field_drafts
                .get(&config_field_key(&cluster_key))
                .map(String::as_str),
            Some("cluster draft")
        );
        assert!(!settings.config_drafts.contains_key("old-node-definition"));
        assert!(!settings_operation_request_is_current(&request));
        let (tx, _) = mpsc::channel();
        let (results, rx) = mpsc::channel();
        settings.operation_lane = Some(SettingsOperationLane { tx, rx });
        results
            .send(SettingsOperationResult {
                node_epoch: Some(epoch),
                request_id: 10,
                scope: SettingsOperationScope::SifsMount,
                draft_generation: 0,
                payload: SettingsOperationPayload::SifsMount(Err("old node error".into())),
            })
            .unwrap();
        settings.pump_operations();
        assert_eq!(
            settings.module_selected_node_id.as_deref(),
            Some("another-node")
        );
        assert!(settings.sifs_mount_action_error.is_none());
        assert!(settings.node_settings_loading);
    }

    #[test]
    fn cluster_refresh_preserves_selected_node_configuration() {
        let backend = Backend::local_fallback();
        let mut settings = SettingsState::new(&backend);
        let node_item = settings
            .config_items
            .iter_mut()
            .find(|item| item.node_scoped)
            .unwrap();
        node_item.value = "remote node value".into();
        let key = node_item.key.clone();
        settings.merge_config_scope(resolve_items(backend.config.as_ref()), false);
        assert_eq!(
            settings
                .config_items
                .iter()
                .find(|item| item.key == key)
                .unwrap()
                .value,
            "remote node value"
        );
    }

    #[test]
    fn remote_selection_cannot_change_the_local_host_consent_document() {
        let mut backend = Backend::local_fallback();
        backend.settings_node_id = Some("remote-node".into());
        assert!(
            backend
                .set_local_remote_management_permission(true)
                .is_err()
        );
        assert!(
            backend
                .set_local_external_integrations_permission(true)
                .is_err()
        );
        let permissions =
            sinomo_runtime::node::LocalNodePermissionStore::new(backend.data_dir.root())
                .load()
                .unwrap();
        assert!(!permissions.remote_management_enabled());
        assert!(!permissions.external_integrations_enabled());
    }

    #[test]
    fn every_node_page_has_the_shared_node_selector() -> sui::Result<()> {
        use crate::state::AppState;
        for section in [
            SettingsSection::Storage,
            SettingsSection::RemoteManagement,
            SettingsSection::Modules,
        ] {
            let shared = Shared::new(AppState::ready(Backend::local_fallback()));
            shared.settings_mut().open(section);
            let runtime = sui::Application::new()
                .window(
                    sui::WindowBuilder::new()
                        .title("Node settings selection")
                        .root(view(shared)),
                )
                .build()?;
            let app = sui_testing::TestApp::from_runtime(runtime)?;
            assert_eq!(
                app.main_window()?
                    .get_by_role(SemanticsRole::ComboBox)
                    .with_name("settings-node-selector")
                    .count()?,
                1,
                "{section:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn cluster_agent_settings_link_to_node_host_consent() -> sui::Result<()> {
        use crate::state::AppState;
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared.settings_mut().create_agent();
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Agent configuration scope")
                    .root(ScrollView::vertical(agent_external_integrations_sections(
                        &shared, &reader,
                    ))),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Allow external integrations…")
                .count()?,
            0
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Host access settings")
            .click()?;
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::RemoteManagement)
        );
        Ok(())
    }
}
