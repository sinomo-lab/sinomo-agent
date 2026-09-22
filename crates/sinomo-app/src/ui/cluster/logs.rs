//! Node-attributed log tails. Refreshes retain controls, list state and selection.
use super::*;
use crate::backend::observe::ObserveLogBatch;
use sinomo_runtime::observe::{
    ObserveLogSource, ObserveLogsRequest, OperationalLogRecord, OperationalSeverity,
};
use std::hash::{Hash, Hasher};
use sui::{VirtualList, VirtualListChrome};

const MAX_ROWS: usize = 1_000;
const MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct LogRow {
    id: String,
    node: String,
    role: Option<String>,
    record: Arc<OperationalLogRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct LogKey {
    node: Option<String>,
    query: ObserveLogsRequest,
    live: bool,
    loading: bool,
    rows: Arc<[LogRow]>,
    status: String,
    issues: Arc<[String]>,
    selected: Option<String>,
}

impl Default for LogKey {
    fn default() -> Self {
        Self {
            node: None,
            query: ObserveLogsRequest {
                limit: 200,
                ..Default::default()
            },
            live: true,
            loading: false,
            rows: Arc::from([]),
            status: "Refresh to collect recent logs".into(),
            issues: Arc::from([]),
            selected: None,
        }
    }
}

#[derive(Default)]
pub(super) struct LogState {
    pub key: LogKey,
    generation: u64,
    rx: Option<Receiver<ObserveLogBatch>>,
    requested: bool,
}

impl LogState {
    fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.key.rows = Arc::from([]);
        self.key.issues = Arc::from([]);
        self.key.selected = None;
        self.key.status = "Waiting for selected logs".into();
        self.requested = true;
    }

    fn apply(&mut self, batch: ObserveLogBatch) -> bool {
        if batch.generation != self.generation {
            return false;
        }
        let (mut rows, mut issues, mut complete) = (Vec::new(), Vec::new(), 0);
        let total = batch.targets.len();
        if batch.omitted_nodes > 0 {
            issues.push(format!(
                "{} nodes outside this collection limit; select a node to inspect it",
                batch.omitted_nodes
            ));
        }
        for target in batch.targets {
            let node = target.node_id;
            let response = match target.response {
                Ok(response) => response,
                Err(error) => {
                    issues.push(format!("{node}: {}", short_text(&error, 240)));
                    continue;
                }
            };
            complete += 1;
            if !response.runtime_available {
                issues.push(format!(
                    "{node}: runtime collection unavailable; operational logs only"
                ));
            }
            if response.truncated || response.runtime_evicted > 0 {
                issues.push(format!(
                    "{node}: recent tail only · {} runtime records evicted",
                    response.runtime_evicted
                ));
            }
            for error in response.errors {
                issues.push(format!("{node}: {}", short_text(&error, 240)));
            }
            let mut occurrences = BTreeMap::<u64, usize>::new();
            // Reapply the query for older peers that ignore newer filter fields.
            for record in response
                .records
                .into_iter()
                .filter(|record| self.key.query.matches(record))
            {
                let mut hash = std::collections::hash_map::DefaultHasher::new();
                serde_json::to_string(&record)
                    .unwrap_or_default()
                    .hash(&mut hash);
                if record.source == ObserveLogSource::Runtime {
                    response.process_started_at.hash(&mut hash);
                }
                let digest = hash.finish();
                let occurrence = occurrences.entry(digest).or_default();
                let id = format!("{node}:{digest:016x}:{occurrence}");
                *occurrence += 1;
                rows.push(LogRow {
                    id,
                    node: node.clone(),
                    role: (record.source == ObserveLogSource::Runtime)
                        .then(|| response.process_role.clone())
                        .flatten(),
                    record: Arc::new(record),
                });
            }
        }
        rows.sort_by(|a, b| {
            b.record
                .timestamp
                .cmp(&a.record.timestamp)
                .then_with(|| a.id.cmp(&b.id))
        });
        let count = rows.len();
        let mut bytes = 0;
        rows.truncate(MAX_ROWS);
        rows.retain(|row| {
            bytes += serde_json::to_vec(row.record.as_ref()).map_or(MAX_BYTES, |value| value.len());
            bytes <= MAX_BYTES
        });
        if rows.len() < count {
            issues.push("Display limit reached; select a node or refine the filters".into());
        }
        if !rows
            .iter()
            .any(|row| Some(&row.id) == self.key.selected.as_ref())
        {
            self.key.selected = None;
        }
        self.key.status = format!(
            "{} logs · {complete}/{total} nodes responded · {} UTC · newest first",
            rows.len(),
            format_args!(
                "{:02}:{:02}:{:02}",
                batch.received_at.hour(),
                batch.received_at.minute(),
                batch.received_at.second()
            )
        );
        self.key.rows = rows.into();
        self.key.issues = issues.into();
        true
    }
}

fn short_text(text: &str, max: usize) -> String {
    let mut value = text.chars().take(max).collect::<String>();
    if text.chars().count() > max {
        value.push('…');
    }
    value
}

pub(super) fn visible(shared: &Shared) -> bool {
    shared.is_ready()
        && shared.active_tab() == Tab::Cluster
        && shared.cluster().subview == ClusterSubview::Logs
}

pub(super) fn live(shared: &Shared) -> bool {
    visible(shared) && shared.cluster().logs.key.live
}

pub(super) fn request(shared: &Shared, explicit: bool) -> bool {
    if !visible(shared) {
        return false;
    }
    let (backend, waker) = {
        let app = shared.borrow();
        let Some(ready) = app.ready.as_ref() else {
            return false;
        };
        (ready.backend.clone(), app.waker.clone())
    };
    let mut cluster = shared.cluster_mut();
    if cluster
        .last_topology_refresh
        .is_none_or(|at| at.elapsed().as_secs() >= 30)
    {
        cluster.begin_refresh(backend.clone(), waker.clone());
    }
    let state = &mut cluster.logs;
    if explicit {
        state.requested = true;
    }
    if state.rx.is_some() || (!state.key.live && !state.requested) {
        return false;
    }
    let (generation, node, query) = (
        state.generation,
        state.key.node.clone(),
        state.key.query.clone(),
    );
    let (tx, rx) = mpsc::sync_channel(1);
    match thread::Builder::new()
        .name("sinomo-app-logs".into())
        .spawn(move || {
            let batch = backend.observe_cluster_logs(generation, node, query);
            let _ = tx.send(batch);
            if let Some(waker) = waker {
                waker.wake();
            }
        }) {
        Ok(_) => {
            state.rx = Some(rx);
            state.key.loading = true;
            state.requested = false;
        }
        Err(error) => {
            state.key.issues = vec![format!("Log worker could not start: {error}")].into();
            state.requested = false;
        }
    }
    true
}

pub(super) fn pump(shared: &Shared) -> bool {
    let requested;
    {
        let mut cluster = shared.cluster_mut();
        let state = &mut cluster.logs;
        match state.rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(batch)) => {
                state.apply(batch);
            }
            Some(Err(TryRecvError::Disconnected)) => {
                state.key.issues = vec!["Log worker stopped before returning".into()].into();
            }
            _ => return false,
        }
        state.rx = None;
        state.key.loading = false;
        requested = state.requested;
    }
    if requested {
        request(shared, true);
    }
    true
}

fn node_options(key: &ClusterStructureKey) -> Vec<(Option<String>, String)> {
    let mut options = vec![(None, "All nodes".into())];
    for node in &key.snapshot.nodes {
        let label = if node.display_name.is_empty() {
            node.node_id.clone()
        } else {
            node.display_name.clone()
        };
        options.push((
            Some(node.node_id.clone()),
            format!(
                "{label}{} · {}",
                if Some(&node.node_id) == key.snapshot.local_node_id.as_ref() {
                    " (this node)"
                } else {
                    ""
                },
                short_text(&node.node_id, 8)
            ),
        ));
    }
    if options.len() == 1 {
        options.push((Some("local".into()), "This node".into()));
    }
    if let Some(node) = &key.logs.node
        && !options.iter().any(|(id, _)| id.as_ref() == Some(node))
    {
        options.push((Some(node.clone()), node.clone()));
    }
    options
}

pub(super) fn view(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared.signals().cluster_structure.clone();
    let controls_shared = shared.clone();
    let controls_reader = reader.clone();
    // Sample changes cannot replace focused filter controls.
    let controls = RebuildOnChange::new_observable(
        source.select_named("log scope controls", |key| {
            (
                key.theme,
                node_options(key),
                key.logs.node.clone(),
                key.logs.live,
            )
        }),
        move |(_, nodes, selected, live)| {
            let action = controls_shared.clone();
            let options = nodes.clone();
            let node = Select::new("Log node scope")
                .options(nodes.iter().map(|(_, label)| label.clone()))
                .selected(nodes.iter().position(|(id, _)| id == selected).unwrap_or(0))
                .theme_when(clone_reader(&controls_reader))
                .on_change_with_ctx(move |ctx, index, _| {
                    if let Some((node, _)) = options.get(index) {
                        {
                            let mut cluster = action.cluster_mut();
                            cluster.logs.key.node = node.clone();
                            cluster.logs.invalidate();
                        }
                        request(&action, true);
                        request_metric_explorer_relayout(ctx);
                    }
                });
            let action = controls_shared.clone();
            let refresh = Button::new("Refresh logs")
                .theme_when(clone_reader(&controls_reader))
                .on_press_with_ctx(move |ctx| {
                    request(&action, true);
                    request_metric_explorer_relayout(ctx);
                });
            let action = controls_shared.clone();
            let pause = Button::new(if *live { "Pause logs" } else { "Resume logs" })
                .theme_when(clone_reader(&controls_reader))
                .on_press_with_ctx(move |ctx| {
                    let live = {
                        let mut cluster = action.cluster_mut();
                        let state = &mut cluster.logs;
                        state.key.live = !state.key.live;
                        state.generation = state.generation.wrapping_add(1);
                        state.requested = false;
                        state.key.live
                    };
                    if live {
                        request(&action, true);
                    }
                    request_metric_explorer_relayout(ctx);
                });
            WidgetPod::new(
                Flex::horizontal()
                    .gap(8.0)
                    .with_item(node, FlexItem::fill().min_width(0.0))
                    .with_child(pause)
                    .with_child(refresh),
            )
        },
    );
    let action = shared.clone();
    let search = TextInput::new("Search logs")
        .placeholder("Search messages, components or fields")
        .value(
            shared
                .cluster()
                .logs
                .key
                .query
                .contains
                .clone()
                .unwrap_or_default(),
        )
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, text| {
            {
                let mut cluster = action.cluster_mut();
                cluster.logs.key.query.contains = (!text.is_empty()).then_some(text);
                cluster.logs.invalidate();
            }
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let levels = [
        None,
        Some(OperationalSeverity::Trace),
        Some(OperationalSeverity::Debug),
        Some(OperationalSeverity::Info),
        Some(OperationalSeverity::Warn),
        Some(OperationalSeverity::Error),
    ];
    let action = shared.clone();
    let severity = Select::new("Log minimum level")
        .options(["All levels", "Trace+", "Debug+", "Info+", "Warn+", "Error"])
        .selected(
            levels
                .iter()
                .position(|level| *level == shared.cluster().logs.key.query.min_severity)
                .unwrap_or(0),
        )
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, index, _| {
            {
                let mut cluster = action.cluster_mut();
                cluster.logs.key.query.min_severity = levels[index];
                cluster.logs.invalidate();
            }
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let filters = Flex::horizontal()
        .gap(8.0)
        .with_item(search, FlexItem::fill().min_width(0.0))
        .with_child(severity);
    let status = log_label("", reader)
        .semantic_name("Log collection status")
        .text_from(source.select_named("log status", |key| {
            format!(
                "{}{}",
                if key.logs.loading {
                    "Collecting… · "
                } else {
                    ""
                },
                key.logs.status
            )
        }));
    let issues = log_label("", reader)
        .semantic_name("Log collection coverage")
        .text_from(source.select_named("log coverage", |key| {
            if key.logs.issues.is_empty() {
                "Recent daemon and operational logs. Node clocks may differ.".into()
            } else {
                key.logs.issues.join("\n")
            }
        }));
    let toolbar = Surface::window(
        Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(8.0)
            .with_child(controls)
            .with_child(filters)
            .with_child(status),
    )
    .padding(Insets::all(12.0))
    .fill()
    .theme_when(clone_reader(reader));
    let row_shared = shared.clone();
    let row_reader = reader.clone();
    let list_reader = reader.clone();
    let list_action = shared.clone();
    let list = widgets::VirtualCollectionHost::new(
        "Log collection",
        source.select_named("log rows", |key| {
            widgets::VirtualCollectionSnapshot::new(
                key.logs
                    .rows
                    .iter()
                    .map(|row| {
                        (
                            row.id.clone(),
                            (
                                key.theme,
                                row.clone(),
                                key.logs.selected.as_ref() == Some(&row.id),
                            ),
                        )
                    })
                    .collect(),
                key.logs.selected.clone(),
            )
        }),
        move |model, state| {
            VirtualList::new("Collected logs", model, move |_, data| {
                let (shared, reader) = (row_shared.clone(), row_reader.clone());
                RebuildOnChange::new_observable(data, move |(_, row, selected)| {
                    WidgetPod::new(row_view(&shared, &reader, row, *selected))
                })
            })
            .state(state)
            .estimated_row_height(72.0)
            .overscan_viewports(0.5)
            .cache_capacity(24)
            .padding(Insets::all(12.0))
            .spacing(8.0)
            .chrome(VirtualListChrome::Transparent)
            .theme_when(clone_reader(&list_reader))
            .row_name(|key, _| format!("Log {key}"))
            .on_change_with_ctx(move |id, ctx| {
                list_action.cluster_mut().logs.key.selected = Some(id);
                request_metric_explorer_relayout(ctx);
            })
        },
    );
    let detail_reader = reader.clone();
    let detail = RebuildOnChange::new_observable(
        source.select_named("selected log", |key| {
            (
                key.theme,
                key.logs
                    .rows
                    .iter()
                    .find(|row| Some(&row.id) == key.logs.selected.as_ref())
                    .cloned(),
            )
        }),
        move |(_, row)| {
            let text = row
                .as_ref()
                .map(|row| {
                    format!(
                        "{}\n{}\n{}",
                        metadata(row),
                        row.record.message,
                        serde_json::to_string_pretty(&row.record.fields).unwrap_or_default()
                    )
                })
                .unwrap_or_else(|| {
                    "Select a log to read its full message and structured fields".into()
                });
            WidgetPod::new(
                Surface::window(
                    ScrollView::vertical(
                        log_label(text, &detail_reader)
                            .semantic_name("Log details")
                            .selectable(sui::SelectionScope::new())
                            .copy_to_clipboard(true),
                    )
                    .name("Log detail scroll"),
                )
                .padding(Insets::all(12.0))
                .fill()
                .theme_when(clone_reader(&detail_reader)),
            )
        },
    );
    let coverage = Surface::window(ScrollView::vertical(issues).name("Log coverage scroll"))
        .padding(Insets::all(8.0))
        .fill()
        .theme_when(clone_reader(reader));
    let list = Surface::window(list)
        .fill()
        .theme_when(clone_reader(reader));
    Dock::new(Dock::new(list).bottom(180.0, detail))
        .top(148.0, toolbar)
        .bottom(66.0, coverage)
}

fn metadata(row: &LogRow) -> String {
    format!(
        "{} · {} · {} · {}{} · {}{}",
        row.record
            .timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        row.record.severity.as_str().to_uppercase(),
        row.node,
        match row.record.source {
            ObserveLogSource::Operational => "operational",
            ObserveLogSource::Runtime => "runtime",
        },
        row.role
            .as_ref()
            .map(|role| format!(" ({role})"))
            .unwrap_or_default(),
        row.record.component,
        if row.record.truncated {
            " · record truncated"
        } else {
            ""
        }
    )
}

fn row_summary(shared: &Shared, row: &LogRow) -> String {
    let cluster = shared.cluster();
    let node = cluster
        .snapshot
        .nodes
        .iter()
        .find(|node| node.node_id == row.node)
        .map(|node| node.display_name.as_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(&row.node);
    let at = row.record.timestamp;
    format!(
        "{:02}:{:02}:{:02}.{:03} UTC · {} · {} · {} · {}{}",
        at.hour(),
        at.minute(),
        at.second(),
        at.millisecond(),
        row.record.severity.as_str().to_uppercase(),
        node,
        match row.record.source {
            ObserveLogSource::Operational => "operational",
            ObserveLogSource::Runtime => "runtime",
        },
        row.record.component,
        if row.record.truncated {
            " · truncated"
        } else {
            ""
        }
    )
}

fn row_view(
    shared: &Shared,
    reader: &ThemeReader,
    row: &LogRow,
    selected: bool,
) -> impl Widget + use<> {
    let action = shared.clone();
    let id = row.id.clone();
    let preview = log_label(
        short_text(&row.record.message.replace(['\n', '\r'], " "), 240),
        reader,
    )
    .semantic_name("Log message")
    .single_line();
    let details = Button::new("Details")
        .semantic_name(format!("View log {}", row.id))
        .appearance(if selected {
            ButtonAppearance::Tonal
        } else {
            ButtonAppearance::Ghost
        })
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            action.cluster_mut().logs.key.selected = Some(id.clone());
            request_metric_explorer_relayout(ctx);
        });
    Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(2.0)
        .with_child(
            log_label(row_summary(shared, row), reader)
                .semantic_name("Log source")
                .single_line(),
        )
        .with_child(
            Flex::horizontal()
                .gap(8.0)
                .with_item(preview, FlexItem::fill().min_width(0.0))
                .with_child(details),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::observe::ObserveTargetSample;
    use sinomo_runtime::observe::ObserveLogsResponse;

    #[test]
    fn logs_view_retains_scope_controls_and_opens_details() -> sui::Result<()> {
        let mut app_state = crate::state::AppState::ready(Backend::local_fallback());
        app_state.active_tab = Tab::Cluster;
        let shared = Shared::new(app_state);
        let at = OffsetDateTime::now_utc();
        let record = OperationalLogRecord {
            timestamp: at,
            severity: OperationalSeverity::Warn,
            component: "sinomo.dfs".into(),
            message: "Membership handoff completed".into(),
            operation_id: Some("handoff-7".into()),
            fields: serde_json::json!({"shard":"services"}),
            source: ObserveLogSource::Runtime,
            truncated: false,
        };
        {
            let mut cluster = shared.cluster_mut();
            cluster.subview = ClusterSubview::Logs;
            cluster.logs.apply(ObserveLogBatch {
                generation: 0,
                received_at: at,
                omitted_nodes: 0,
                targets: vec![
                    ObserveTargetSample {
                        node_id: "remote-node".into(),
                        response: Ok(ObserveLogsResponse {
                            records: vec![record],
                            runtime_available: true,
                            ..Default::default()
                        }),
                    },
                    ObserveTargetSample {
                        node_id: "offline-node".into(),
                        response: Err("temporarily unavailable".into()),
                    },
                ],
            });
        }
        let app = sui_testing::TestApp::from_runtime(
            sui::Application::new()
                .window(sui::WindowBuilder::new().root(super::super::view(shared.clone())))
                .build()?,
        )?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                1280.0, 900.0,
            ))))?;
        let filter = window
            .get_by_role(SemanticsRole::ComboBox)
            .with_name("Log node scope");
        filter.focus()?;
        let before = window.snapshot()?;
        let control_id = before
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some("Log node scope"))
            .unwrap()
            .id;
        shared.cluster_mut().logs.key.status = "1 logs · 1/2 nodes responded".into();
        let after = window.snapshot()?;
        assert!(
            matches!(after.accessibility.nodes.iter().find(|node| node.name.as_deref() == Some("Log collection status")).and_then(|node| node.value.as_ref()),
            Some(sui::SemanticsValue::Text(value)) if value.contains("1/2 nodes responded")),
            "native text accessibility needs both the semantic label and text value"
        );
        assert_eq!(
            control_id,
            after
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Log node scope"))
                .unwrap()
                .id
        );
        filter.expect().to_be_focused()?;
        let id = shared.cluster().logs.key.rows[0].id.clone();
        window
            .get_by_role(SemanticsRole::Button)
            .with_name(format!("View log {id}"))
            .click()?;
        assert_eq!(shared.cluster().logs.key.selected.as_ref(), Some(&id));
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Pause logs")
            .click()?;
        assert!(!live(&shared));
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Resume logs")
            .expect()
            .to_be_visible()?;
        if std::env::var_os("SINOMO_LOG_SHOTS").is_some() {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/observe-logs-ui.png");
            window.capture_screenshot()?.write_png(path)?;
        }
        shared.cluster_mut().subview = ClusterSubview::Overview;
        assert!(!visible(&shared));
        assert!(!request(&shared, true));
        Ok(())
    }

    #[test]
    fn changing_scope_discards_inflight_result_and_preserves_partial_nodes() {
        let mut state = LogState::default();
        state.invalidate();
        let mut batch = ObserveLogBatch {
            generation: 0,
            received_at: OffsetDateTime::now_utc(),
            omitted_nodes: 0,
            targets: vec![],
        };
        assert!(!state.apply(batch.clone()));
        batch.generation = state.generation;
        let record = OperationalLogRecord {
            timestamp: batch.received_at,
            severity: OperationalSeverity::Warn,
            component: "dfs".into(),
            message: "handoff complete".into(),
            operation_id: None,
            fields: serde_json::Value::Null,
            source: ObserveLogSource::Runtime,
            truncated: false,
        };
        batch.targets = vec![
            ObserveTargetSample {
                node_id: "node-a".into(),
                response: Ok(ObserveLogsResponse {
                    records: vec![record.clone(), record],
                    runtime_available: true,
                    ..Default::default()
                }),
            },
            ObserveTargetSample {
                node_id: "node-b".into(),
                response: Err("offline".into()),
            },
        ];
        assert!(state.apply(batch));
        assert_eq!(state.key.rows.len(), 2);
        assert_ne!(state.key.rows[0].id, state.key.rows[1].id);
        assert_eq!(state.key.rows[0].node, "node-a");
        assert!(state.key.status.contains("1/2 nodes"));
        assert!(state.key.issues[0].contains("node-b: offline"));
    }
}

fn log_label(text: impl Into<String>, reader: &ThemeReader) -> Label {
    let reader = reader.clone();
    Label::new(text).style_when(move || reader().body_text_style())
}
