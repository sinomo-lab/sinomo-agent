//! Node-attributed log stream. Full entries stay visible without opening a detail view.
use super::*;
use crate::backend::observe::ObserveLogBatch;
use sinomo_runtime::observe::{
    ObserveLogSource, ObserveLogsRequest, OperationalLogRecord, OperationalSeverity,
};
use std::hash::{Hash, Hasher};
use sui::{VirtualList, VirtualListChrome, VirtualListSelectionMode};

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
}

impl LogKey {
    pub(super) fn is_live(&self) -> bool {
        self.live
    }
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
    // The controls stay mounted: the node select follows the log scope and is rebuilt only when
    // the cluster's node list changes, and the pause button follows the live state.
    let node_shared = shared.clone();
    let node_reader = reader.clone();
    let node = RebuildOnChange::key_from(
        source.select_named("log node options", node_options),
        move |nodes| {
            let selected_shared = node_shared.clone();
            let selected_nodes = nodes.clone();
            let action = node_shared.clone();
            let options = nodes.clone();
            WidgetPod::new(
                Select::new("Log node scope")
                    .options(nodes.iter().map(|(_, label)| label.clone()))
                    .selected_when(move || {
                        let selected = selected_shared.cluster().logs.key.node.clone();
                        Some(
                            selected_nodes
                                .iter()
                                .position(|(id, _)| *id == selected)
                                .unwrap_or(0),
                        )
                    })
                    .theme_when(clone_reader(&node_reader))
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
                    }),
            )
        },
    );
    let action = shared.clone();
    let refresh = Button::new("Refresh logs")
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let action = shared.clone();
    let pause = widgets::Restyle::new(
        Button::new("Pause logs")
            .theme_when(clone_reader(reader))
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
            }),
        source.select_named("log live mode", |key| key.logs.live),
        |mut button, live| {
            button.set_label(if *live { "Pause logs" } else { "Resume logs" });
            button
        },
    );
    let controls = Flex::horizontal()
        .gap(8.0)
        .with_item(node, FlexItem::fill().min_width(0.0))
        .with_child(pause)
        .with_child(refresh);
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
    let row_reader = reader.clone();
    let list_reader = reader.clone();
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
                                row.clone(),
                                key.snapshot
                                    .nodes
                                    .iter()
                                    .find(|node| node.node_id == row.node)
                                    .map(|node| node.display_name.clone())
                                    .unwrap_or_default(),
                            ),
                        )
                    })
                    .collect(),
                None,
            )
        }),
        move |model, state| {
            VirtualList::new("Collected logs", model, move |key, data| {
                row_view(&row_reader, key, data)
            })
            .state(state)
            .selection_mode(VirtualListSelectionMode::None)
            .estimated_row_height(120.0)
            .overscan_viewports(0.5)
            .cache_capacity(24)
            .padding(Insets::all(12.0))
            .gap(8.0)
            .chrome(VirtualListChrome::Transparent)
            .theme_when(clone_reader(&list_reader))
            .row_name(|key, _| format!("Log {key}"))
        },
    );
    let coverage = Surface::window(ScrollView::vertical(issues).name("Log coverage scroll"))
        .padding(Insets::all(8.0))
        .fill()
        .theme_when(clone_reader(reader));
    let list = Surface::window(list)
        .fill()
        .theme_when(clone_reader(reader));
    Dock::new(list).top(148.0, toolbar).bottom(66.0, coverage)
}

fn entry_text(row: &LogRow, node_name: &str) -> String {
    let node = if node_name.is_empty() || node_name == row.node {
        row.node.clone()
    } else {
        format!("{node_name} ({})", row.node)
    };
    let mut text = format!(
        "{} \u{00b7} {} \u{00b7} {} \u{00b7} {}{} \u{00b7} {}{}{}\n{}",
        row.record
            .timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
        row.record.severity.as_str().to_uppercase(),
        node,
        match row.record.source {
            ObserveLogSource::Operational => "operational",
            ObserveLogSource::Runtime => "runtime",
        },
        row.role
            .as_ref()
            .map(|role| format!(" ({role})"))
            .unwrap_or_default(),
        row.record.component,
        row.record
            .operation_id
            .as_ref()
            .map(|id| format!(" \u{00b7} operation {id}"))
            .unwrap_or_default(),
        if row.record.truncated {
            " \u{00b7} record truncated at collection"
        } else {
            ""
        },
        row.record.message,
    );
    if !row.record.fields.is_null() && row.record.fields != serde_json::json!({}) {
        text.push('\n');
        text.push_str(&serde_json::to_string_pretty(&row.record.fields).unwrap_or_default());
    }
    text
}

/// One log entry, built once per entry id. Its text follows the row data (for example a node's
/// display name arriving later), so a text selection inside it survives updates.
fn row_view(reader: &ThemeReader, id: &str, data: Signal<(LogRow, String)>) -> Label {
    let (row, node_name) = data.get();
    log_label(entry_text(&row, &node_name), reader)
        .text_from(data.select_named("log entry text", |(row, node_name)| {
            entry_text(row, node_name)
        }))
        .semantic_name(format!("Log entry {id}"))
        .selectable(sui::SelectionScope::new())
        .copy_to_clipboard(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::observe::ObserveTargetSample;
    use sinomo_runtime::observe::ObserveLogsResponse;

    #[test]
    fn logs_view_shows_full_entries_without_drilldown_and_retains_controls() -> sui::Result<()> {
        let mut app_state = crate::state::AppState::ready(Backend::local_fallback());
        app_state.active_tab = Tab::Cluster;
        let shared = Shared::new(app_state);
        let at = OffsetDateTime::now_utc();
        let record = OperationalLogRecord {
            timestamp: at,
            severity: OperationalSeverity::Warn,
            component: "sinomo.dfs".into(),
            message: format!(
                "Membership handoff completed\n{}MESSAGE_END",
                "full message ".repeat(30)
            ),
            operation_id: Some("handoff-7".into()),
            fields: serde_json::json!({"shard":"services", "nested":{"detail":"FIELD_END"}}),
            source: ObserveLogSource::Runtime,
            truncated: false,
        };
        let next_record = OperationalLogRecord {
            timestamp: at - time::Duration::seconds(1),
            message: "Next complete log entry".into(),
            fields: serde_json::Value::Null,
            ..record.clone()
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
                            records: vec![record, next_record],
                            runtime_available: true,
                            process_role: Some("daemon".into()),
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
        // TestApp focuses a Select by clicking it, which opens its popup.
        filter.press("Escape")?;
        let entry = after
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("Log entry "))
            })
            .expect("full log entry visible immediately");
        let Some(sui::SemanticsValue::Text(text)) = &entry.value else {
            panic!("entry must expose selectable text")
        };
        for expected in [
            "remote-node",
            "runtime (daemon)",
            "operation handoff-7",
            "MESSAGE_END",
            "FIELD_END",
            "services",
        ] {
            assert!(text.contains(expected), "full entry is missing {expected}");
        }
        assert!(
            text.contains("completed\n"),
            "message line breaks must survive"
        );
        let next_entry = after
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                matches!(node.value.as_ref(),
            Some(sui::SemanticsValue::Text(value)) if value.contains("Next complete log entry"))
            })
            .expect("next stream entry");
        assert!(
            entry.bounds.height() > 120.0,
            "full content must grow beyond the estimated row height"
        );
        assert!(
            entry.bounds.max_y() <= next_entry.bounds.y() + 0.5,
            "expanded rows must not overlap"
        );
        assert!(!after.accessibility.nodes.iter().any(|node| {
            node.name
                .as_deref()
                .is_some_and(|name| name.starts_with("View log ") || name == "Log detail scroll")
        }));
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
                .join("../../target/observe-stream-ui.png");
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
    Label::new(text).text_style_when(move || reader().body_text_style())
}
