use super::*;
use sinomo_client::usage_contract::{UsageEvent, UsageQuery, UsageQueryResponse, UsageRecord};
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct UsageKey {
    pub query: UsageQuery,
    pub section: usize,
    pub range: usize,
    pub loading: bool,
    pub response: Option<UsageQueryResponse>,
    pub error: Option<String>,
    pub ledger_sizes: BTreeMap<String, u64>,
    pub ledger_error: Option<String>,
    pub evidence: Option<String>,
    pub evidence_loading: bool,
}
#[derive(Default)]
pub(super) struct UsageState {
    pub key: UsageKey,
    rx: Option<Receiver<(UsageQuery, Result<UsageQueryResponse, String>)>>,
    last: Option<std::time::Instant>,
    sizes_rx: Option<Receiver<Result<UsageQueryResponse, String>>>,
    evidence_rx: Option<Receiver<Result<String, String>>>,
    cursors: Vec<Option<sinomo_client::usage_contract::UsageCursor>>,
}
pub(super) fn visible(shared: &Shared) -> bool {
    shared.is_ready()
        && shared.active_tab() == Tab::Cluster
        && shared.cluster().subview == ClusterSubview::Usage
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
        .is_none_or(|last| last.elapsed() >= std::time::Duration::from_secs(30))
    {
        cluster.begin_refresh(backend.clone(), waker.clone());
    }
    let state = &mut cluster.usage;
    if state.key.query.cursor.is_none() {
        state.cursors.clear();
    }
    state.key.query.group_by = match state.key.section {
        1 => sinomo_client::usage_contract::UsageGrouping::Provider,
        2 => sinomo_client::usage_contract::UsageGrouping::Tool,
        3 => sinomo_client::usage_contract::UsageGrouping::Skill,
        4 => sinomo_client::usage_contract::UsageGrouping::Node,
        _ => sinomo_client::usage_contract::UsageGrouping::All,
    };
    if state.rx.is_some()
        || (!explicit
            && state
                .last
                .is_some_and(|last| last.elapsed() < std::time::Duration::from_secs(30)))
    {
        return false;
    }
    if state.key.query.cursor.is_none() && (1..=4).contains(&state.key.range) {
        let now = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).max(0) as u64;
        state.key.query.start_ms = now.saturating_sub(
            [0, 3_600_000, 86_400_000, 604_800_000, 2_592_000_000][state.key.range],
        );
        state.key.query.end_ms = u64::MAX;
    }
    let query = state.key.query.clone();
    let (tx, rx) = mpsc::sync_channel(1);
    match thread::Builder::new()
        .name("sinomo-app-usage".into())
        .spawn(move || {
            let result = backend.usage_query(&query);
            let _ = tx.send((query, result));
            if let Some(waker) = waker {
                waker.wake();
            }
        }) {
        Ok(_) => {
            state.rx = Some(rx);
            state.key.loading = true;
            state.last = Some(std::time::Instant::now());
        }
        Err(error) => state.key.error = Some(error.to_string()),
    }
    true
}
pub(super) fn pump(shared: &Shared) -> bool {
    let mut auxiliary_changed = false;
    {
        let mut c = shared.cluster_mut();
        let state = &mut c.usage;
        if let Some(result) = state.sizes_rx.as_ref().and_then(|rx| rx.try_recv().ok()) {
            state.sizes_rx = None;
            auxiliary_changed = true;
            match result {
                Ok(response) => {
                    state.key.ledger_sizes = response.ledger_sizes;
                    state.key.ledger_error = (!response.failed.is_empty()
                        || !response.omitted.is_empty())
                    .then(|| "Some nodes did not return storage sizes".into());
                }
                Err(error) => state.key.ledger_error = Some(error),
            }
        }
        if let Some(result) = state.evidence_rx.as_ref().and_then(|rx| rx.try_recv().ok()) {
            state.evidence_rx = None;
            state.key.evidence_loading = false;
            state.key.evidence =
                Some(result.unwrap_or_else(|error| format!("Evidence unavailable: {error}")));
            auxiliary_changed = true;
        }
    }
    let retry;
    {
        let mut cluster = shared.cluster_mut();
        let state = &mut cluster.usage;
        let (query, result) = match state.rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(result)) => result,
            Some(Err(TryRecvError::Disconnected)) => (
                state.key.query.clone(),
                Err("usage worker stopped before returning".into()),
            ),
            _ => return auxiliary_changed,
        };
        retry = query != state.key.query;
        if !retry {
            match result {
                Ok(response) => {
                    state.key.ledger_sizes = response.ledger_sizes.clone();
                    state.key.response = Some(response);
                    state.key.error = None;
                }
                Err(error) => state.key.error = Some(error),
            }
        }
        state.rx = None;
        state.key.loading = false;
    }
    if retry {
        request(shared, true);
    }
    true
}

fn label(text: impl Into<String>, reader: &ThemeReader) -> Label {
    let reader = reader.clone();
    Label::new(text).text_style_when(move || reader().body_text_style())
}
pub(super) fn view(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared.signals().cluster_structure.clone();
    let action = shared.clone();
    let refresh = Button::new("Refresh usage")
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let action = shared.clone();
    let sections = Select::new("Usage section")
        .options(["Summary", "Providers", "Tools", "Skills", "Records"])
        .selected_when({
            let selected = shared.clone();
            move || Some(selected.cluster().usage.key.section)
        })
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, index, _| {
            action.cluster_mut().usage.key.section = index;
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let action = shared.clone();
    let ranges = Select::new("Usage time range")
        .options([
            "All time",
            "Last hour",
            "Last day",
            "Last week",
            "Last month",
            "Custom",
        ])
        .selected_when({
            let selected = shared.clone();
            move || Some(selected.cluster().usage.key.range)
        })
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, index, _| {
            let now = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).max(0) as u64;
            let duration = [
                u64::MAX,
                3_600_000,
                86_400_000,
                604_800_000,
                2_592_000_000,
                0,
            ][index];
            {
                let mut cluster = action.cluster_mut();
                cluster.usage.key.range = index;
                if index != 5 {
                    cluster.usage.key.query.start_ms = now.saturating_sub(duration);
                    cluster.usage.key.query.end_ms = u64::MAX;
                }
                cluster.usage.key.query.cursor = None;
            }
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let provider = filter(shared, reader, "Provider", 6);
    let controls = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .with_child(sections)
        .with_child(ranges)
        .with_child(provider)
        .with_child(refresh);
    let filters = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .with_item(
            filter(shared, reader, "Agent", 0),
            FlexItem::fill().min_width(140.0),
        )
        .with_item(
            filter(shared, reader, "Chat", 1),
            FlexItem::fill().min_width(140.0),
        )
        .with_item(
            filter(shared, reader, "Account", 2),
            FlexItem::fill().min_width(140.0),
        )
        .with_item(
            filter(shared, reader, "Model", 3),
            FlexItem::fill().min_width(140.0),
        )
        .with_item(
            filter(shared, reader, "Operation", 4),
            FlexItem::fill().min_width(140.0),
        )
        .with_item(
            filter(shared, reader, "Node", 5),
            FlexItem::fill().min_width(140.0),
        );
    let dates = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .with_item(
            date_filter(shared, reader, true),
            FlexItem::fill().min_width(230.0),
        )
        .with_item(
            date_filter(shared, reader, false),
            FlexItem::fill().min_width(230.0),
        );
    let action = shared.clone();
    let clear = Button::new("Clear filters")
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            {
                let mut c = action.cluster_mut();
                c.usage.key.query.filter = Default::default();
                c.usage.key.query.nodes.clear();
                c.usage.key.query.cursor = None;
            }
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    let active = label("", reader).text_from(source.select_named("usage active filters", |key| {
        let f = &key.usage.query.filter;
        [
            ("Event", &f.event_kind),
            ("Tool", &f.tool),
            ("Skill", &f.skill),
            ("Purpose", &f.purpose),
        ]
        .into_iter()
        .filter_map(|(label, value)| value.as_ref().map(|v| format!("{label}: {v}")))
        .collect::<Vec<_>>()
        .join(" · ")
    }));
    let status = label("", reader).text_from(source.select_named("usage coverage", |key| {
        if let Some(error) = &key.usage.error {
            return format!("Usage unavailable: {error}");
        }
        key.usage
            .response
            .as_ref()
            .map(|r| {
                format!(
                    "{} · {} nodes answered · {} failed · {} omitted{}",
                    if r.partial() {
                        "Partial results"
                    } else {
                        "Complete coverage"
                    },
                    r.answered.len(),
                    r.failed.len(),
                    r.omitted.len(),
                    if key.usage.loading {
                        " · refreshing"
                    } else {
                        ""
                    }
                )
            })
            .unwrap_or_else(|| "Loading usage".into())
    }));
    let toolbar = Flex::vertical()
        .gap(8.0)
        .with_child(controls)
        .with_child(filters)
        .with_child(dates)
        .with_child(
            Flex::horizontal()
                .gap(8.0)
                .with_child(clear)
                .with_child(active),
        )
        .with_child(status);
    let reader2 = reader.clone();
    let content_shared = shared.clone();
    let body = RebuildOnChange::key_from(
        source.select_named("usage response", |key| key.usage.clone()),
        move |key| presentation::content(&content_shared, &reader2, key),
    );
    let row_reader = reader.clone();
    let list_reader = reader.clone();
    let row_shared = shared.clone();
    let records = widgets::VirtualCollectionHost::new(
        "Usage records",
        source.select_named("usage records", |key| {
            widgets::VirtualCollectionSnapshot::new(
                key.usage
                    .response
                    .as_ref()
                    .map(|response| {
                        response
                            .records
                            .iter()
                            .map(|record| (record.id.clone(), record.clone()))
                            .collect()
                    })
                    .unwrap_or_default(),
                None,
            )
        }),
        move |model, state| {
            sui::VirtualList::new(
                "Usage records",
                model,
                move |id, data: Signal<UsageRecord>| {
                    let text = label(record_text(&data.get()), &row_reader)
                        .text_from(data.select_named("usage record", record_text))
                        .semantic_name(format!("Usage {id}"))
                        .selectable(sui::SelectionScope::new())
                        .copy_to_clipboard(true);
                    let record_id = id.to_string();
                    let cost = label("", &row_reader).text_from(
                        row_shared.signals().cluster_structure.select_named(
                            "usage record estimated cost",
                            move |key| {
                                key.usage
                                    .response
                                    .as_ref()
                                    .and_then(|response| response.record_costs.get(&record_id))
                                    .map(cost_text)
                                    .unwrap_or_default()
                            },
                        ),
                    );
                    let action = row_shared.clone();
                    let record = data.get();
                    let button = Button::new("Open activity evidence")
                        .theme_when(clone_reader(&row_reader))
                        .on_press_with_ctx(move |ctx| {
                            request_evidence(&action, record.clone());
                            request_metric_explorer_relayout(ctx);
                        });
                    let action = row_shared.clone();
                    let chat = data.get().attribution.chat.clone();
                    let open = Button::new("Open conversation")
                        .theme_when(clone_reader(&row_reader))
                        .on_press_with_ctx(move |ctx| {
                            if let Some(chat) = &chat {
                                if action.chat_mut().open_usage_chat(chat) {
                                    action.set_active_tab(Tab::Sinomo);
                                } else {
                                    action.cluster_mut().usage.key.error =
                                        Some("Reconnect to open this conversation".into());
                                }
                                request_metric_explorer_relayout(ctx);
                            }
                        });
                    Flex::vertical()
                        .gap(4.0)
                        .with_child(text)
                        .with_child(cost)
                        .with_child(
                            Flex::horizontal()
                                .gap(8.0)
                                .with_child(button)
                                .with_child(open),
                        )
                },
            )
            .state(state)
            .selection_mode(sui::VirtualListSelectionMode::None)
            .estimated_row_height(100.0)
            .cache_capacity(24)
            .padding(Insets::all(12.0))
            .theme_when(clone_reader(&list_reader))
        },
    );
    let action = shared.clone();
    let next = Button::new("Next records")
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            let offset = action
                .cluster()
                .usage
                .key
                .response
                .as_ref()
                .and_then(|r| r.next_cursor.clone());
            if let Some(offset) = offset {
                {
                    let mut c = action.cluster_mut();
                    let previous = c.usage.key.query.cursor.clone();
                    c.usage.cursors.push(previous);
                    c.usage.key.query.cursor = Some(offset);
                }
                request(&action, true);
                request_metric_explorer_relayout(ctx);
            }
        });
    let action = shared.clone();
    let previous = Button::new("Previous records")
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            let previous = action.cluster_mut().usage.cursors.pop();
            if let Some(cursor) = previous {
                action.cluster_mut().usage.key.query.cursor = cursor;
                request(&action, true);
                request_metric_explorer_relayout(ctx);
            }
        });
    let evidence = label("", reader)
        .text_from(source.select_named("usage activity evidence", |key| {
            if key.usage.evidence_loading {
                "Loading activity evidence…".into()
            } else {
                key.usage.evidence.clone().unwrap_or_default()
            }
        }))
        .selectable(sui::SelectionScope::new())
        .copy_to_clipboard(true);
    let content = SwitchView::new()
        .selected_from(source.select_named("usage record section", |key| {
            usize::from(key.usage.section == 4)
        }))
        .with_child(ScrollView::new(body))
        .with_child(
            Dock::new(records).bottom(
                40.0,
                Flex::horizontal()
                    .gap(8.0)
                    .with_child(previous)
                    .with_child(next),
            ),
        );
    let evidence = Presence::new(
        SizedBox::new()
            .height(150.0)
            .child(ScrollView::new(evidence)),
    )
    .shown_from(source.select_named("usage evidence visibility", |key| {
        key.usage.evidence.is_some() || key.usage.evidence_loading
    }))
    .collapse(Axis::Vertical);
    let content = Flex::vertical()
        .with_item(content, FlexItem::fill())
        .with_child(evidence);
    Surface::window(
        Flex::vertical()
            .with_child(
                Surface::panel(toolbar)
                    .padding(Insets::all(10.0))
                    .theme_when(clone_reader(reader)),
            )
            .with_item(content, FlexItem::fill()),
    )
    .fill()
    .theme_when(clone_reader(reader))
}

fn filter(
    shared: &Shared,
    reader: &ThemeReader,
    name: &'static str,
    field: usize,
) -> impl Widget + use<> {
    let action = shared.clone();
    let input = TextInput::new(format!("Usage {name}"))
        .placeholder(name)
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, text| {
            {
                let mut cluster = action.cluster_mut();
                let q = &mut cluster.usage.key.query;
                let value = (!text.trim().is_empty()).then(|| text.trim().to_string());
                match field {
                    0 => q.filter.agent = value,
                    1 => q.filter.chat = value,
                    2 => q.filter.account = value,
                    3 => q.filter.model = value,
                    4 => q.filter.operation = value,
                    6 => q.filter.provider = value,
                    _ => {
                        q.nodes = value
                            .into_iter()
                            .flat_map(|s| {
                                s.split(',')
                                    .map(str::trim)
                                    .filter(|s| !s.is_empty())
                                    .map(str::to_string)
                                    .collect::<Vec<_>>()
                            })
                            .collect()
                    }
                };
                q.cursor = None;
            }
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    widgets::Restyle::new(
        input,
        shared.signals().cluster_structure.select_named(
            format!("usage {name} filter"),
            move |key| {
                let q = &key.usage.query;
                match field {
                    0 => q.filter.agent.clone(),
                    1 => q.filter.chat.clone(),
                    2 => q.filter.account.clone(),
                    3 => q.filter.model.clone(),
                    4 => q.filter.operation.clone(),
                    6 => q.filter.provider.clone(),
                    _ => (!q.nodes.is_empty()).then(|| q.nodes.join(", ")),
                }
                .unwrap_or_default()
            },
        ),
        |mut input, value| {
            input.set_value(value.clone());
            input
        },
    )
}

fn parse_date(text: &str) -> Result<u64, String> {
    let normalized = if text.len() == 10 {
        format!("{text}T00:00:00Z")
    } else {
        text.to_string()
    };
    OffsetDateTime::parse(&normalized, &time::format_description::well_known::Rfc3339)
        .map(|t| (t.unix_timestamp_nanos() / 1_000_000).max(0) as u64)
        .map_err(|_| "Enter YYYY-MM-DD or an RFC3339 date and time".to_string())
}

fn date_filter(shared: &Shared, reader: &ThemeReader, start: bool) -> impl Widget + use<> {
    let action = shared.clone();
    let input = TextInput::new(if start { "Usage start" } else { "Usage end" })
        .placeholder(if start {
            "Start (YYYY-MM-DD UTC)"
        } else {
            "Before (YYYY-MM-DD UTC)"
        })
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |ctx, text| {
            let value = if text.trim().is_empty() {
                Ok(if start { 0 } else { u64::MAX })
            } else {
                parse_date(text.trim())
            };
            match value {
                Ok(value) => {
                    let mut c = action.cluster_mut();
                    c.usage.key.range = 5;
                    if start {
                        c.usage.key.query.start_ms = value;
                    } else {
                        c.usage.key.query.end_ms = value;
                    }
                    c.usage.key.query.cursor = None;
                }
                Err(error) => {
                    action.cluster_mut().usage.key.error = Some(error);
                    return;
                }
            }
            request(&action, true);
            request_metric_explorer_relayout(ctx);
        });
    widgets::Restyle::new(
        input,
        shared.signals().cluster_structure.select_named(
            if start {
                "usage range start"
            } else {
                "usage range end"
            },
            move |key| {
                if start {
                    key.usage.query.start_ms
                } else {
                    key.usage.query.end_ms
                }
            },
        ),
        move |mut input, value| {
            let text = if (*value == 0 && start) || (*value == u64::MAX && !start) {
                String::new()
            } else {
                OffsetDateTime::from_unix_timestamp_nanos(*value as i128 * 1_000_000)
                    .ok()
                    .and_then(|t| {
                        t.format(&time::format_description::well_known::Rfc3339)
                            .ok()
                    })
                    .unwrap_or_default()
            };
            input.set_value(text);
            input
        },
    )
}
fn cost_text(cost: &sinomo_client::usage_contract::UsageCost) -> String {
    let amount = cost
        .amounts
        .iter()
        .map(|(currency, value)| format!("{currency} {value:.4}"))
        .collect::<Vec<_>>()
        .join(", ");
    let total = cost.priced_requests.saturating_add(cost.unpriced_requests);
    let share = if total == 0 {
        "No metered requests".into()
    } else {
        format!(
            "{:.1}% priced",
            100.0 * cost.priced_requests as f64 / total as f64
        )
    };
    let estimate = if amount.is_empty() {
        "No metered estimate".into()
    } else if cost.unpriced_requests > 0 {
        format!("At least {amount}")
    } else {
        amount
    };
    format!(
        "{estimate} · {share} · {} priced · {} unpriced · {} included in plan · {} local",
        cost.priced_requests,
        cost.unpriced_requests,
        cost.subscription_requests,
        cost.local_requests
    )
}
fn record_text(record: &UsageRecord) -> String {
    let text = match &record.event {
        UsageEvent::ModelRequest {
            provider,
            account,
            model,
            operation,
            status,
            error_class,
            duration_ms,
            first_output_ms,
            retry_count,
            reported,
            units,
        } => format!(
            "{provider} / {account} / {model} · {operation} · {status:?}{}\n{} · first output {} · retries {}\n{}",
            error_class
                .as_ref()
                .map(|e| format!(" ({e})"))
                .unwrap_or_default(),
            duration_ms
                .map(|ms| format!("{ms} ms"))
                .unwrap_or_else(|| "duration unreported".into()),
            first_output_ms
                .map(|ms| format!("{ms} ms"))
                .unwrap_or_else(|| "unreported".into()),
            retry_count
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unreported".into()),
            if *reported {
                presentation::units_text(units)
            } else {
                "units not reported".into()
            }
        ),
        UsageEvent::ToolCall {
            tool,
            status,
            duration_ms,
            detached,
            runtime,
            executing_node,
            mcp_server,
        } => format!(
            "{tool} · {status:?} · {duration_ms} ms{}\nRuntime {runtime} · node {}{}",
            if *detached { " · detached" } else { "" },
            executing_node.as_deref().unwrap_or("unreported"),
            mcp_server
                .as_ref()
                .map(|server| format!(" · MCP {server}"))
                .unwrap_or_default()
        ),
        UsageEvent::SkillActivated {
            skill,
            version,
            content_hash,
            trigger,
            resource,
        } => format!(
            "{skill} {version} · {}{}\nContent {content_hash}",
            trigger.replace('_', " "),
            resource
                .as_ref()
                .map(|r| format!(" · {r}"))
                .unwrap_or_default()
        ),
        UsageEvent::SkillVersionPublished {
            skill,
            version,
            previous_version,
            source,
            content_hash,
            ..
        } => format!(
            "{skill} · {} → {version} · {source}\nContent {content_hash}",
            previous_version.as_deref().unwrap_or("new")
        ),
        UsageEvent::QuotaSampled { account, snapshot } => format!(
            "{} / {account} · quota snapshot · {} limits",
            snapshot.provider,
            snapshot.limits.len()
        ),
    };
    let when = OffsetDateTime::from_unix_timestamp((record.timestamp_ms / 1000) as i64)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| "Unknown time".into());
    let a = &record.attribution;
    format!(
        "{when} · {}\n{text}\n{} · {}\nSession {} · operation {}{}",
        a.node,
        a.agent.as_deref().unwrap_or(&a.origin),
        a.purpose.replace('_', " "),
        a.session.as_deref().unwrap_or("not applicable"),
        a.operation.as_deref().unwrap_or("not reported"),
        a.tool_call
            .as_ref()
            .map(|id| format!(" · call {id}"))
            .unwrap_or_default()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use sinomo_client::usage_contract::*;
    #[test]
    fn date_inputs_accept_advertised_date_and_explicit_time() {
        assert_eq!(
            parse_date("2026-11-03").unwrap(),
            parse_date("2026-11-03T00:00:00Z").unwrap()
        );
        assert!(parse_date("2026-11-31").is_err());
        assert!(parse_date("not a date").is_err());
    }
    #[test]
    fn usage_sections_render_empty_partial_and_populated_data() -> sui::Result<()> {
        for section in 0..5 {
            for populated in [false, true] {
                let shared = Shared::new(AppState::ready(Backend::local_fallback()));
                {
                    let mut cluster = shared.cluster_mut();
                    cluster.subview = ClusterSubview::Usage;
                    cluster.usage.key.section = section;
                    let mut response = UsageQueryResponse {
                        answered: vec!["node-one".into()],
                        failed: vec![UsageNodeFailure {
                            node: "offline-node".into(),
                            error: "offline".into(),
                        }],
                        ..Default::default()
                    };
                    if populated {
                        response.totals.model_requests = 2;
                        response.records.push(UsageRecord {
                            id: "record-one".into(),
                            timestamp_ms: 1_000,
                            attribution: UsageAttribution {
                                node: "node-one".into(),
                                ..Default::default()
                            },
                            event: UsageEvent::ToolCall {
                                tool: "fixture-tool".into(),
                                status: UsageStatus::Succeeded,
                                duration_ms: 7,
                                detached: false,
                                runtime: "agent".into(),
                                executing_node: None,
                                mcp_server: None,
                            },
                        });
                    }
                    cluster.usage.key.response = Some(response);
                }
                let reader = cluster_theme_reader(&shared);
                let app = sui_testing::TestApp::from_runtime(
                    sui::Application::new()
                        .window(sui::WindowBuilder::new().root(view(&shared, &reader)))
                        .build()?,
                )?;
                let window = app.main_window()?;
                window
                    .root()
                    .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                        1280.0, 900.0,
                    ))))?;
                app.run_until_idle()?;
                let snapshot = window.snapshot()?;
                assert!(!snapshot.accessibility.nodes.is_empty());
                if section != 4 {
                    assert!(snapshot.accessibility.nodes.iter().any(|node| {
                        node.name
                            .as_deref()
                            .is_some_and(|name| name.contains("Partial results"))
                    }));
                }
            }
        }
        Ok(())
    }
    #[test]
    fn record_text_does_not_render_debug_payloads_or_invent_tokens() {
        let record = UsageRecord {
            id: "r".into(),
            timestamp_ms: 0,
            attribution: Default::default(),
            event: UsageEvent::ModelRequest {
                provider: "fixture".into(),
                account: "default".into(),
                model: "test".into(),
                operation: "completion".into(),
                status: UsageStatus::Failed,
                error_class: Some("provider".into()),
                retry_count: None,
                duration_ms: Some(1),
                first_output_ms: None,
                reported: false,
                units: Default::default(),
            },
        };
        let text = record_text(&record);
        assert!(text.contains("units not reported"));
        assert!(!text.contains("LanguageModelUsage"));
    }
}

fn count(value: Option<u64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unreported".into())
}

mod presentation;

pub(super) fn request_sizes(shared: &Shared) {
    if !shared.is_ready() {
        return;
    }
    let (backend, waker) = {
        let app = shared.borrow();
        (
            app.ready.as_ref().unwrap().backend.clone(),
            app.waker.clone(),
        )
    };
    let mut c = shared.cluster_mut();
    if c.usage.sizes_rx.is_some() {
        return;
    }
    let (tx, rx) = mpsc::channel();
    match thread::Builder::new()
        .name("sinomo-observe-usage-sizes".into())
        .spawn(move || {
            let result = backend.usage_query(&UsageQuery {
                limit: 0,
                ..Default::default()
            });
            let _ = tx.send(result);
            if let Some(w) = waker {
                w.wake();
            }
        }) {
        Ok(_) => c.usage.sizes_rx = Some(rx),
        Err(error) => c.usage.key.ledger_error = Some(error.to_string()),
    }
}
fn request_evidence(shared: &Shared, record: UsageRecord) {
    let (backend, waker) = {
        let app = shared.borrow();
        (
            app.ready.as_ref().unwrap().backend.clone(),
            app.waker.clone(),
        )
    };
    let mut c = shared.cluster_mut();
    let (tx, rx) = mpsc::channel();
    c.usage.key.evidence = Some(record_text(&record));
    c.usage.key.evidence_loading = true;
    match thread::Builder::new()
        .name("sinomo-usage-evidence".into())
        .spawn(move || {
            let result = backend.usage_evidence(&record);
            let _ = tx.send(result);
            if let Some(w) = waker {
                w.wake();
            }
        }) {
        Ok(_) => c.usage.evidence_rx = Some(rx),
        Err(error) => {
            c.usage.key.evidence_loading = false;
            c.usage.key.evidence = Some(error.to_string());
        }
    }
}

#[cfg(test)]
mod presentation_tests {
    use super::*;
    use crate::state::AppState;
    use sinomo_client::usage_contract::*;
    fn sample() -> UsageQueryResponse {
        let mut totals = UsageTotals {
            model_requests: 120,
            reported_requests: 120,
            retry_reporting_requests: 120,
            retries: 3,
            model_errors: 2,
            tool_calls: 45,
            tool_errors: 1,
            skill_activations: 8,
            skill_publications: 2,
            detached_tool_calls: 4,
            last_used_ms: Some(1_793_664_000_000),
            ..Default::default()
        };
        totals.units = LanguageModelUsage {
            input_tokens: Some(1_250_000),
            output_tokens: Some(95_000),
            cache_read_input_tokens: Some(450_000),
            reasoning_tokens: Some(35_000),
            ..Default::default()
        };
        totals.model_latency_ms.insert(800, 110);
        totals.model_latency_ms.insert(2400, 10);
        totals.tool_latency_ms.insert(70, 44);
        totals.tool_latency_ms.insert(900, 1);
        totals.runtimes.insert("sandbox".into(), 45);
        let cost = UsageCost {
            amounts: BTreeMap::from([("USD".into(), 0.52)]),
            priced_requests: 118,
            unpriced_requests: 2,
            price_sources: vec!["Built-in catalog".into()],
            unpriced_models: vec!["local/custom-model".into()],
            ..Default::default()
        };
        let groups = vec![
            UsageGroup {
                dimensions: UsageFilter {
                    provider: Some("openai".into()),
                    account: Some("work".into()),
                    model: Some("gpt-4.1-mini".into()),
                    agent: Some("coding-agent".into()),
                    chat: Some("Workspace review".into()),
                    purpose: Some("agent_turn".into()),
                    ..Default::default()
                },
                node: "workstation".into(),
                totals: totals.clone(),
                cost: cost.clone(),
            },
            UsageGroup {
                dimensions: UsageFilter {
                    tool: Some("read_file".into()),
                    ..Default::default()
                },
                node: "workstation".into(),
                totals: totals.clone(),
                cost: Default::default(),
            },
            UsageGroup {
                dimensions: UsageFilter {
                    skill: Some("review-code".into()),
                    agent: Some("coding-agent".into()),
                    ..Default::default()
                },
                node: "workstation".into(),
                totals: totals.clone(),
                cost: Default::default(),
            },
        ];
        let series = (0..12)
            .map(|i| {
                let c = UsageCost {
                    amounts: BTreeMap::from([("USD".into(), (i as f64 + 1.0) * 0.004)]),
                    ..Default::default()
                };
                UsageBucket {
                    start_ms: 1_793_620_800_000 + i * 3_600_000,
                    totals: totals.clone(),
                    cost: c.clone(),
                    providers: BTreeMap::from([("openai".into(), c)]),
                }
            })
            .collect();
        UsageQueryResponse {
            totals,
            cost,
            groups,
            series,
            answered: vec!["workstation".into(), "server".into()],
            failed: vec![UsageNodeFailure {
                node: "laptop".into(),
                error: "Temporarily offline".into(),
            }],
            quota_history: vec![UsageRecord {
                id: "quota-sample".into(), timestamp_ms: 1_793_664_000_000,
                attribution: UsageAttribution::default(),
                event: UsageEvent::QuotaSampled {
                    account: "work".into(),
                    snapshot: serde_json::from_value(serde_json::json!({
                        "provider": "openai", "plan": "Team", "captured_at_unix": 1793664000,
                        "limits": [{"id":"weekly", "name":"Weekly allowance", "used_percent":42.0, "remaining_percent":58.0}],
                        "credits":{"balance":"12.50"}
                    })).unwrap(),
                },
            }],
            skill_history: vec![UsageRecord {
                id: "skill-publication".into(), timestamp_ms: 1_793_664_000_000,
                attribution: UsageAttribution { session: Some("authoring-session".into()), ..Default::default() },
                event: UsageEvent::SkillVersionPublished {
                    skill: "review-code".into(), previous_version: Some("1.0.0".into()), version: "1.1.0".into(),
                    previous_content_hash: Some("previous-hash".into()), content_hash:"published-hash".into(), source:"agent".into(),
                },
            }],
            budgets: vec![UsageBudgetProgress {
                name: "Daily allowance".into(),
                currency: "USD".into(),
                amount: 5.0,
                spent: 0.52,
                projected: 1.30,
                period_start_ms: 1_793_664_000_000,
                period_end_ms: 1_793_750_400_000,
                unpriced_requests: 2,
            }],
            ..Default::default()
        }
    }
    #[test]
    fn populated_usage_charts_and_sections_render_at_desktop_and_narrow_widths() -> sui::Result<()>
    {
        for width in [1280.0, 440.0] {
            for section in 0..4 {
                let shared = Shared::new(AppState::ready(Backend::local_fallback()));
                {
                    let mut c = shared.cluster_mut();
                    c.subview = ClusterSubview::Usage;
                    c.usage.key.section = section;
                    c.usage.key.response = Some(sample());
                }
                let reader = cluster_theme_reader(&shared);
                let app = sui_testing::TestApp::from_runtime(
                    sui::Application::new()
                        .window(sui::WindowBuilder::new().root(view(&shared, &reader)))
                        .build()?,
                )?;
                let window = app.main_window()?;
                window
                    .root()
                    .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                        width, 1000.0,
                    ))))?;
                app.run_until_idle()?;
                let snapshot = window.snapshot()?;
                assert!(snapshot.accessibility.nodes.iter().any(|n| {
                    n.name
                        .as_deref()
                        .is_some_and(|s| s.contains("Partial results"))
                }));
                if let Ok(dir) = std::env::var("SINOMO_USAGE_SCREENSHOTS") {
                    std::fs::create_dir_all(&dir).unwrap();
                    window.capture_screenshot()?.write_png(
                        std::path::Path::new(&dir)
                            .join(format!("usage-{section}-{}.png", width as u32)),
                    )?;
                }
            }
        }
        Ok(())
    }
}
