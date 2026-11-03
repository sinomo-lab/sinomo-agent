//! Usage presentation is a projection of Observe results, never a recorder.
use super::*;
use sinomo_client::usage_contract::*;

pub(super) fn content(shared: &Shared, reader: &ThemeReader, key: &UsageKey) -> WidgetPod {
    let mut rows = Flex::vertical().align_items(Alignment::Stretch).gap(16.0);
    let Some(response) = &key.response else {
        return WidgetPod::new(rows.with_child(label(
            if key.loading {
                "Collecting usage…"
            } else {
                "No usage loaded. Refresh to collect retained history."
            },
            reader,
        )));
    };
    for failure in &response.failed {
        rows = rows.with_child(label(
            format!("{} — {}", failure.node, failure.error),
            reader,
        ));
    }
    for omitted in &response.omitted {
        rows = rows.with_child(label(omitted, reader));
    }
    for warning in &response.warnings {
        rows = rows.with_child(label(warning, reader));
    }
    match key.section {
        0 => summary(shared, reader, response, &mut rows),
        1 => providers(shared, reader, response, &mut rows),
        2 => tools(shared, reader, response, &mut rows),
        3 => skills(shared, reader, response, &mut rows),
        _ => {}
    }
    WidgetPod::new(
        Surface::window(rows)
            .padding(Insets::all(16.0))
            .theme_when(clone_reader(reader)),
    )
}
fn summary(shared: &Shared, r: &ThemeReader, response: &UsageQueryResponse, rows: &mut Flex) {
    let t = &response.totals;
    let mut tiles = Flex::horizontal().gap(10.0).wrap(FlexWrap::Wrap);
    for (title, value, section, kind) in [
        (
            "Estimated cost",
            response
                .cost
                .amounts
                .iter()
                .map(|(currency, value)| format!("{currency} {value:.2}"))
                .collect::<Vec<_>>()
                .join(" · "),
            1,
            None,
        ),
        (
            "Model requests",
            format!(
                "{} · {:.1}% errors",
                t.model_requests,
                rate(t.model_errors, t.model_requests)
            ),
            1,
            Some("lm.request.completed"),
        ),
        (
            "Tool calls",
            format!(
                "{} · {:.1}% failures",
                t.tool_calls,
                rate(t.tool_errors, t.tool_calls)
            ),
            2,
            Some("agent.tool.call.completed"),
        ),
        (
            "Skill activations",
            t.skill_activations.to_string(),
            3,
            Some("skill.activated"),
        ),
        (
            "Publications",
            t.skill_publications.to_string(),
            3,
            Some("skill.version.published"),
        ),
    ] {
        let action = shared.clone();
        let button = Button::new(format!("{title}\n{value}"))
            .min_height(76.0)
            .appearance(ButtonAppearance::Tonal)
            .theme_when(clone_reader(r))
            .on_press_with_ctx(move |ctx| {
                {
                    let mut c = action.cluster_mut();
                    c.usage.key.section = section;
                    c.usage.key.query.filter.event_kind = kind.map(str::to_string);
                    c.usage.key.query.cursor = None;
                }
                request(&action, true);
                request_metric_explorer_relayout(ctx);
            });
        tiles.push_item(button, FlexItem::fill().min_width(148.0));
    }
    rows.push(tiles);
    rows.push(label(cost_text(&response.cost), r));
    rows.push(label(
        format!(
            "Reported units · {} / {} requests",
            t.reported_requests, t.model_requests
        ),
        r,
    ));
    rows.push(label(units_text(&t.units), r));
    if t.model_requests == 0
        && t.tool_calls == 0
        && t.skill_activations == 0
        && t.skill_publications == 0
    {
        rows.push(label(
            "No recorded work in this range. Unreported units remain unknown.",
            r,
        ));
    }
    for currency in response.cost.amounts.keys() {
        let providers = response
            .groups
            .iter()
            .filter_map(|g| g.dimensions.provider.clone())
            .collect::<BTreeSet<_>>();
        let series = providers
            .into_iter()
            .take(8)
            .map(|provider| {
                let points = response
                    .series
                    .iter()
                    .map(|b| {
                        (
                            b.start_ms,
                            b.providers
                                .get(&provider)
                                .and_then(|c| c.amounts.get(currency))
                                .copied()
                                .unwrap_or(0.0),
                        )
                    })
                    .collect();
                (provider, points)
            })
            .collect();
        rows.push(UsagePlot {
            title: format!("Estimated cost over time · {currency}"),
            series,
            reader: r.clone(),
            shared: shared.clone(),
            bucket_ms: shared.cluster().usage.key.query.bucket_ms,
        });
    }
    for (title, dimension, unit) in [
        ("Units by model", "model", true),
        ("Requests by purpose", "purpose", false),
        ("Top agents by cost", "agent", false),
        ("Top chats by cost", "chat", false),
    ] {
        let groups = grouped(&response.groups, dimension);
        let mut bars = Vec::new();
        for (name, g) in groups {
            let (value, display) = if unit {
                let n = g
                    .totals
                    .units
                    .input_tokens
                    .unwrap_or(0)
                    .saturating_add(g.totals.units.output_tokens.unwrap_or(0));
                (
                    n as f64,
                    format!(
                        "{} in · {} out",
                        count(g.totals.units.input_tokens),
                        count(g.totals.units.output_tokens)
                    ),
                )
            } else if dimension == "purpose" {
                (
                    g.totals.model_requests as f64,
                    format!("{} requests", g.totals.model_requests),
                )
            } else {
                (
                    g.cost.amounts.get("USD").copied().unwrap_or(0.0),
                    cost_text(&g.cost),
                )
            };
            bars.push((name, value, display, g.dimensions));
        }
        bars.sort_by(|a, b| b.1.total_cmp(&a.1));
        bars.truncate(8);
        rows.push(label(title, r));
        let max = bars.iter().map(|b| b.1).fold(0.0f64, f64::max).max(1.0);
        for (name, value, display, filter) in bars {
            let action = shared.clone();
            let select = Button::new(name.clone())
                .appearance(ButtonAppearance::Ghost)
                .theme_when(clone_reader(r))
                .on_press_with_ctx(move |ctx| {
                    apply_filter(&action, filter.clone());
                    request_metric_explorer_relayout(ctx);
                });
            rows.push(
                Flex::vertical()
                    .align_items(Alignment::Stretch)
                    .gap(4.0)
                    .with_child(select)
                    .with_child(label(display, r))
                    .with_child(
                        sui::ProgressBar::new(name)
                            .range(0.0, max)
                            .value(value)
                            .height(8.0)
                            .theme_when(clone_reader(r)),
                    ),
            );
        }
    }
    rows.push(label("Budgets · display only", r));
    if response.budgets.is_empty() {
        rows.push(label("No budgets configured.", r));
    }
    for b in &response.budgets {
        rows.push(label(
            format!(
                "{}   {} {:.2} / {:.2} ({:.0}%)   projected {:.2} · {} unpriced requests",
                b.name,
                b.currency,
                b.spent,
                b.amount,
                if b.amount > 0.0 {
                    100.0 * b.spent / b.amount
                } else {
                    0.0
                },
                b.projected,
                b.unpriced_requests
            ),
            r,
        ));
    }
    let action = shared.clone();
    rows.push(
        Button::new("Usage settings")
            .theme_when(clone_reader(r))
            .on_press(move || {
                action.settings_mut().open(SettingsSection::Usage);
                action.set_active_tab(Tab::Settings);
            }),
    );
}
fn providers(shared: &Shared, r: &ThemeReader, response: &UsageQueryResponse, rows: &mut Flex) {
    rows.push(label("Providers, accounts and models", r));
    for (name, g) in grouped(&response.groups, "provider") {
        let t = &g.totals;
        let card = Flex::vertical()
            .gap(8.0)
            .with_child(label(name, r))
            .with_child(label(
                format!(
                    "{} requests · {} errors · latency p50 {} ms / p95 {} ms",
                    t.model_requests,
                    t.model_errors,
                    count(latency_percentile(&t.model_latency_ms, 50)),
                    count(latency_percentile(&t.model_latency_ms, 95))
                ),
                r,
            ))
            .with_child(label(
                format!(
                    "Reported units: {} / {} requests · {} retries across {} reporting requests",
                    t.reported_requests, t.model_requests, t.retries, t.retry_reporting_requests
                ),
                r,
            ))
            .with_child(label(units_text(&t.units), r))
            .with_child(label(cost_text(&g.cost), r))
            .with_child(label(
                format!(
                    "Price source: {}",
                    if g.cost.price_sources.is_empty() {
                        "No applicable catalog price".into()
                    } else {
                        g.cost.price_sources.join(" · ")
                    }
                ),
                r,
            ))
            .with_child(inspect(shared, r, "Inspect requests", g.dimensions, 4));
        rows.push(
            Surface::panel(card)
                .padding(Insets::all(12.0))
                .theme_when(clone_reader(r)),
        );
    }
    if !response.cost.unpriced_models.is_empty() {
        rows.push(label(
            format!(
                "Unpriced models: {}",
                response.cost.unpriced_models.join(", ")
            ),
            r,
        ));
    }
    rows.push(label("Quota and credit history", r));
    if response.quota_history.is_empty() {
        rows.push(label("No provider quota snapshots in this range.", r));
    }
    for record in &response.quota_history {
        if let UsageEvent::QuotaSampled { account, snapshot } = &record.event {
            let mut card = Flex::vertical().gap(6.0).with_child(label(
                format!(
                    "{} / {account} · {} · {}",
                    snapshot.provider,
                    snapshot.plan.as_deref().unwrap_or("No plan reported"),
                    time_text(record.timestamp_ms)
                ),
                r,
            ));
            for limit in &snapshot.limits {
                card = card.with_child(label(quota_limit(limit), r));
                if let Some(used) = limit
                    .used_percent
                    .or_else(|| limit.remaining_percent.map(|remaining| 100.0 - remaining))
                {
                    card = card.with_child(
                        sui::ProgressBar::new(
                            limit.name.clone().unwrap_or_else(|| limit.id.clone()),
                        )
                        .range(0.0, 100.0)
                        .value(used.clamp(0.0, 100.0))
                        .height(8.0)
                        .theme_when(clone_reader(r)),
                    );
                }
            }
            if let Some(credits) = &snapshot.credits {
                card = card.with_child(label(
                    format!(
                        "Credits: {}",
                        credits.balance.as_deref().unwrap_or("unreported")
                    ),
                    r,
                ));
            }
            rows.push(
                Surface::panel(card)
                    .padding(Insets::all(12.0))
                    .theme_when(clone_reader(r)),
            );
        }
    }
    if response.history_truncated {
        rows.push(label("Latest 200 history entries shown. Use Records and a narrower range for older evidence.",r));
    }
}
fn tools(shared: &Shared, r: &ThemeReader, response: &UsageQueryResponse, rows: &mut Flex) {
    rows.push(label("Tools · select a tool to inspect its calls", r));
    for (name, g) in grouped(&response.groups, "tool") {
        let t = &g.totals;
        let card = Flex::vertical()
            .gap(8.0)
            .with_child(label(name, r))
            .with_child(label(
                format!(
                    "{} calls · {} failures · {} detached · p50 {} ms / p95 {} ms",
                    t.tool_calls,
                    t.tool_errors,
                    t.detached_tool_calls,
                    count(latency_percentile(&t.tool_latency_ms, 50)),
                    count(latency_percentile(&t.tool_latency_ms, 95))
                ),
                r,
            ))
            .with_child(label(
                t.statuses
                    .iter()
                    .map(|(s, n)| format!("{} {n}", s.trim_start_matches("tool.")))
                    .collect::<Vec<_>>()
                    .join(" · "),
                r,
            ))
            .with_child(label(
                format!(
                    "Runtime: {} · Executing nodes: {} · Last used {}",
                    t.runtimes.keys().cloned().collect::<Vec<_>>().join(", "),
                    t.executing_nodes
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                    t.last_used_ms
                        .map(time_text)
                        .unwrap_or_else(|| "unknown".into())
                ),
                r,
            ))
            .with_child(inspect(shared, r, "Inspect calls", g.dimensions, 4));
        rows.push(
            Surface::panel(card)
                .padding(Insets::all(12.0))
                .theme_when(clone_reader(r)),
        );
    }
}
fn skills(shared: &Shared, r: &ThemeReader, response: &UsageQueryResponse, rows: &mut Flex) {
    rows.push(label("Skills · activations and publications", r));
    for (name, g) in grouped(&response.groups, "skill") {
        let agents = g
            .totals
            .agents
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let card = Flex::vertical()
            .gap(8.0)
            .with_child(label(
                format!(
                    "{name} · {} activations · {} publications",
                    g.totals.skill_activations, g.totals.skill_publications
                ),
                r,
            ))
            .with_child(label(
                format!(
                    "Triggers: {}",
                    g.totals
                        .skill_triggers
                        .iter()
                        .map(|(s, n)| format!("{} {n}", s.replace('_', " ")))
                        .collect::<Vec<_>>()
                        .join(" · ")
                ),
                r,
            ))
            .with_child(label(
                format!(
                    "Agents: {} · Last used {}",
                    if agents.is_empty() {
                        "Not attributed"
                    } else {
                        &agents
                    },
                    g.totals
                        .last_used_ms
                        .map(time_text)
                        .unwrap_or_else(|| "unknown".into())
                ),
                r,
            ))
            .with_child(inspect(shared, r, "Inspect activations", g.dimensions, 4));
        rows.push(
            Surface::panel(card)
                .padding(Insets::all(12.0))
                .theme_when(clone_reader(r)),
        );
    }
    rows.push(label("Version timeline", r));
    for record in &response.skill_history {
        if let UsageEvent::SkillVersionPublished {
            skill,
            version,
            previous_version,
            source,
            content_hash,
            ..
        } = &record.event
        {
            let action = shared.clone();
            let name = skill.clone();
            let selected = version.clone();
            let card = Flex::vertical()
                .gap(8.0)
                .with_child(label(
                    format!(
                        "{} · {skill} · {} → {version}",
                        time_text(record.timestamp_ms),
                        previous_version.as_deref().unwrap_or("new")
                    ),
                    r,
                ))
                .with_child(label(
                    format!(
                        "{source} · author session {}",
                        record
                            .attribution
                            .session
                            .as_deref()
                            .unwrap_or("not reported")
                    ),
                    r,
                ))
                .with_child(label(format!("Content {content_hash}"), r))
                .with_child(
                    Button::new("Open skill version")
                        .theme_when(clone_reader(r))
                        .on_press(move || {
                            crate::ui::settings::open_usage_skill(&action, &name, &selected)
                        }),
                );
            rows.push(
                Surface::panel(card)
                    .padding(Insets::all(12.0))
                    .theme_when(clone_reader(r)),
            );
        }
    }
}
fn inspect(
    shared: &Shared,
    r: &ThemeReader,
    title: &str,
    filter: UsageFilter,
    section: usize,
) -> Button {
    let action = shared.clone();
    Button::new(title)
        .theme_when(clone_reader(r))
        .on_press_with_ctx(move |ctx| {
            action.cluster_mut().usage.key.section = section;
            apply_filter(&action, filter.clone());
            request_metric_explorer_relayout(ctx);
        })
}
pub(super) fn units_text(units: &LanguageModelUsage) -> String {
    let mut text = format!(
        "Input {} · Output {} · Cache read {} · Cache write {} · Reasoning {}",
        count(units.input_tokens),
        count(units.output_tokens),
        count(units.cache_read_input_tokens),
        count(units.cache_write_input_tokens),
        count(units.reasoning_tokens)
    );
    if let Some(searches) = units.hosted_searches {
        text.push_str(&format!(" · {searches} searches"));
    }
    if let Some(images) = &units.generated_images {
        for image in images {
            text.push_str(&format!(
                " · {} images {} / {}",
                image.count,
                image.size.as_deref().unwrap_or("unspecified size"),
                image.quality.as_deref().unwrap_or("unspecified quality")
            ));
        }
    }
    for unit in &units.named_units {
        text.push_str(&format!(
            " · {} {}",
            unit.count,
            unit.name.replace('_', " ")
        ));
    }
    text
}

fn grouped(groups: &[UsageGroup], axis: &str) -> BTreeMap<String, UsageGroup> {
    let mut output = BTreeMap::<String, UsageGroup>::new();
    for g in groups {
        let d = &g.dimensions;
        let name = match axis {
            "provider" => d.provider.as_ref().map(|p| {
                format!(
                    "{} / {} / {}",
                    p,
                    d.account.as_deref().unwrap_or("—"),
                    d.model.as_deref().unwrap_or("quota")
                )
            }),
            "tool" => d.tool.clone(),
            "skill" => d.skill.clone(),
            "model" => d.model.clone(),
            "purpose" => d.purpose.clone(),
            "agent" => d.agent.clone(),
            "chat" => d.chat.clone(),
            _ => None,
        };
        let Some(name) = name else {
            continue;
        };
        let filter = match axis {
            "provider" => UsageFilter {
                provider: d.provider.clone(),
                account: d.account.clone(),
                model: d.model.clone(),
                ..Default::default()
            },
            "tool" => UsageFilter {
                tool: d.tool.clone(),
                ..Default::default()
            },
            "skill" => UsageFilter {
                skill: d.skill.clone(),
                ..Default::default()
            },
            "model" => UsageFilter {
                model: d.model.clone(),
                ..Default::default()
            },
            "purpose" => UsageFilter {
                purpose: d.purpose.clone(),
                ..Default::default()
            },
            "agent" => UsageFilter {
                agent: d.agent.clone(),
                ..Default::default()
            },
            _ => UsageFilter {
                chat: d.chat.clone(),
                ..Default::default()
            },
        };
        let aggregate = output.entry(name).or_insert_with(|| UsageGroup {
            dimensions: filter,
            node: String::new(),
            totals: Default::default(),
            cost: Default::default(),
        });
        merge_totals(&mut aggregate.totals, &g.totals);
        pricing::merge_cost(&mut aggregate.cost, &g.cost);
    }
    output
}
fn rate(n: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * n as f64 / total as f64
    }
}
fn time_text(ms: u64) -> String {
    OffsetDateTime::from_unix_timestamp((ms / 1000) as i64)
        .ok()
        .and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| "Unknown time".into())
}
fn quota_limit(limit: &sinomo_runtime::api::lm::LanguageModelUsageLimit) -> String {
    let title = limit.name.as_deref().unwrap_or(&limit.id);
    let remaining = if limit.unlimited == Some(true) {
        "Unlimited".into()
    } else if let Some(percent) = limit.remaining_percent {
        format!("{percent:.1}% remaining")
    } else if let Some(percent) = limit.used_percent {
        format!("{percent:.1}% used")
    } else if let Some(remaining) = limit.remaining {
        format!(
            "{remaining} remaining{}",
            limit
                .entitlement
                .map(|total| format!(" of {total}"))
                .unwrap_or_default()
        )
    } else {
        "Remaining usage not reported".into()
    };
    let reset = limit
        .reset_at_unix
        .map(|t| {
            format!(
                " · resets {}",
                time_text((t.max(0) as u64).saturating_mul(1000))
            )
        })
        .unwrap_or_default();
    format!(
        "{title} · {remaining}{reset}{}",
        if limit.exhausted == Some(true) {
            " · exhausted"
        } else {
            ""
        }
    )
}

fn apply_filter(shared: &Shared, filter: UsageFilter) {
    {
        let mut c = shared.cluster_mut();
        let old = &mut c.usage.key.query.filter;
        macro_rules! fields{($($field:ident),*)=>{$(if filter.$field.is_some(){old.$field=filter.$field.clone();})*}}
        fields!(
            provider, account, model, tool, skill, agent, chat, purpose, operation
        );
        old.event_kind = None;
        c.usage.key.query.cursor = None;
    }
    request(shared, true);
}
struct UsagePlot {
    title: String,
    series: Vec<(String, Vec<(u64, f64)>)>,
    reader: ThemeReader,
    shared: Shared,
    bucket_ms: u64,
}
impl Widget for UsagePlot {
    fn measure(&mut self, _: &mut MeasureCtx, c: Constraints) -> Size {
        let width = if c.max.width.is_finite() {
            c.max.width
        } else {
            640.0
        };
        let columns = ((width - 62.0) / 130.0).floor().max(1.0) as usize;
        let extra_rows = self.series.len().div_ceil(columns).saturating_sub(1);
        c.clamp(Size::new(width, 260.0 + extra_rows as f32 * 24.0))
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        let bounds = ctx.bounds();
        let theme = (self.reader)();
        let pal = theme.surfaces;
        let style = theme.body_text_style();
        widgets::draw_text_in(
            ctx,
            Rect::new(bounds.x(), bounds.y(), bounds.width(), 26.0),
            &self.title,
            style.clone(),
        );
        let legend_columns = ((bounds.width() - 62.0) / 130.0).floor().max(1.0) as usize;
        let extra_legend_height =
            self.series.len().div_ceil(legend_columns).saturating_sub(1) as f32 * 24.0;
        let plot = Rect::new(
            bounds.x() + 52.0,
            bounds.y() + 34.0,
            (bounds.width() - 62.0).max(1.0),
            (bounds.height() - 110.0 - extra_legend_height).max(1.0),
        );
        let start = self
            .series
            .iter()
            .flat_map(|(_, p)| p.iter().map(|p| p.0))
            .min()
            .unwrap_or(0);
        let end = self
            .series
            .iter()
            .flat_map(|(_, p)| p.iter().map(|p| p.0))
            .max()
            .unwrap_or(start + 1)
            .max(start + 1);
        let max = self
            .series
            .iter()
            .flat_map(|(_, p)| p.iter().map(|p| p.1))
            .fold(0.0f64, f64::max)
            .max(0.000001);
        let colors = crate::theme::agent_hues(&theme);
        for tick in 0..=4 {
            let y = plot.y() + plot.height() * tick as f32 / 4.0;
            let mut path = PathBuilder::new();
            path.move_to(Point::new(plot.x(), y));
            path.line_to(Point::new(plot.max_x(), y));
            ctx.stroke(path.build(), pal.border, StrokeStyle::new(1.0));
            widgets::draw_text_in(
                ctx,
                Rect::new(bounds.x(), y - 8.0, 50.0, 20.0),
                &format!("{:.3}", max * (1.0 - tick as f64 / 4.0)),
                style.clone(),
            );
        }
        let ticks: &[f64] = if plot.width() >= 360.0 {
            &[0.0, 0.5, 1.0]
        } else {
            &[0.0, 1.0]
        };
        for &fraction in ticks {
            let value = start + ((end - start) as f64 * fraction) as u64;
            let text = OffsetDateTime::from_unix_timestamp((value / 1000) as i64)
                .map(|t| {
                    format!(
                        "{:02}-{:02} {:02}:{:02}",
                        u8::from(t.month()),
                        t.day(),
                        t.hour(),
                        t.minute()
                    )
                })
                .unwrap_or_default();
            let x = plot.x() + (plot.width() - 120.0) * fraction as f32;
            widgets::draw_text_in(
                ctx,
                Rect::new(x, plot.max_y() + 8.0, 120.0, 24.0),
                &text,
                style.clone(),
            );
        }
        for (index, (name, points)) in self.series.iter().enumerate() {
            let mut path = PathBuilder::new();
            for (i, (time, value)) in points.iter().enumerate() {
                let point = Point::new(
                    plot.x()
                        + plot.width() * ((*time - start) as f64 / (end - start) as f64) as f32,
                    plot.max_y() - plot.height() * (*value / max) as f32,
                );
                if i == 0 {
                    path.move_to(point);
                } else {
                    path.line_to(point);
                }
            }
            ctx.stroke(
                path.build(),
                colors[index % colors.len()],
                StrokeStyle::new(2.0),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    plot.x() + (index % legend_columns) as f32 * 130.0,
                    plot.max_y() + 38.0 + (index / legend_columns) as f32 * 24.0,
                    128.0,
                    24.0,
                ),
                name,
                style.clone(),
            );
        }
    }
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Pointer(pointer) = event {
            if pointer.kind == PointerEventKind::Down
                && pointer.button == Some(PointerButton::Primary)
            {
                let times = self
                    .series
                    .iter()
                    .flat_map(|(_, p)| p.iter().map(|p| p.0))
                    .collect::<Vec<_>>();
                let Some(start) = times.iter().min().copied() else {
                    return;
                };
                let end = times.iter().max().copied().unwrap_or(start);
                let bounds = ctx.bounds();
                let fraction = ((pointer.position.x - bounds.x() - 52.0)
                    / (bounds.width() - 62.0).max(1.0))
                .clamp(0.0, 1.0);
                let selected = start + ((end - start) as f64 * fraction as f64) as u64;
                let bucket = selected / self.bucket_ms * self.bucket_ms;
                {
                    let mut c = self.shared.cluster_mut();
                    c.usage.key.range = 5;
                    c.usage.key.query.start_ms = bucket;
                    c.usage.key.query.end_ms = bucket.saturating_add(self.bucket_ms);
                    c.usage.key.query.cursor = None;
                }
                request(&self.shared, true);
                request_metric_explorer_relayout(ctx);
            }
        }
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Image, ctx.bounds());
        node.name = Some(self.title.clone());
        node.description=Some("Estimated cost by provider. Select a point to filter the time range; date controls provide the same keyboard-accessible selection.".into());
        ctx.push(node);
    }
}
