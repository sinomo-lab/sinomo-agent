//! Retained, virtualized explorer catalog. Sampling values does not replace the list.
use super::*;
use sui::{VirtualList, VirtualListChrome};

#[derive(Clone, Debug, PartialEq)]
struct CatalogRow {
    theme: DefaultTheme,
    metric: MetricCatalogSummary,
    domain: String,
    heading: bool,
    selected: bool,
}

fn matches(row: &MetricCatalogSummary, key: &ClusterStructureKey, search: &str) -> bool {
    key.explorer.kind_filter.includes(row.descriptor.kind)
        && (search.is_empty()
            || row.descriptor.name.to_ascii_lowercase().contains(search)
            || row.descriptor.help.to_ascii_lowercase().contains(search)
            || domain(row).to_ascii_lowercase().contains(search))
}

fn domain(row: &MetricCatalogSummary) -> String {
    row.descriptor.semantics.domain.clone().unwrap_or_else(|| {
        row.descriptor
            .name
            .split('.')
            .nth(1)
            .unwrap_or("other")
            .to_string()
    })
}

fn collection(key: &ClusterStructureKey) -> widgets::VirtualCollectionSnapshot<String, CatalogRow> {
    let search = key.explorer.search.trim().to_ascii_lowercase();
    let mut groups = BTreeMap::<String, Vec<&MetricCatalogSummary>>::new();
    for row in key
        .catalog
        .iter()
        .filter(|row| matches(row, key, &search))
        .take(EXPLORER_CATALOG_RESULT_LIMIT)
    {
        groups.entry(domain(row)).or_default().push(row);
    }
    let mut rows = Vec::new();
    for (domain, entries) in groups {
        for (index, entry) in entries.into_iter().enumerate() {
            let name = entry.descriptor.name.clone();
            rows.push((
                name.clone(),
                CatalogRow {
                    theme: key.theme,
                    metric: entry.clone(),
                    domain: domain.clone(),
                    heading: index == 0,
                    selected: key.explorer.selected_metric.as_ref() == Some(&name),
                },
            ));
        }
    }
    widgets::VirtualCollectionSnapshot::new(rows, key.explorer.selected_metric.clone())
}

fn row(shared: &Shared, reader: &ThemeReader, row: &CatalogRow) -> WidgetPod {
    let theme = row.theme;
    let pal = Palette::from_theme(&theme, theme.surfaces.dark);
    let name = row.metric.descriptor.name.clone();
    let detail = format!(
        "{}{} · {} · {} live series · {}",
        if row.selected { "Selected · " } else { "" },
        metric_kind_label(row.metric.descriptor.kind),
        metric_unit_label(row.metric.descriptor.unit),
        row.metric.current_series,
        row.metric.descriptor.help
    );
    let action = shared.clone();
    let mut body = Flex::vertical().align_items(Alignment::Stretch).gap(2.0);
    if row.heading {
        body.push(
            Label::new(row.domain.to_ascii_uppercase()).text_style(pal.text_style(
                &theme,
                theme.text.xs.size,
                FontWeight::SEMIBOLD,
                pal.text_faint,
            )),
        );
    }
    body.push(
        Button::new(short_metric_name(&name, &row.domain))
            .semantic_name(name.clone())
            .description(format!("{name}. {detail}"))
            .appearance(if row.selected {
                ButtonAppearance::Tonal
            } else {
                ButtonAppearance::Ghost
            })
            .tone(if row.selected {
                SemanticTone::Accent
            } else {
                SemanticTone::Neutral
            })
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                action.cluster_mut().select_explorer_metric(name.clone());
                request_metric_explorer_relayout(ctx);
            }),
    );
    body.push(Label::new(detail).text_style(pal.text_style(
        &theme,
        theme.text.xs.size,
        FontWeight::NORMAL,
        pal.text_faint,
    )));
    WidgetPod::new(body)
}

pub(super) fn view(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let source = shared.signals().cluster_structure.clone();
    let collection = source.select_named("explorer catalog rows", collection);
    let row_shared = shared.clone();
    let action = shared.clone();
    let row_reader = reader.clone();
    let list_reader = reader.clone();
    let list = widgets::VirtualCollectionHost::new(
        "Metric catalog collection",
        collection,
        move |model, state| {
            VirtualList::new("Registered metric catalog", model, move |_key, data| {
                let shared = row_shared.clone();
                let reader = row_reader.clone();
                RebuildOnChange::key_from(data, move |data| row(&shared, &reader, data))
            })
            .state(state)
            .estimated_row_height(76.0)
            .overscan_viewports(0.5)
            .cache_capacity(24)
            .padding(Insets::all(12.0))
            .row_padding(Insets::ZERO)
            .gap(10.0)
            .chrome(VirtualListChrome::Transparent)
            .theme_when(clone_reader(&list_reader))
            .row_name(|name, _| name.clone())
            .on_change_with_ctx(move |ctx, name| {
                action.cluster_mut().select_explorer_metric(name);
                request_metric_explorer_relayout(ctx);
            })
        },
    );
    let header_source = source.select_named("explorer catalog counts", |key| {
        let search = key.explorer.search.trim().to_ascii_lowercase();
        let count = key
            .catalog
            .iter()
            .filter(|row| matches(row, key, &search))
            .count();
        (key.theme, key.catalog.len(), count, key.catalog_truncated)
    });
    let header = RebuildOnChange::key_from(header_source, |(theme, total, matching, capped)| {
        let text = if matching == total && *matching <= EXPLORER_CATALOG_RESULT_LIMIT {
            format!(
                "{total} registered metric{}{}",
                if *total == 1 { "" } else { "s" },
                if *capped {
                    " · collection capped for safety"
                } else {
                    ""
                }
            )
        } else {
            format!(
                "{} shown of {matching} matching · {total} registered{}",
                (*matching).min(EXPLORER_CATALOG_RESULT_LIMIT),
                if *matching > EXPLORER_CATALOG_RESULT_LIMIT {
                    " · refine search"
                } else {
                    ""
                }
            )
        };
        WidgetPod::new(
            Surface::window(Label::new(text).theme(*theme))
                .padding(Insets::all(12.0))
                .fill_width(true)
                .theme(*theme),
        )
    });
    let empty_reader = reader.clone();
    let empty = RebuildOnChange::key_from(
        source.select_named("explorer empty catalog", |key| {
            (key.theme, key.catalog.is_empty())
        }),
        move |(_, waiting)| {
            WidgetPod::new(
                EmptyState::new(
                    if *waiting {
                        "Waiting for registered metrics"
                    } else {
                        "No matching metrics"
                    },
                    if *waiting {
                        "Refresh or wait for the first live observation sample"
                    } else {
                        "Try another name, help phrase, domain, or metric kind"
                    },
                )
                .name("Metric catalog results")
                .icon(IconGlyph::Search)
                .transparent()
                .theme_when(clone_reader(&empty_reader)),
            )
        },
    );
    SwitchView::new()
        .selected_from(source.select_named("explorer catalog visibility", |key| {
            let search = key.explorer.search.trim().to_ascii_lowercase();
            usize::from(key.catalog.iter().any(|row| matches(row, key, &search)))
        }))
        .with_child(empty)
        .with_child(Dock::new(list).top(56.0, header))
}
