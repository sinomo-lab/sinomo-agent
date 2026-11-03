//! Retained, virtualized explorer catalog. Sampling values does not replace the list or its rows.
use super::*;
use sui::{VirtualList, VirtualListChrome};

#[derive(Clone, Debug, PartialEq)]
struct CatalogRow {
    metric: MetricCatalogSummary,
    domain: String,
    heading: bool,
    selected: bool,
}

impl CatalogRow {
    fn detail(&self) -> String {
        format!(
            "{}{} · {} · {} live series · {}",
            if self.selected { "Selected · " } else { "" },
            metric_kind_label(self.metric.descriptor.kind),
            metric_unit_label(self.metric.descriptor.unit),
            self.metric.current_series,
            self.metric.descriptor.help
        )
    }
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

/// One catalog row, built once per metric. Its domain heading, selection styling, and live
/// series count follow the row's data signal, so samples and selection keep the row and its
/// button's focus.
fn row(shared: &Shared, reader: &ThemeReader, data: Signal<CatalogRow>) -> Flex {
    let initial = data.get();
    let name = initial.metric.descriptor.name.clone();
    let heading = Presence::new(sui::prelude::Padding::new(
        Insets {
            left: 0.0,
            top: 0.0,
            right: 0.0,
            bottom: 2.0,
        },
        Label::new(initial.domain.to_ascii_uppercase())
            .text_from(
                data.select_named("catalog row domain", |row| row.domain.to_ascii_uppercase()),
            )
            .text_style_when(themed_style(
                reader,
                |theme| theme.text.xs,
                FontWeight::SEMIBOLD,
                |theme| theme.surfaces.text_faint,
            )),
    ))
    .transition(crate::ui::widgets::reveal_transition())
    .shown_from(data.select_named("catalog row heading", |row| row.heading))
    .collapse(Axis::Vertical);
    let action = shared.clone();
    let press_name = name.clone();
    let description_name = name.clone();
    let button = widgets::Restyle::new(
        Button::new(short_metric_name(&name, &initial.domain))
            .semantic_name(name.clone())
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                action
                    .cluster_mut()
                    .select_explorer_metric(press_name.clone());
                request_metric_explorer_relayout(ctx);
            }),
        data.select_named("catalog row button", move |row| {
            (
                row.selected,
                format!("{description_name}. {}", row.detail()),
            )
        }),
        |button, (selected, description)| {
            button
                .description(description.clone())
                .appearance(if *selected {
                    ButtonAppearance::Tonal
                } else {
                    ButtonAppearance::Ghost
                })
                .tone(if *selected {
                    SemanticTone::Accent
                } else {
                    SemanticTone::Neutral
                })
        },
    );
    let detail = Label::new(initial.detail())
        .text_from(data.select_named("catalog row detail", CatalogRow::detail))
        .text_style_when(themed_style(
            reader,
            |theme| theme.text.xs,
            FontWeight::NORMAL,
            |theme| theme.surfaces.text_faint,
        ));
    Flex::vertical()
        .align_items(Alignment::Stretch)
        .with_child(heading)
        .with_child(
            Flex::vertical()
                .align_items(Alignment::Stretch)
                .gap(2.0)
                .with_child(button)
                .with_child(detail),
        )
}

fn catalog_counts(key: &ClusterStructureKey) -> String {
    let search = key.explorer.search.trim().to_ascii_lowercase();
    let total = key.catalog.len();
    let matching = key
        .catalog
        .iter()
        .filter(|row| matches(row, key, &search))
        .count();
    if matching == total && matching <= EXPLORER_CATALOG_RESULT_LIMIT {
        format!(
            "{total} registered metric{}{}",
            if total == 1 { "" } else { "s" },
            if key.catalog_truncated {
                " · collection capped for safety"
            } else {
                ""
            }
        )
    } else {
        format!(
            "{} shown of {matching} matching · {total} registered{}",
            matching.min(EXPLORER_CATALOG_RESULT_LIMIT),
            if matching > EXPLORER_CATALOG_RESULT_LIMIT {
                " · refine search"
            } else {
                ""
            }
        )
    }
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
                row(&row_shared, &row_reader, data)
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
    let header = Surface::window(
        Label::new(String::new())
            .text_from(source.select_named("explorer catalog counts", catalog_counts))
            .theme_when(clone_reader(reader)),
    )
    .padding(Insets::all(12.0))
    .fill_width(true)
    .theme_when(clone_reader(reader));
    let empty = |title: &str, description: &str| {
        EmptyState::new(title, description)
            .name("Metric catalog results")
            .icon(IconGlyph::Search)
            .transparent()
            .theme_when(clone_reader(reader))
    };
    let empty = SwitchView::new()
        .selected_from(source.select_named("explorer empty catalog", |key| {
            usize::from(key.catalog.is_empty())
        }))
        .with_child(empty(
            "No matching metrics",
            "Try another name, help phrase, domain, or metric kind",
        ))
        .with_child(empty(
            "Waiting for registered metrics",
            "Refresh or wait for the first live observation sample",
        ));
    SwitchView::new()
        .selected_from(source.select_named("explorer catalog visibility", |key| {
            let search = key.explorer.search.trim().to_ascii_lowercase();
            usize::from(key.catalog.iter().any(|row| matches(row, key, &search)))
        }))
        .with_child(empty)
        .with_child(Dock::new(list).top(56.0, header))
}
