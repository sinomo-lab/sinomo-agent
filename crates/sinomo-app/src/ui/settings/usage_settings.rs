use super::*;
use sinomo_client::usage_contract::{UsageSettings, pricing};
pub(super) fn section(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let groups = settings_projection(shared, "usage configuration", |snapshot| {
        let configuration = &snapshot.configuration;
        configuration
            .items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.key.trim_start_matches('/') == pricing::SETTINGS_KEY)
            .map(|(index, item)| ConfigRowStructure {
                index,
                item: item.clone(),
                expanded: configuration.expanded.contains(&item.key),
                revision: configuration.ui_revision,
            })
            .collect::<Vec<_>>()
    });
    let s = shared.clone();
    let r = reader.clone();
    let editor = KeyedStack::vertical(
        groups,
        |row: &ConfigRowStructure| (row.item.key.clone(), row.index),
        move |(_, index), row| config_row(&s, &r, *index, row),
    );
    let catalog = settings_projection(shared, "usage effective prices", |snapshot| {
        snapshot
            .configuration
            .items
            .iter()
            .find(|item| item.key.trim_start_matches('/') == pricing::SETTINGS_KEY)
            .and_then(|item| serde_json::from_value::<UsageSettings>(item.raw_value.clone()).ok())
            .map(|s| s.prices)
            .unwrap_or_default()
    });
    let s = shared.clone();
    let r = reader.clone();
    let prices = RebuildOnChange::key_from(catalog, move |overrides| {
        let mut card=SettingsCard::new("Price catalog").description("Token rates are per million tokens. Image, search and named-unit rates are per unit. Overrides take precedence and re-price retained history.");
        for (origin, price) in overrides
            .iter()
            .map(|p| ("Override", p))
            .chain(pricing::default_prices().iter().map(|p| ("Built-in", p)))
        {
            card = card.row(muted_label(
                &s,
                format!(
                    "{origin}: {} / {} / {} · {}\n{}\nSource: {}",
                    price.provider,
                    price.model_pattern,
                    price.operation,
                    price.currency,
                    price
                        .units
                        .iter()
                        .map(|(unit, value)| format!("{} {value}", unit.replace('_', " ")))
                        .collect::<Vec<_>>()
                        .join(" · "),
                    price.source
                ),
            ));
        }
        WidgetPod::new(card.build(&s, &r))
    });
    let size_shared = shared.clone();
    let sizes = Label::new("")
        .text_from(
            shared
                .signals()
                .cluster_structure
                .select_named("usage storage sizes", |key| key.usage_storage_summary()),
        )
        .text_style_when(move || settings_text_style(&size_shared, TextSize::Sm));
    let action = shared.clone();
    let refresh = sui::Button::new("Refresh ledger sizes")
        .theme_when(clone_reader(reader))
        .on_press(move || crate::ui::cluster::refresh_usage_sizes(&action));
    Column::new(reader,|theme|theme.metrics.form_section_description_gap)
        .with_child(muted_label(shared,"Budgets only display spending progress. They never block requests or change model selection. Blank retention values keep history forever; deleting history cannot be undone."))
        .with_child(editor).with_child(prices)
        .with_child(SettingsCard::new("Storage by node").row(sizes).row(refresh).build(shared,reader))
}
