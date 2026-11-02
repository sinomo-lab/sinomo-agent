//! App theming on SUI's built-in presets.
//!
//! Each appearance choice selects a stock [`DefaultTheme`] preset unchanged: **Light**, **Dark**,
//! and **Void** (true black for OLED: borders instead of shadows). The only application
//! preference applied on top is the reading text scale, which multiplies SUI's own type ramp.
//! Widgets and custom paint read colors, metrics and type directly from the resolved theme's
//! token layers (`surfaces`, `palette`, `decorative`, `metrics`, `text`).

use sui::{Color, ControlSize, DecorativeHue, DefaultTheme, FontWeight, TextStyle, ThemeTextToken};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AppThemeMode {
    Light,
    Dark,
    /// True black for OLED: elevation is drawn with borders, never shadows.
    Void,
    #[default]
    System,
}

impl AppThemeMode {
    pub fn label(self) -> &'static str {
        match self {
            AppThemeMode::Light => "Light",
            AppThemeMode::Dark => "Dark",
            AppThemeMode::Void => "Void",
            AppThemeMode::System => "System",
        }
    }

    pub fn next(self) -> Self {
        match self {
            AppThemeMode::Light => AppThemeMode::Dark,
            AppThemeMode::Dark => AppThemeMode::Void,
            AppThemeMode::Void => AppThemeMode::System,
            AppThemeMode::System => AppThemeMode::Light,
        }
    }

    fn is_dark(self) -> bool {
        match self {
            AppThemeMode::Light => false,
            AppThemeMode::Dark | AppThemeMode::Void => true,
            // No OS hook wired yet; default the "System" choice to dark, which
            // suits the product's look.
            AppThemeMode::System => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppTextScale {
    #[default]
    Standard,
    Large,
    ExtraLarge,
}

impl AppTextScale {
    pub const ALL: [Self; 3] = [Self::Standard, Self::Large, Self::ExtraLarge];

    pub fn label(self) -> &'static str {
        match self {
            Self::Standard => "Standard",
            Self::Large => "Large",
            Self::ExtraLarge => "Extra large",
        }
    }

    pub fn factor(self) -> f32 {
        match self {
            Self::Standard => 1.0,
            Self::Large => 1.15,
            Self::ExtraLarge => 1.3,
        }
    }
}

/// Resolve the concrete widget theme for a mode.
pub fn resolve_theme(mode: AppThemeMode) -> DefaultTheme {
    resolve_theme_with_preferences(mode, AppTextScale::default())
}

pub fn resolve_theme_with_preferences(
    mode: AppThemeMode,
    text_scale: AppTextScale,
) -> DefaultTheme {
    resolve_sized_theme(mode, text_scale, ControlSize::Medium)
}

/// Resolve the theme at a contextual control size, such as compact chrome.
pub fn resolve_sized_theme(
    mode: AppThemeMode,
    text_scale: AppTextScale,
    size: ControlSize,
) -> DefaultTheme {
    // Theme readers run throughout layout and paint, so resolve each finite
    // preference combination only once.
    static THEMES: [[[std::sync::OnceLock<DefaultTheme>; 3]; 3]; 4] =
        [const { [const { [const { std::sync::OnceLock::new() }; 3] }; 3] }; 4];
    let mode_index = match mode {
        AppThemeMode::Light => 0,
        AppThemeMode::Dark => 1,
        AppThemeMode::Void => 2,
        AppThemeMode::System => 3,
    };
    let scale_index = match text_scale {
        AppTextScale::Standard => 0,
        AppTextScale::Large => 1,
        AppTextScale::ExtraLarge => 2,
    };
    let size_index = match size {
        ControlSize::Small => 0,
        ControlSize::Medium => 1,
        ControlSize::Large => 2,
    };
    *THEMES[mode_index][scale_index][size_index].get_or_init(|| {
        let theme = build_theme(mode, text_scale);
        if size == ControlSize::Medium {
            theme
        } else {
            theme.with_size(size)
        }
    })
}

fn build_theme(mode: AppThemeMode, text_scale: AppTextScale) -> DefaultTheme {
    let mut theme = match mode {
        AppThemeMode::Light => DefaultTheme::light(),
        AppThemeMode::Void => DefaultTheme::void(),
        AppThemeMode::Dark | AppThemeMode::System => {
            if mode.is_dark() {
                DefaultTheme::dark()
            } else {
                DefaultTheme::light()
            }
        }
    };
    let factor = text_scale.factor();
    if factor == 1.0 {
        return theme;
    }
    // Scale SUI's own ramp. Control size stays a local layout choice, so the
    // preset's Medium metrics are re-derived from the scaled ramp unchanged.
    let text = &mut theme.text;
    for token in [
        &mut text.xs,
        &mut text.sm,
        &mut text.base,
        &mut text.lg,
        &mut text.xl,
        &mut text._2xl,
        &mut text._3xl,
        &mut text._4xl,
        &mut text._5xl,
        &mut text._6xl,
        &mut text._7xl,
        &mut text._8xl,
        &mut text._9xl,
    ] {
        token.size *= factor;
        token.line_height *= factor;
    }
    theme.with_size(ControlSize::Medium)
}

/// Decorative hues that identify agents, assigned in spawn order. Non-text use only: avatar
/// rings, presence dots, cluster edges, meter channels. Labels always use ink tokens.
const AGENT_HUE_ORDER: [DecorativeHue; 6] = [
    DecorativeHue::Cyan,
    DecorativeHue::Violet,
    DecorativeHue::Magenta,
    DecorativeHue::Amber,
    DecorativeHue::Green,
    DecorativeHue::Blue,
];

/// The six agent identity colors, taken from the theme's decorative palette.
pub fn agent_hues(theme: &DefaultTheme) -> [Color; 6] {
    AGENT_HUE_ORDER.map(|hue| theme.decorative.get(hue).solid)
}

/// A text style at one of the theme's type-ramp tokens.
pub fn token_text_style(
    theme: &DefaultTheme,
    token: ThemeTextToken,
    weight: FontWeight,
    color: Color,
) -> TextStyle {
    TextStyle {
        font_size: token.size,
        line_height: token.line_height,
        weight,
        color,
        ..theme.body_text_style()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui::ThemeShadow;

    #[test]
    fn void_mode_resolves_true_black_theme() {
        let theme = resolve_theme(AppThemeMode::Void);
        assert_eq!(theme.colors.name, "void");
        assert_eq!(theme.surfaces.window, Color::BLACK);
        // Void draws elevation with borders, never shadows.
        assert_eq!(theme.shadows.drop.sm, ThemeShadow::empty());
    }

    #[test]
    fn theme_mode_cycle_includes_void() {
        assert_eq!(AppThemeMode::Dark.next(), AppThemeMode::Void);
        assert_eq!(AppThemeMode::Void.next(), AppThemeMode::System);
        assert_eq!(AppThemeMode::Void.label(), "Void");
    }

    #[test]
    fn void_round_trips_through_serde() {
        let json = serde_json::to_string(&AppThemeMode::Void).expect("serialize");
        assert_eq!(json, "\"void\"");
        let mode: AppThemeMode = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(mode, AppThemeMode::Void);
    }

    #[test]
    fn standard_text_uses_the_stock_presets_unchanged() {
        assert_eq!(resolve_theme(AppThemeMode::Light), DefaultTheme::light());
        assert_eq!(resolve_theme(AppThemeMode::Dark), DefaultTheme::dark());
        assert_eq!(resolve_theme(AppThemeMode::System), DefaultTheme::dark());
        assert_eq!(resolve_theme(AppThemeMode::Void), DefaultTheme::void());
    }

    #[test]
    fn text_scale_multiplies_the_stock_ramp_only() {
        let standard = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::Standard);
        let large = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::Large);

        assert_eq!(standard.text.base, DefaultTheme::dark().text.base);
        assert!(large.text.base.size > standard.text.base.size);
        assert!(large.typography.body_font_size > standard.typography.body_font_size);
        assert_eq!(large.metrics.min_height, standard.metrics.min_height);
        assert_eq!(large.surfaces, standard.surfaces);
    }

    #[test]
    fn agent_hues_come_from_the_decorative_palette() {
        let theme = resolve_theme(AppThemeMode::Dark);
        let hues = agent_hues(&theme);
        assert_eq!(hues[0], theme.decorative.get(DecorativeHue::Cyan).solid);
        for (index, hue) in hues.iter().enumerate() {
            assert!(!hues[index + 1..].contains(hue), "agent hues are distinct");
        }
    }
}
