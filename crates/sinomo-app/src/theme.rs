//! App theming: a thin layer over SUI's `DefaultTheme` plus a few semantic colors
//! the bespoke widgets paint with (top bar, sidebars, grouped settings rows).

use sui::{Color, DefaultTheme, FontWeight, TextStyle};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AppThemeMode {
    Light,
    Dark,
    #[default]
    System,
}

impl AppThemeMode {
    pub fn label(self) -> &'static str {
        match self {
            AppThemeMode::Light => "Light",
            AppThemeMode::Dark => "Dark",
            AppThemeMode::System => "System",
        }
    }

    pub fn next(self) -> Self {
        match self {
            AppThemeMode::Light => AppThemeMode::Dark,
            AppThemeMode::Dark => AppThemeMode::System,
            AppThemeMode::System => AppThemeMode::Light,
        }
    }

    fn is_dark(self) -> bool {
        match self {
            AppThemeMode::Light => false,
            AppThemeMode::Dark => true,
            // No OS hook wired yet; default the "System" choice to dark, which
            // suits the product's look.
            AppThemeMode::System => true,
        }
    }
}

/// Resolve the concrete widget theme for a mode.
pub fn resolve_theme(mode: AppThemeMode) -> DefaultTheme {
    if mode.is_dark() {
        DefaultTheme::dark()
    } else {
        DefaultTheme::light()
    }
}

/// Extra semantic colors derived from the widget palette, used by the bespoke
/// chrome (title bar, sidebar, grouped lists) so it stays coherent across modes.
#[derive(Clone, Copy)]
pub struct Palette {
    pub dark: bool,
    /// Window background.
    pub window: Color,
    /// Raised panel / card surface.
    pub panel: Color,
    /// Sidebar / secondary surface.
    pub sidebar: Color,
    /// Title-bar surface.
    pub titlebar: Color,
    /// Hairline separators and borders.
    pub border: Color,
    /// Stronger border (focused/active).
    pub border_strong: Color,
    /// Primary text.
    pub text: Color,
    /// Secondary / muted text.
    pub text_muted: Color,
    /// Faint text (timestamps, hints).
    pub text_faint: Color,
    /// Accent (brand) color.
    pub accent: Color,
    /// Accent, hover state.
    pub accent_hover: Color,
    /// Text drawn on top of the accent color.
    pub on_accent: Color,
    /// Hovered row/control fill.
    pub hover: Color,
    /// Selected row fill.
    pub selected: Color,
    /// Inset field background (inputs, code).
    pub field: Color,
    /// Positive / online status.
    pub good: Color,
    /// Warning status.
    pub warn: Color,
    /// Error / offline status.
    pub bad: Color,
}

impl Palette {
    /// Adapt SUI's reusable surface palette to the app's existing chrome names.
    pub fn from_theme(theme: &DefaultTheme, dark: bool) -> Self {
        let surfaces = &theme.surfaces;
        Palette {
            dark: surfaces.dark || dark,
            window: surfaces.window,
            panel: surfaces.panel,
            sidebar: surfaces.sidebar,
            titlebar: surfaces.titlebar,
            border: surfaces.border,
            border_strong: surfaces.border_strong,
            text: surfaces.text,
            text_muted: surfaces.text_muted,
            text_faint: surfaces.text_faint,
            accent: surfaces.accent,
            accent_hover: surfaces.accent_hover,
            on_accent: surfaces.on_accent,
            hover: surfaces.hover,
            selected: surfaces.selected,
            field: surfaces.field,
            good: surfaces.good,
            warn: surfaces.warn,
            bad: surfaces.bad,
        }
    }

    /// Body text style in the primary text color.
    pub fn body(&self, theme: &DefaultTheme) -> TextStyle {
        TextStyle {
            color: self.text,
            ..theme.body_text_style()
        }
    }

    /// A text style with explicit size/weight/color, inheriting font metrics.
    pub fn text_style(
        &self,
        theme: &DefaultTheme,
        size: f32,
        weight: FontWeight,
        color: Color,
    ) -> TextStyle {
        TextStyle {
            font_size: size,
            line_height: (size * 1.32).round(),
            weight,
            color,
            ..theme.body_text_style()
        }
    }
}
