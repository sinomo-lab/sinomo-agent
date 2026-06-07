//! App theming: a thin layer over SUI's `DefaultTheme` plus a few semantic colors
//! the bespoke widgets paint with (top bar, sidebars, grouped settings rows).

use sui::{Color, DefaultTheme, FontWeight, TextStyle};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppThemeMode {
    Light,
    Dark,
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
    /// Derive the app's chrome palette from the toolkit's resolved [`ControlPalette`]
    /// (`theme.palette`) and color roles (`theme.colors`), so the bespoke chrome stays on the
    /// exact same, scheme-tuned color system as the `sui-widgets` the tabs are built from. Surfaces,
    /// text, accent and status come straight from the theme (this is what makes light mode legible);
    /// hairlines, hover and selection stay as subtle translucent overlays so the flat chrome reads
    /// well on any surface.
    ///
    // TODO(ui-polish): re-verify surface/border values against captures once the renderer's shadow
    // primitive lands — pure-black `base_100` in dark mode wants elevation under it.
    pub fn from_theme(theme: &DefaultTheme, dark: bool) -> Self {
        let cp = &theme.palette;
        let colors = &theme.colors;
        let line = if dark { 0.10 } else { 0.12 };
        Palette {
            dark,
            window: cp.surface,
            panel: cp.surface_raised,
            sidebar: cp.surface,
            titlebar: cp.surface_raised,
            border: with_alpha(cp.text, line),
            border_strong: with_alpha(cp.text, line * 2.0),
            text: cp.text,
            // Derive muted/faint from full text rather than the toolkit's roles: the theme fades
            // muted text far toward the surface (fine on a control, too low-contrast for the chrome's
            // heavy use of secondary text — especially in light mode on a near-white surface).
            text_muted: mix(cp.text, cp.surface, 0.34),
            text_faint: mix(cp.text, cp.surface, 0.52),
            accent: cp.accent,
            accent_hover: cp.accent_hover,
            on_accent: cp.accent_text,
            hover: with_alpha(cp.text, if dark { 0.06 } else { 0.05 }),
            selected: cp.selection,
            field: cp.control,
            good: colors.success,
            warn: colors.warning,
            bad: colors.error,
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

/// Override a color's alpha, preserving its RGB — used for translucent hairlines, hover and
/// selection overlays that sit on top of any surface.
fn with_alpha(c: Color, a: f32) -> Color {
    Color::rgba(c.red, c.green, c.blue, a)
}

/// Linear blend of `a` toward `b` by `t` (0 = a, 1 = b), preserving a's alpha. Used to derive
/// contrast-tuned muted/faint text from the theme's text + surface colors.
fn mix(a: Color, b: Color, t: f32) -> Color {
    let t = t.clamp(0.0, 1.0);
    Color::rgba(
        a.red + (b.red - a.red) * t,
        a.green + (b.green - a.green) * t,
        a.blue + (b.blue - a.blue) * t,
        a.alpha,
    )
}
