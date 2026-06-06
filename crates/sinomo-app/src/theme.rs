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
    pub fn from_theme(theme: &DefaultTheme, dark: bool) -> Self {
        let p = &theme.palette;
        let accent = theme.colors.primary;
        if dark {
            Palette {
                dark,
                window: gray(0.086),
                panel: gray(0.122),
                sidebar: gray(0.102),
                titlebar: gray(0.143),
                border: rgba(1.0, 1.0, 1.0, 0.08),
                border_strong: rgba(1.0, 1.0, 1.0, 0.16),
                text: gray(0.94),
                text_muted: gray(0.64),
                text_faint: gray(0.46),
                accent,
                accent_hover: mix(accent, gray(1.0), 0.14),
                on_accent: theme.colors.primary_content,
                hover: rgba(1.0, 1.0, 1.0, 0.05),
                selected: with_alpha(accent, 0.22),
                field: gray(0.16),
                good: rgba(0.30, 0.81, 0.46, 1.0),
                warn: rgba(0.98, 0.74, 0.20, 1.0),
                bad: rgba(0.97, 0.37, 0.37, 1.0),
            }
        } else {
            Palette {
                dark,
                window: gray(0.93),
                panel: gray(0.997),
                sidebar: gray(0.955),
                titlebar: gray(0.98),
                border: rgba(0.0, 0.0, 0.0, 0.13),
                border_strong: rgba(0.0, 0.0, 0.0, 0.24),
                text: gray(0.10),
                text_muted: gray(0.36),
                text_faint: gray(0.55),
                accent,
                accent_hover: mix(accent, gray(0.0), 0.10),
                on_accent: gray(1.0),
                hover: rgba(0.0, 0.0, 0.0, 0.05),
                selected: with_alpha(accent, 0.16),
                field: gray(0.965),
                good: rgba(0.13, 0.55, 0.27, 1.0),
                warn: rgba(0.78, 0.52, 0.05, 1.0),
                bad: rgba(0.80, 0.18, 0.18, 1.0),
            }
        }
        .with_fallback_text(p.text, p.text_muted)
    }

    fn with_fallback_text(mut self, _text: Color, _muted: Color) -> Self {
        // Reserved hook: we currently prefer our own neutral ramp over the raw
        // palette text colors for a flatter, product-grade look.
        let _ = &mut self;
        self
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

fn gray(v: f32) -> Color {
    Color::rgba(v, v, v, 1.0)
}

fn rgba(r: f32, g: f32, b: f32, a: f32) -> Color {
    Color::rgba(r, g, b, a)
}

fn with_alpha(c: Color, a: f32) -> Color {
    Color::rgba(c.red, c.green, c.blue, a)
}

/// Linear blend of `a` toward `b` by `t` (0 = a, 1 = b), preserving a's alpha.
fn mix(a: Color, b: Color, t: f32) -> Color {
    let t = t.clamp(0.0, 1.0);
    Color::rgba(
        a.red + (b.red - a.red) * t,
        a.green + (b.green - a.green) * t,
        a.blue + (b.blue - a.blue) * t,
        a.alpha,
    )
}
