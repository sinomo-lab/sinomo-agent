//! App theming: the Mesh design language mapped onto SUI's `DefaultTheme`,
//! plus the app-level semantic palette the bespoke widgets paint with.
//!
//! Mesh ships three themes — **Light** (ink on paper), **Dark** (graphite,
//! live glows) and **Void** (true-black OLED: borders instead of shadows,
//! dimmed whites, damped glows). The sui built-in themes carry the exact Mesh
//! role tokens; [`Palette`] re-exports them under the app's names and adds the
//! product-identity tokens (agent hues, mesh decoration tints, glows).

use sui::{Color, ControlSize, DefaultTheme, FontWeight, TextStyle, ThemeShadow};

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
    }
    // Medium is the standard interface size. Smaller and larger metrics are contextual component
    // choices, not an application-wide user preference.
    .with_size(ControlSize::Medium);

    let factor = text_scale.factor();
    // Keep the application on SUI's shared type ramp. Scaling changes the
    // whole ramp while control size remains a local layout choice.
    let sizes = [
        (12.0, 17.0),
        (13.0, 18.0),
        (15.0, 22.0),
        (17.0, 25.0),
        (19.0, 27.0),
        (21.0, 29.0),
        (25.0, 33.0),
        (31.0, 39.0),
        (37.0, 45.0),
        (49.0, 55.0),
        (61.0, 67.0),
        (73.0, 79.0),
        (97.0, 103.0),
    ];
    let token = |(size, line_height): (f32, f32)| sui::ThemeTextToken {
        size: size * factor,
        line_height: line_height * factor,
    };
    theme.text.xs = token(sizes[0]);
    theme.text.sm = token(sizes[1]);
    theme.text.base = token(sizes[2]);
    theme.text.lg = token(sizes[3]);
    theme.text.xl = token(sizes[4]);
    theme.text._2xl = token(sizes[5]);
    theme.text._3xl = token(sizes[6]);
    theme.text._4xl = token(sizes[7]);
    theme.text._5xl = token(sizes[8]);
    theme.text._6xl = token(sizes[9]);
    theme.text._7xl = token(sizes[10]);
    theme.text._8xl = token(sizes[11]);
    theme.text._9xl = token(sizes[12]);
    theme.sync_derived_fields();

    theme.metrics.form_row_label_width = 136.0;
    theme.metrics.form_row_control_width = 520.0;
    theme.metrics.form_section_max_width = 860.0;
    theme
}

/// The six agent identity hues (`--sm-agent-1…6`), shared across themes and
/// assigned in spawn order. Non-text use only: avatar rings, presence dots,
/// cluster edges, meter channels. Labels always use ink tokens.
pub const AGENT_HUES: [Color; 6] = [
    // cyan · core / this agent
    Color::rgba(
        0x29 as f32 / 255.0,
        0xc4 as f32 / 255.0,
        0xde as f32 / 255.0,
        1.0,
    ),
    // violet · voice, secondary
    Color::rgba(
        0x8f as f32 / 255.0,
        0x7d as f32 / 255.0,
        0xf8 as f32 / 255.0,
        1.0,
    ),
    // magenta
    Color::rgba(
        0xe7 as f32 / 255.0,
        0x67 as f32 / 255.0,
        0xb5 as f32 / 255.0,
        1.0,
    ),
    // amber
    Color::rgba(
        0xeb as f32 / 255.0,
        0xa2 as f32 / 255.0,
        0x3f as f32 / 255.0,
        1.0,
    ),
    // green
    Color::rgba(
        0x43 as f32 / 255.0,
        0xc9 as f32 / 255.0,
        0x83 as f32 / 255.0,
        1.0,
    ),
    // blue
    Color::rgba(
        0x5b as f32 / 255.0,
        0x8d as f32 / 255.0,
        0xef as f32 / 255.0,
        1.0,
    ),
];

/// The Mesh semantic palette used by the bespoke chrome (title bar, sidebars,
/// transcript, and cluster graph) so it stays coherent with the
/// composed sui widgets across all three themes.
#[derive(Clone, Copy)]
pub struct Palette {
    pub dark: bool,
    /// Window background (`--sm-bg`).
    pub window: Color,
    /// Recessed background tier (`--sm-bg-subtle`): sidebars, table headers.
    pub bg_subtle: Color,
    /// Card / panel surface (`--sm-surface`). In Void this stays true black —
    /// the border is the card.
    pub panel: Color,
    /// Nested/raised fill one step above the panel (`--sm-surface-2`).
    pub surface_2: Color,
    /// Strongest neutral fill: hover wells, tracks (`--sm-surface-3`).
    pub surface_3: Color,
    /// Floating surface for menus, dialogs, toasts (`--sm-overlay`).
    pub overlay: Color,
    /// Sidebar / secondary chrome surface (bg-subtle tier).
    pub sidebar: Color,
    /// Title-bar surface (bg-subtle tier).
    pub titlebar: Color,
    /// Hairline separators and borders (`--sm-border`).
    pub border: Color,
    /// Stronger border: focused/active/overlay edges (`--sm-border-strong`).
    pub border_strong: Color,
    /// In-component hairlines (`--sm-border-subtle`).
    pub border_subtle: Color,
    /// Primary text (`--sm-text`).
    pub text: Color,
    /// Secondary text (`--sm-text-2`).
    pub text_muted: Color,
    /// Tertiary text: meta, labels (`--sm-text-3`).
    pub text_faint: Color,
    /// Disabled content (`--sm-text-disabled`).
    pub text_disabled: Color,
    /// Content on inverted surfaces (`--sm-text-invert`).
    pub text_invert: Color,
    /// Accent (signal cyan) fill (`--sm-accent`).
    pub accent: Color,
    /// Accent, hover state (`--sm-accent-hover`).
    pub accent_hover: Color,
    /// Text drawn on top of the accent color (`--sm-on-accent`).
    pub on_accent: Color,
    /// Accent-hued text legible on plain surfaces (`--sm-accent-text`).
    pub accent_text: Color,
    /// Translucent accent wash: selected rows, badges (`--sm-accent-soft`).
    pub accent_soft: Color,
    /// Translucent accent border (`--sm-accent-border`).
    pub accent_border: Color,
    /// Dedicated keyboard-focus ring color (`--sm-focus`).
    pub focus: Color,
    /// Hovered row/control tint.
    pub hover: Color,
    /// Selected row fill (`--sm-selection`).
    pub selected: Color,
    /// Inset field background (inputs, code) (`--sm-field-bg`).
    pub field: Color,
    /// Modal scrim (`--sm-scrim`).
    pub scrim: Color,
    /// Positive / online status, solid (`--sm-ok`).
    pub good: Color,
    /// Positive status text on plain/soft surfaces (`--sm-ok-text`).
    pub ok_text: Color,
    /// Translucent positive wash (`--sm-ok-soft`).
    pub ok_soft: Color,
    /// Warning status, solid (`--sm-warn`).
    pub warn: Color,
    /// Warning text on plain/soft surfaces (`--sm-warn-text`).
    pub warn_text: Color,
    /// Translucent warning wash (`--sm-warn-soft`).
    pub warn_soft: Color,
    /// Error / offline status, solid (`--sm-danger`).
    pub bad: Color,
    /// Danger text on plain/soft surfaces (`--sm-danger-text`).
    pub danger_text: Color,
    /// Translucent danger wash (`--sm-danger-soft`).
    pub danger_soft: Color,
    /// Danger fill, hover state (`--sm-danger-hover`).
    pub danger_hover: Color,
    /// Text drawn on top of the danger color (`--sm-on-danger`).
    pub on_danger: Color,
    /// Informational status, solid (`--sm-info`).
    pub info: Color,
    /// Informational text on plain/soft surfaces (`--sm-info-text`).
    pub info_text: Color,
    /// Translucent informational wash (`--sm-info-soft`).
    pub info_soft: Color,
    /// Ambient dot-lattice tint (`--sm-mesh-dot`).
    pub mesh_dot: Color,
    /// Ambient hairline tint (`--sm-mesh-line`).
    pub mesh_line: Color,
    /// Accent glow for live signals (`--sm-glow-accent`); empty in Light.
    pub glow_accent: ThemeShadow,
    /// Voice/duplex glow (`--sm-glow-voice`); empty in Light.
    pub glow_voice: ThemeShadow,
    /// The six agent identity hues, in spawn order.
    pub agent: [Color; 6],
}

impl Palette {
    /// Adapt SUI's Mesh surface palette to the app's semantic names.
    pub fn from_theme(theme: &DefaultTheme, dark: bool) -> Self {
        let surfaces = &theme.surfaces;
        let controls = &theme.palette;
        let (mesh_dot, mesh_line) = mesh_decoration(theme);
        Palette {
            dark: surfaces.dark || dark,
            window: surfaces.window,
            bg_subtle: surfaces.window_subtle,
            panel: surfaces.panel,
            surface_2: surfaces.surface_2,
            surface_3: surfaces.surface_3,
            overlay: surfaces.overlay,
            sidebar: surfaces.sidebar,
            titlebar: surfaces.titlebar,
            border: surfaces.border,
            border_strong: surfaces.border_strong,
            border_subtle: surfaces.border_subtle,
            text: surfaces.text,
            text_muted: surfaces.text_muted,
            text_faint: surfaces.text_faint,
            text_disabled: surfaces.text_disabled,
            text_invert: surfaces.text_invert,
            accent: surfaces.accent,
            accent_hover: surfaces.accent_hover,
            on_accent: surfaces.on_accent,
            accent_text: surfaces.accent_text,
            accent_soft: surfaces.accent_soft,
            accent_border: surfaces.accent_border,
            focus: surfaces.focus,
            hover: surfaces.hover,
            selected: surfaces.selected,
            field: surfaces.field,
            scrim: surfaces.overlay_scrim,
            good: surfaces.good,
            ok_text: surfaces.good_text,
            ok_soft: surfaces.good_soft,
            warn: surfaces.warn,
            warn_text: surfaces.warn_text,
            warn_soft: surfaces.warn_soft,
            bad: surfaces.bad,
            danger_text: surfaces.bad_text,
            danger_soft: surfaces.bad_soft,
            danger_hover: controls.danger_hover,
            on_danger: controls.danger_text,
            info: surfaces.info,
            info_text: surfaces.info_text,
            info_soft: surfaces.info_soft,
            mesh_dot,
            mesh_line,
            glow_accent: theme.glows.accent,
            glow_voice: theme.glows.secondary,
            agent: AGENT_HUES,
        }
    }

    /// Body text style in the primary text color.
    pub fn body(&self, theme: &DefaultTheme) -> TextStyle {
        TextStyle {
            color: self.text,
            ..theme.body_text_style()
        }
    }

    /// Resolve legacy authored sizes onto the shared SUI typography ramp.
    ///
    /// Custom visualization code still describes the intended hierarchy with a
    /// nominal size, but the rendered style always uses an exact theme token so
    /// it shares the same line grid and user text scaling as composed widgets.
    pub fn text_style(
        &self,
        theme: &DefaultTheme,
        size: f32,
        weight: FontWeight,
        color: Color,
    ) -> TextStyle {
        let token = if size <= 12.0 {
            theme.text.xs
        } else if size <= 14.0 {
            theme.text.sm
        } else if size <= 16.0 {
            theme.text.base
        } else if size <= 18.0 {
            theme.text.lg
        } else if size <= 20.0 {
            theme.text.xl
        } else if size <= 23.0 {
            theme.text._2xl
        } else if size <= 28.0 {
            theme.text._3xl
        } else {
            theme.text._4xl
        };
        TextStyle {
            font_size: token.size,
            line_height: token.line_height,
            weight,
            color,
            ..theme.body_text_style()
        }
    }
}

/// Ambient mesh decoration tints per theme (`--sm-mesh-dot` / `--sm-mesh-line`).
/// Budget: one motif per view, never louder than content. Flattened onto the
/// window color with CSS-space compositing so the linear-space renderer paints
/// them at their authored whisper level.
fn mesh_decoration(theme: &DefaultTheme) -> (Color, Color) {
    let window = theme.surfaces.window;
    let (dot, line) = match theme.colors.name {
        "dark" => (
            Color::rgba(167.0 / 255.0, 184.0 / 255.0, 214.0 / 255.0, 0.14),
            Color::rgba(167.0 / 255.0, 184.0 / 255.0, 214.0 / 255.0, 0.08),
        ),
        "void" => (
            Color::rgba(158.0 / 255.0, 175.0 / 255.0, 205.0 / 255.0, 0.12),
            Color::rgba(158.0 / 255.0, 175.0 / 255.0, 205.0 / 255.0, 0.07),
        ),
        _ => (
            Color::rgba(13.0 / 255.0, 18.0 / 255.0, 32.0 / 255.0, 0.16),
            Color::rgba(13.0 / 255.0, 18.0 / 255.0, 32.0 / 255.0, 0.07),
        ),
    };
    (dot.over(window), line.over(window))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn palette_exposes_mesh_status_triads() {
        let theme = resolve_theme(AppThemeMode::Dark);
        let pal = Palette::from_theme(&theme, true);

        // Solid, soft and text roles are distinct for every status hue. The
        // soft washes arrive pre-flattened (opaque) so the linear-space
        // renderer paints them at their CSS-authored weight.
        assert_ne!(pal.warn_soft, pal.warn);
        assert_eq!(pal.warn_soft.alpha, 1.0);
        assert_ne!(pal.ok_text, pal.good);
        assert_ne!(pal.danger_soft, pal.bad);
        assert_eq!(pal.accent_soft.alpha, 1.0);
        assert_eq!(pal.selected.alpha, 1.0);
        // Glows exist in dark and carry the accent hue.
        assert!(pal.glow_accent.first.is_some());

        let light = resolve_theme(AppThemeMode::Light);
        let light_pal = Palette::from_theme(&light, false);
        // Light does not glow on paper.
        assert!(light_pal.glow_accent.first.is_none());
        assert!(!light_pal.dark);
    }

    #[test]
    fn app_typography_has_a_readable_floor_and_independent_scaling() {
        let standard = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::Standard);
        let large_text = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::Large);

        assert_eq!(standard.text.xs.size, 12.0);
        assert_eq!(standard.text.base.size, 15.0);
        assert!(large_text.text.base.size > standard.text.base.size);
        assert_eq!(large_text.metrics.min_height, standard.metrics.min_height);
        assert_eq!(standard.metrics.min_height, 32.0);
        assert!(standard.metrics.form_section_max_width >= 860.0);

        let palette = Palette::from_theme(&standard, true);
        assert!(
            palette
                .text_style(&standard, 9.0, FontWeight::NORMAL, palette.text)
                .font_size
                >= standard.text.xs.size
        );
    }
}
