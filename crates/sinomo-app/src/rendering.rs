//! Local window-rendering preferences for the SUI desktop surface.

use sui::{WindowColorManagementMode, WindowRenderOptions, WindowStemDarkening, WindowTextHinting};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppRenderMode {
    #[default]
    Automatic,
    ForceSdr,
    PreferWideGamut,
    PreferHdr,
}

impl AppRenderMode {
    pub fn label(self) -> &'static str {
        match self {
            AppRenderMode::Automatic => "Automatic",
            AppRenderMode::ForceSdr => "Force SDR",
            AppRenderMode::PreferWideGamut => "Wide gamut",
            AppRenderMode::PreferHdr => "Prefer HDR",
        }
    }

    pub fn color_management_mode(self) -> WindowColorManagementMode {
        match self {
            AppRenderMode::Automatic => WindowColorManagementMode::Automatic,
            AppRenderMode::ForceSdr => WindowColorManagementMode::ForceSdr,
            AppRenderMode::PreferWideGamut => WindowColorManagementMode::PreferWideGamut,
            AppRenderMode::PreferHdr => WindowColorManagementMode::PreferHdr,
        }
    }
}

/// Use the same text rendering policy as the SUI widget book. Forced hinting
/// and stem darkening make adjacent app labels look uneven on some Windows
/// displays.
pub fn resolve_render_options(mode: AppRenderMode) -> WindowRenderOptions {
    WindowRenderOptions::new(true, 1.0)
        .with_text_hinting(WindowTextHinting::None)
        .with_stem_darkening(WindowStemDarkening::None)
        .with_color_management_mode(mode.color_management_mode())
}
