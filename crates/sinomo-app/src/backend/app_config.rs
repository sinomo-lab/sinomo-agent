//! Local app-only UI configuration.
//!
//! These settings are intentionally not stored through the cluster config
//! service. They describe this app instance's presentation preferences.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sinomo_runtime::local::SinomoDataDir;

use crate::rendering::AppRenderMode;
use crate::theme::{AppTextScale, AppThemeMode};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppUiConfig {
    #[serde(default)]
    pub theme: AppThemeMode,
    #[serde(default = "default_render_mode")]
    pub render_mode: AppRenderMode,
    #[serde(default)]
    pub text_scale: AppTextScale,
    /// Device selectors are local to the app host because that node owns the
    /// physical microphone, speaker, and camera.
    #[serde(default)]
    pub media: AppMediaPreferences,
}

impl Default for AppUiConfig {
    fn default() -> Self {
        Self {
            theme: AppThemeMode::System,
            render_mode: default_render_mode(),
            text_scale: AppTextScale::default(),
            media: AppMediaPreferences::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppMediaPreferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub microphone: Option<RememberedNativeDevice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaker: Option<RememberedNativeDevice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub camera: Option<RememberedNativeDevice>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RememberedNativeDevice {
    pub native_id: String,
    pub display_name: String,
}

impl RememberedNativeDevice {
    pub fn new(native_id: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            native_id: native_id.into(),
            display_name: display_name.into(),
        }
    }
}

pub fn default_render_mode() -> AppRenderMode {
    if cfg!(debug_assertions) {
        AppRenderMode::ForceSdr
    } else {
        AppRenderMode::Automatic
    }
}

pub fn default_data_dir() -> SinomoDataDir {
    SinomoDataDir::from_env_or_home()
        .unwrap_or_else(|_| SinomoDataDir::new(std::env::temp_dir().join("sinomo")))
}

pub fn ui_config_file(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.shared_configs_dir().join("app").join("ui.json")
}

pub fn load_ui_config(data_dir: &SinomoDataDir) -> AppUiConfig {
    load_ui_config_from_path(&ui_config_file(data_dir))
}

pub fn load_theme(data_dir: &SinomoDataDir) -> AppThemeMode {
    load_ui_config(data_dir).theme
}

pub fn load_render_mode(data_dir: &SinomoDataDir) -> AppRenderMode {
    load_ui_config(data_dir).render_mode
}

pub fn load_text_scale(data_dir: &SinomoDataDir) -> AppTextScale {
    load_ui_config(data_dir).text_scale
}

pub fn load_media_preferences(data_dir: &SinomoDataDir) -> AppMediaPreferences {
    load_ui_config(data_dir).media
}

pub fn save_theme(data_dir: &SinomoDataDir, theme: AppThemeMode) -> Result<(), String> {
    let mut config = load_ui_config(data_dir);
    config.theme = theme;
    save_ui_config(data_dir, &config)
}

pub fn save_render_mode(
    data_dir: &SinomoDataDir,
    render_mode: AppRenderMode,
) -> Result<(), String> {
    let mut config = load_ui_config(data_dir);
    config.render_mode = render_mode;
    save_ui_config(data_dir, &config)
}

pub fn save_text_scale(data_dir: &SinomoDataDir, text_scale: AppTextScale) -> Result<(), String> {
    let mut config = load_ui_config(data_dir);
    config.text_scale = text_scale;
    save_ui_config(data_dir, &config)
}

pub fn save_media_preferences(
    data_dir: &SinomoDataDir,
    media: &AppMediaPreferences,
) -> Result<(), String> {
    let mut config = load_ui_config(data_dir);
    config.media = media.clone();
    save_ui_config(data_dir, &config)
}

fn load_ui_config_from_path(path: &Path) -> AppUiConfig {
    let Ok(text) = fs::read_to_string(path) else {
        return AppUiConfig::default();
    };
    serde_json::from_str(&text).unwrap_or_default()
}

fn save_ui_config(data_dir: &SinomoDataDir, config: &AppUiConfig) -> Result<(), String> {
    let path = ui_config_file(data_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(config).map_err(|error| error.to_string())? + "\n";
    fs::write(&path, text).map_err(|error| format!("write {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_data_dir(slug: &str) -> SinomoDataDir {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time is after unix epoch")
            .as_nanos();
        SinomoDataDir::new(std::env::temp_dir().join(format!(
            "sinomo-app-config-{slug}-{}-{unique}",
            std::process::id()
        )))
    }

    #[test]
    fn ui_config_file_lives_under_configs_app() {
        let data_dir = SinomoDataDir::new("root");

        assert_eq!(
            ui_config_file(&data_dir),
            PathBuf::from("root")
                .join("configs")
                .join("app")
                .join("ui.json")
        );
    }

    #[test]
    fn missing_ui_config_defaults_to_system_theme() {
        let data_dir = temp_data_dir("missing");

        assert_eq!(load_ui_config(&data_dir), AppUiConfig::default());
        assert_eq!(load_theme(&data_dir), AppThemeMode::System);
        assert_eq!(load_text_scale(&data_dir), AppTextScale::Standard);
    }

    #[test]
    fn missing_ui_config_uses_debug_capture_render_default() {
        let data_dir = temp_data_dir("missing-render");

        assert_eq!(load_render_mode(&data_dir), default_render_mode());
        if cfg!(debug_assertions) {
            assert_eq!(load_render_mode(&data_dir), AppRenderMode::ForceSdr);
        } else {
            assert_eq!(load_render_mode(&data_dir), AppRenderMode::Automatic);
        }
    }

    #[test]
    fn save_theme_round_trips_local_ui_config() {
        let data_dir = temp_data_dir("round-trip");

        save_theme(&data_dir, AppThemeMode::Light).expect("save theme");

        assert_eq!(load_theme(&data_dir), AppThemeMode::Light);
        let saved = fs::read_to_string(ui_config_file(&data_dir)).expect("read saved config");
        assert!(saved.contains("\"theme\": \"light\""));
    }

    #[test]
    fn save_render_mode_round_trips_local_ui_config() {
        let data_dir = temp_data_dir("render-round-trip");

        save_render_mode(&data_dir, AppRenderMode::ForceSdr).expect("save render mode");

        assert_eq!(load_render_mode(&data_dir), AppRenderMode::ForceSdr);
        let saved = fs::read_to_string(ui_config_file(&data_dir)).expect("read saved config");
        assert!(saved.contains("\"render_mode\": \"force_sdr\""));
    }

    #[test]
    fn text_scale_round_trips_local_ui_config() {
        let data_dir = temp_data_dir("scale-round-trip");

        save_text_scale(&data_dir, AppTextScale::ExtraLarge).expect("save text scale");

        assert_eq!(load_text_scale(&data_dir), AppTextScale::ExtraLarge);
        let saved = fs::read_to_string(ui_config_file(&data_dir)).expect("read saved config");
        assert!(saved.contains("\"text_scale\": \"extra_large\""));
    }

    #[test]
    fn legacy_ui_config_defaults_media_devices_to_system_defaults() {
        let data_dir = temp_data_dir("legacy-media");
        let path = ui_config_file(&data_dir);
        fs::create_dir_all(path.parent().expect("config parent")).expect("create config parent");
        fs::write(
            &path,
            serde_json::to_string_pretty(&serde_json::json!({
                "theme": "dark",
                "render_mode": default_render_mode(),
            }))
            .expect("serialize legacy config"),
        )
        .expect("write legacy config");

        let loaded = load_ui_config(&data_dir);

        assert_eq!(loaded.theme, AppThemeMode::Dark);
        assert_eq!(loaded.text_scale, AppTextScale::Standard);
        assert_eq!(loaded.media, AppMediaPreferences::default());
    }

    #[test]
    fn save_media_preferences_round_trips_without_losing_ui_preferences() {
        let data_dir = temp_data_dir("media-round-trip");
        save_theme(&data_dir, AppThemeMode::Light).expect("save theme");
        save_render_mode(&data_dir, AppRenderMode::PreferWideGamut).expect("save render mode");
        let media = AppMediaPreferences {
            microphone: Some(RememberedNativeDevice::new(
                "cpal:input:1:desk-mic",
                "Desk microphone",
            )),
            speaker: Some(RememberedNativeDevice::new(
                "cpal:output:2:headphones",
                "Headphones",
            )),
            camera: Some(RememberedNativeDevice::new("camera:front", "Front camera")),
        };

        save_media_preferences(&data_dir, &media).expect("save media preferences");

        assert_eq!(load_media_preferences(&data_dir), media);
        assert_eq!(load_theme(&data_dir), AppThemeMode::Light);
        assert_eq!(load_render_mode(&data_dir), AppRenderMode::PreferWideGamut);
    }

    #[test]
    fn clearing_one_media_preference_preserves_the_other_devices() {
        let data_dir = temp_data_dir("media-clear-one");
        let mut media = AppMediaPreferences {
            microphone: Some(RememberedNativeDevice::new("mic", "Microphone")),
            speaker: Some(RememberedNativeDevice::new("speaker", "Speaker")),
            camera: Some(RememberedNativeDevice::new("camera", "Camera")),
        };
        save_media_preferences(&data_dir, &media).expect("save media preferences");

        media.microphone = None;
        save_media_preferences(&data_dir, &media).expect("clear microphone preference");

        let loaded = load_media_preferences(&data_dir);
        assert_eq!(loaded.microphone, None);
        assert_eq!(loaded.speaker, media.speaker);
        assert_eq!(loaded.camera, media.camera);
    }
}
