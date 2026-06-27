//! Local app-only UI configuration.
//!
//! These settings are intentionally not stored through the cluster config
//! service. They describe this app instance's presentation preferences.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sinomo_runtime::local::SinomoDataDir;

use crate::rendering::AppRenderMode;
use crate::theme::AppThemeMode;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppUiConfig {
    #[serde(default)]
    pub theme: AppThemeMode,
    #[serde(default = "default_render_mode")]
    pub render_mode: AppRenderMode,
}

impl Default for AppUiConfig {
    fn default() -> Self {
        Self {
            theme: AppThemeMode::System,
            render_mode: default_render_mode(),
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
}
