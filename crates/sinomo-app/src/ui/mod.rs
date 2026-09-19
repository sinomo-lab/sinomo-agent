//! All widgets and per-tab views.

mod chat_render;
mod files_thumbnail;
#[cfg(test)]
mod hover_tests;
mod module_widget;

pub mod chat;
pub mod cluster;
pub mod evolution;
pub mod files;
pub mod media_setup;
mod pinned_tab;
pub mod settings;
pub mod shell;
pub mod widgets;
mod workspace;

pub use shell::Shell;
