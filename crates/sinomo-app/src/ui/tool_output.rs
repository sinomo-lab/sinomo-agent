//! Presentation dispatch for files in tool-call output details.

use sinomo_client::chat_contract::ChatToolOutputFile;
use sui::{Alignment, Button, Flex, FlexItem, Label, UiHandle};

use super::files::FilesSourceKind;
use super::image_preview::ImageFilePreview;
use crate::backend::sifs::SifsDaemonAccess;
use crate::state::Shared;

pub(super) fn supports_image_preview(file: &ChatToolOutputFile) -> bool {
    file.error.is_none()
        && matches!(
            file.media_type.as_deref(),
            Some("image/png" | "image/jpeg" | "image/webp")
        )
}

pub(super) fn preview_height(file: &ChatToolOutputFile) -> f32 {
    if supports_image_preview(file) {
        400.0
    } else {
        110.0
    }
}

pub(super) fn file_view(
    shared: &Shared,
    file: &ChatToolOutputFile,
    access: Option<SifsDaemonAccess>,
    waker: Option<UiHandle>,
) -> Flex {
    file_view_for_source(
        shared,
        file,
        FilesSourceKind::Sifs,
        access,
        waker,
        true,
        supports_image_preview(file),
    )
}

/// Markdown references point at existing files; do not imply an unknown byte count is zero.
pub(super) fn reference_view(
    shared: &Shared,
    file: &ChatToolOutputFile,
    source: FilesSourceKind,
    access: Option<SifsDaemonAccess>,
    waker: Option<UiHandle>,
    image: bool,
) -> Flex {
    file_view_for_source(
        shared,
        file,
        source,
        access,
        waker,
        false,
        image || supports_image_preview(file),
    )
}

fn file_view_for_source(
    shared: &Shared,
    file: &ChatToolOutputFile,
    source: FilesSourceKind,
    access: Option<SifsDaemonAccess>,
    waker: Option<UiHandle>,
    known_size: bool,
    image: bool,
) -> Flex {
    let mut body = Flex::vertical()
        .main_gap(6.0)
        .align_items(Alignment::Stretch);
    if image {
        body = body.with_child(ImageFilePreview::with_source(
            file.clone(),
            source,
            access,
            waker,
        ));
    }
    let size = if file.size_bytes >= 1024 * 1024 {
        format!("{:.1} MiB", file.size_bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{} KiB", file.size_bytes.div_ceil(1024))
    };
    let description = file
        .error
        .as_ref()
        .map(|error| format!("Preview unavailable: {error}"))
        .unwrap_or_else(|| {
            let mut description = format!(
                "{} · {}",
                file.file_name,
                file.media_type.as_deref().unwrap_or("File")
            );
            if known_size {
                description.push_str(&format!(" · {size}"));
            }
            description
        });
    let open = shared.clone();
    let theme = shared.clone();
    let path = file.path.clone();
    let name = file.file_name.clone();
    body = body
        .with_child(Label::new(&file.label))
        .with_child(Label::new(description))
        .with_item(
            Button::new("Open file")
                .enabled(file.error.is_none())
                .theme_when(move || theme.theme())
                .on_press_with_ctx(move |ctx| {
                    open.open_document_tab(source, path.clone(), name.clone());
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                    ctx.request_semantics();
                }),
            FlexItem::new().align_self(Alignment::Start),
        );
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_output_preview_dispatch_preserves_other_media_as_files() {
        for media_type in [
            "image/png",
            "image/jpeg",
            "image/webp",
            "audio/wav",
            "video/mp4",
            "model/gltf-binary",
            "application/pdf",
        ] {
            let file = ChatToolOutputFile {
                media_type: Some(media_type.into()),
                ..Default::default()
            };
            assert_eq!(
                supports_image_preview(&file),
                media_type.starts_with("image/")
            );
        }
        assert!(!supports_image_preview(&ChatToolOutputFile::default()));
    }
}
