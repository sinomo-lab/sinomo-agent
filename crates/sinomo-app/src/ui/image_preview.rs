//! Lazy, bounded previews of existing local and SIFS image files.

use std::sync::{Arc, Condvar, Mutex};

use sinomo_runtime::api::app::chat::ChatToolOutputFile;
use sui::{
    ArrangeCtx, Command, CommandKey, Constraints, EventCtx, ImageSource, Label, MeasureCtx,
    PaintCtx, Rect, RegisteredImage, SemanticsCtx, SemanticsNode, SemanticsRole, Size, UiHandle,
    Widget, WidgetPod,
};

use super::files::{FilesSourceKind, read_local_preview_bytes};
use super::files_thumbnail::{ThumbnailBitmap, decode_raster_preview};
use crate::backend::sifs::SifsDaemonAccess;

const IMAGE_READY: CommandKey<()> = CommandKey::new("sinomo.chat.image-preview.ready");

// A response can reference many images. Bound reads and decoder allocations across widgets.
static IMAGE_LOADS: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

struct ImageLoadPermit;

impl ImageLoadPermit {
    fn acquire() -> Self {
        let (active, ready) = &IMAGE_LOADS;
        let mut count = active.lock().unwrap();
        while *count >= 2 {
            count = ready.wait(count).unwrap();
        }
        *count += 1;
        Self
    }
}

impl Drop for ImageLoadPermit {
    fn drop(&mut self) {
        *IMAGE_LOADS.0.lock().unwrap() -= 1;
        IMAGE_LOADS.1.notify_one();
    }
}

enum PreviewState {
    Idle,
    Loading,
    Ready(Arc<ThumbnailBitmap>),
    Unavailable,
}

pub(super) struct ImageFilePreview {
    image: ChatToolOutputFile,
    source: FilesSourceKind,
    access: Option<SifsDaemonAccess>,
    waker: Option<UiHandle>,
    state: Arc<Mutex<PreviewState>>,
    loading: WidgetPod,
    unavailable: WidgetPod,
}

impl ImageFilePreview {
    #[cfg(test)]
    pub(super) fn new(
        image: ChatToolOutputFile,
        access: Option<SifsDaemonAccess>,
        waker: Option<UiHandle>,
    ) -> Self {
        Self::with_source(image, FilesSourceKind::Sifs, access, waker)
    }

    pub(super) fn with_source(
        image: ChatToolOutputFile,
        source: FilesSourceKind,
        access: Option<SifsDaemonAccess>,
        waker: Option<UiHandle>,
    ) -> Self {
        Self {
            image,
            source,
            access,
            waker,
            state: Arc::new(Mutex::new(PreviewState::Idle)),
            loading: WidgetPod::new(Label::new("Loading image…")),
            unavailable: WidgetPod::new(Label::new("Image preview unavailable")),
        }
    }

    fn start_loading(&self) {
        let mut state = self.state.lock().unwrap();
        if !matches!(*state, PreviewState::Idle) {
            return;
        }
        let access = self.access.clone();
        if self.source == FilesSourceKind::Sifs && access.is_none() {
            *state = PreviewState::Unavailable;
            return;
        }
        *state = PreviewState::Loading;
        drop(state);
        let state = Arc::clone(&self.state);
        let path = self.image.path.clone();
        let waker = self.waker.clone();
        let source = self.source;
        if std::thread::Builder::new()
            .name("chat-image-preview".into())
            .spawn(move || {
                let _permit = ImageLoadPermit::acquire();
                if Arc::strong_count(&state) == 1 {
                    return;
                }
                let result = match source {
                    FilesSourceKind::Local => read_local_preview_bytes(&path),
                    FilesSourceKind::Sifs => access
                        .expect("SIFS access checked above")
                        .read_image_snapshot(&path),
                }
                .and_then(|bytes| decode_raster_preview(&bytes, 512));
                *state.lock().unwrap() = match result {
                    Ok(bitmap) => PreviewState::Ready(bitmap),
                    Err(error) => {
                        tracing::debug!(%path, %error, "generated image preview unavailable");
                        PreviewState::Unavailable
                    }
                };
                if let Some(waker) = waker {
                    waker.broadcast_application(IMAGE_READY, ());
                }
            })
            .is_err()
        {
            *self.state.lock().unwrap() = PreviewState::Unavailable;
        }
    }
}

impl Widget for ImageFilePreview {
    fn command(&mut self, ctx: &mut EventCtx, command: &Command<'_>) {
        if command.is(IMAGE_READY) {
            ctx.request_paint();
            ctx.request_semantics();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.loading.measure(ctx, constraints);
        self.unavailable.measure(ctx, constraints);
        constraints.clamp(Size::new(constraints.max.width.min(512.0), 300.0))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.loading.arrange(ctx, bounds);
        self.unavailable.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        // The transcript only paints visible rows, so offscreen images perform no I/O.
        self.start_loading();
        let state = self.state.lock().unwrap();
        match &*state {
            PreviewState::Ready(bitmap) => {
                let handle = ctx.widget_image_handle(0);
                if !ctx.image_registered(handle) {
                    let Ok(image) = RegisteredImage::from_rgba8(
                        bitmap.width,
                        bitmap.height,
                        bitmap.rgba.to_vec(),
                    ) else {
                        self.unavailable.paint(ctx);
                        return;
                    };
                    ctx.register_image(handle, image);
                }
                let bounds = ctx.bounds();
                let scale = (bounds.width() / bitmap.width as f32)
                    .min(bounds.height() / bitmap.height as f32);
                let width = bitmap.width as f32 * scale;
                let height = bitmap.height as f32 * scale;
                ctx.draw_image_source(
                    Rect::new(
                        bounds.x(),
                        bounds.y() + (bounds.height() - height) * 0.5,
                        width,
                        height,
                    ),
                    ImageSource::new(handle),
                );
            }
            PreviewState::Unavailable => self.unavailable.paint(ctx),
            _ => self.loading.paint(ctx),
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Image, ctx.bounds());
        node.name = Some(self.image.label.clone());
        node.description = Some(match &*self.state.lock().unwrap() {
            PreviewState::Ready(_) => self.image.path.clone(),
            PreviewState::Unavailable => "Image preview unavailable".into(),
            _ => "Loading image".into(),
        });
        ctx.push(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_attachments_read_existing_local_images_without_copying_files() {
        let root = sinomo_testdir::TestDir::new("markdown-local-image");
        let path = root.path().join("image.png");
        image::RgbaImage::from_pixel(64, 32, image::Rgba([20, 210, 40, 255]))
            .save(&path)
            .unwrap();
        let original = std::fs::read(&path).unwrap();
        let bytes = read_local_preview_bytes(path.to_str().unwrap()).unwrap();
        let preview = decode_raster_preview(&bytes, 512).unwrap();
        assert_eq!((preview.width, preview.height), (512, 256));
        assert_eq!(bytes, original);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        let large = root.path().join("large.png");
        std::fs::File::create(&large)
            .unwrap()
            .set_len(32 * 1024 * 1024 + 1)
            .unwrap();
        assert!(
            read_local_preview_bytes(large.to_str().unwrap())
                .unwrap_err()
                .contains("limit")
        );
    }

    #[test]
    fn generated_image_preview_renders_pixels_with_preserved_aspect_ratio() -> sui::Result<()> {
        let preview = ImageFilePreview::new(
            ChatToolOutputFile {
                path: "/generated.png".into(),
                label: "Generated image".into(),
                ..Default::default()
            },
            None,
            None,
        );
        *preview.state.lock().unwrap() = PreviewState::Ready(Arc::new(ThumbnailBitmap {
            width: 2,
            height: 1,
            rgba: Arc::from([20, 210, 40, 255, 20, 210, 40, 255]),
        }));
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Generated image preview")
                    .root(preview),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(sui::Event::Window(sui::WindowEvent::Resized(Size::new(
                400.0, 300.0,
            ))))?;
        let screenshot = window.capture_screenshot()?;
        let green_rows = screenshot
            .pixels()
            .chunks_exact(screenshot.width() as usize * 4)
            .filter(|row| {
                row.chunks_exact(4)
                    .any(|pixel| pixel[1] > 150 && pixel[0] < 80 && pixel[2] < 80)
            })
            .count();
        assert_eq!(
            green_rows, 200,
            "a 2:1 image fits a 400 by 300 preview without cropping or stretching"
        );
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/generated-image-preview.png");
        screenshot
            .write_png(path)
            .expect("write preview verification screenshot");
        Ok(())
    }
}
