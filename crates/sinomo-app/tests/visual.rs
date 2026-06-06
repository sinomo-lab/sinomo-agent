//! Headless visual capture of each tab, rendered with the SUI testing harness.
//!
//! This is a dev tool, not an assertion-heavy test: run it to dump PNGs of the
//! app's tabs to `$SINOMO_APP_SHOTS` (default `/tmp`) for visual inspection.
//!
//! ```sh
//! cargo test -p sinomo-app --test visual -- --nocapture
//! ```

use sinomo_app::state::Tab;
use sinomo_app::theme::AppThemeMode;
use sinomo_app::{boot, build_runtime};
use sui_testing::TestApp;

fn capture(shared: sinomo_app::state::Shared, name: &str) {
    let dir = shots_dir();
    let runtime = build_runtime(shared).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let screenshot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join(format!("sinomo-app-{name}.png"));
    screenshot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

fn shots_dir() -> std::path::PathBuf {
    std::env::var("SINOMO_APP_SHOTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir())
}

#[test]
fn capture_tabs() {
    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    for (tab, name) in [
        (Tab::Sinomo, "sinomo"),
        (Tab::Files, "files"),
        (Tab::Settings, "settings"),
    ] {
        capture(boot(tab), name);
    }

    // Light-theme variants to validate the light palette.
    for (tab, name) in [
        (Tab::Sinomo, "sinomo-light"),
        (Tab::Settings, "settings-light"),
    ] {
        let shared = boot(tab);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        capture(shared, name);
    }
}

fn click_at(root: &sui_testing::Locator, x: f32, y: f32) {
    use sui::{Event, Point, PointerButton, PointerEvent, PointerEventKind};
    let pos = Point::new(x, y);
    let mut down = PointerEvent::new(PointerEventKind::Down, pos);
    down.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(down)).expect("down");
    let mut up = PointerEvent::new(PointerEventKind::Up, pos);
    up.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(up)).expect("up");
}

#[test]
fn capture_files_nav() {
    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");
    let runtime = build_runtime(boot(Tab::Files)).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();

    // First directory row sits just below the breadcrumb in the content area.
    click_at(&root, 120.0, 116.0);

    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-files-nav.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

#[test]
fn capture_typing() {
    use sui::{
        Event, KeyState, KeyboardEvent, Point, PointerButton, PointerEvent, PointerEventKind,
    };

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let runtime = build_runtime(boot(Tab::Sinomo)).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();

    // Focus the composer input, then type a message.
    let pos = Point::new(600.0, 675.0);
    let mut down = PointerEvent::new(PointerEventKind::Down, pos);
    down.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(down))
        .expect("pointer down");
    let mut up = PointerEvent::new(PointerEventKind::Up, pos);
    up.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(up)).expect("pointer up");

    for key in [
        "H", "e", "l", "l", "o", " ", "S", "i", "n", "o", "m", "o", ",", " ", "w", "h", "a", "t",
        " ", "i", "s", " ", "S", "I", "F", "S", "?",
    ] {
        root.dispatch_event(Event::Keyboard(KeyboardEvent::new(key, KeyState::Pressed)))
            .expect("key");
    }

    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-typing.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}
