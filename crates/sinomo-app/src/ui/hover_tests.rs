//! Reproducible mouse-hover workload using the actual app views and debug renderer.
use super::{chat, files};
use crate::{
    backend::Backend,
    state::{AppState, Shared},
};
use std::time::Instant;
use sui::{Event, Point, PointerEvent, PointerEventKind, SemanticsRole, Size, WindowEvent};

fn fixture(name: &str) -> sui::Result<(sui::Runtime, sui::WindowId, sui::RenderOutput)> {
    let shared = Shared::new(AppState::ready(Backend::local_fallback()));
    if name == "Chat" {
        let mut state = shared.chat_mut();
        state.models_loading = false;
        state.sessions = (0..48).map(|index| chat::SessionRef {
            id: format!("hover-{index}"),
            title: format!("Hover row {index:02}: Validate and finish the Todo app already present in this workspace"),
            kind: chat::ConversationKind::Chat, updated_unix: index,
            participant_count: 1, participant_names: vec![],
        }).collect();
    } else {
        let mut state = shared.files_mut();
        state.loaded = true;
        state.loading = false;
        state.entries = (0..48)
            .map(|index| sinomo_cli::app_api::SifsEntry {
                name: format!("Hover row {index:02}.txt"),
                path: format!("/hover-{index}.txt"),
                is_dir: false,
                is_link: false,
                size: Some(128),
            })
            .collect();
        state.entries_total_count = state.entries.len();
    }
    let builder = sui::WindowBuilder::new().title(name);
    let mut runtime = if name == "Chat" {
        sui::Application::new()
            .window(builder.root(chat::view(shared.clone())))
            .build()?
    } else {
        sui::Application::new()
            .window(builder.root(files::view(shared.clone())))
            .build()?
    };
    let id = runtime.window_ids()[0];
    sui::set_window_scene_statistics_detail_mode(id, sui::SceneStatisticsDetailMode::Detailed);
    runtime.handle_event(
        id,
        Event::Window(WindowEvent::Resized(Size::new(1280.0, 720.0))),
    )?;
    let output = runtime.render(id)?;
    Ok((runtime, id, output))
}

fn move_to(
    runtime: &mut sui::Runtime,
    id: sui::WindowId,
    bounds: sui::Rect,
    offset: f32,
) -> sui::Result<()> {
    runtime.handle_event(
        id,
        Event::Pointer(PointerEvent::new(
            PointerEventKind::Move,
            Point::new(
                bounds.x() + 65.0 + offset,
                bounds.y() + bounds.height() * 0.5,
            ),
        )),
    )
}

fn settle(
    runtime: &mut sui::Runtime,
    renderer: &mut sui::WgpuRenderer,
    id: sui::WindowId,
    start: f64,
) -> sui::Result<()> {
    for frame in 0..32 {
        runtime.tick(start + frame as f64 / 30.0);
        for (window, event) in runtime.drain_ready_events() {
            runtime.handle_event(window, event)?;
        }
        if runtime.needs_render(id)? {
            runtime.handle_event(id, Event::Window(WindowEvent::RedrawRequested))?;
            let output = runtime.render(id)?;
            renderer.render(&output.frame)?;
        }
    }
    Ok(())
}

#[test]
fn hovering_lists_repaints_only_the_changed_content() -> sui::Result<()> {
    for name in ["Chat", "Files"] {
        let (mut runtime, id, output) = fixture(name)?;
        let mut renderer = sui::WgpuRenderer::default();
        renderer.render(&output.frame)?;
        let rows: Vec<_> = output
            .semantics
            .iter()
            .filter(|node| {
                node.role == SemanticsRole::ListItem
                    && node
                        .name
                        .as_deref()
                        .is_some_and(|name| name.starts_with("Hover row"))
                    && node.bounds.y() > 0.0
                    && node.bounds.max_y() < 690.0
            })
            .take(2)
            .cloned()
            .collect();
        assert_eq!(rows.len(), 2);
        let table = output
            .semantics
            .iter()
            .find(|node| node.name.as_deref() == Some("files-listing"))
            .map(|node| node.id);
        move_to(&mut runtime, id, rows[0].bounds, 0.0)?;
        settle(&mut runtime, &mut renderer, id, 0.0)?;
        move_to(&mut runtime, id, rows[1].bounds, 0.0)?;
        runtime.handle_event(id, Event::Window(WindowEvent::RedrawRequested))?;
        let hovered = runtime.render(id)?;
        renderer.render(&hovered.frame)?;
        let artifact_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/hover-regression");
        std::fs::create_dir_all(&artifact_dir).expect("create hover artifact directory");
        let capture = renderer.capture_last_frame_rgba(id)?;
        image::save_buffer(
            artifact_dir.join(format!("{}.png", name.to_lowercase())),
            capture.pixels(),
            capture.width(),
            capture.height(),
            image::ColorType::Rgba8,
        )
        .expect("save hover screenshot");
        assert!(!hovered.frame.layer_updates.is_empty());
        for update in &hovered.frame.layer_updates {
            let expected = if name == "Chat" {
                rows.iter().any(|row| row.id == update.owner)
            } else {
                table == Some(update.owner)
            };
            assert!(
                expected,
                "{name}: hover repainted unrelated layer {update:?}"
            );
        }
        assert!(
            hovered
                .semantics
                .iter()
                .any(|node| node.id == rows[1].id && node.state.hovered)
        );
        assert!(
            hovered
                .semantics
                .iter()
                .any(|node| node.id == rows[0].id && !node.state.hovered)
        );
        settle(&mut runtime, &mut renderer, id, 2.0)?;
        move_to(&mut runtime, id, rows[1].bounds, 2.0)?;
        assert!(
            !runtime.needs_render(id)?,
            "{name}: moving inside an unchanged row must not redraw"
        );
    }
    Ok(())
}

#[test]
#[ignore = "hover profiling: run with SUI_PROFILE_WIDGET_TIMINGS=1 and --ignored --nocapture"]
fn list_hover_frame_cost() -> sui::Result<()> {
    for name in ["Chat", "Files"] {
        let (mut runtime, id, output) = fixture(name)?;
        let rows: Vec<_> = output
            .semantics
            .iter()
            .filter(|node| {
                node.role == SemanticsRole::ListItem
                    && node
                        .name
                        .as_deref()
                        .is_some_and(|name| name.starts_with("Hover row"))
                    && node.bounds.y() > 0.0
                    && node.bounds.max_y() < 690.0
            })
            .take(8)
            .map(|node| node.bounds)
            .collect();
        assert!(rows.len() >= 4, "{name}: need several visible rows");
        let mut renderer = sui::WgpuRenderer::default();
        renderer.render(&output.frame)?;
        let mut samples = Vec::new();
        let mut events = Vec::new();
        let mut render = Vec::new();
        let mut paints = 0;
        let mut measures = 0;
        for frame in 0..160 {
            runtime.tick(frame as f64 / 120.0);
            let started = Instant::now();
            for (window, event) in runtime.drain_ready_events() {
                runtime.handle_event(window, event)?;
            }
            let row = rows[frame % rows.len()];
            runtime.handle_event(
                id,
                Event::Pointer(PointerEvent::new(
                    PointerEventKind::Move,
                    Point::new(
                        row.x() + 65.0 + (frame % 4) as f32 * 3.0,
                        row.y() + row.height() * 0.5,
                    ),
                )),
            )?;
            runtime.handle_event(id, Event::Window(WindowEvent::RedrawRequested))?;
            let output = runtime.render(id)?;
            let event_ms = started.elapsed().as_secs_f64() * 1000.0;
            let renderer_started = Instant::now();
            renderer.render(&output.frame)?;
            if frame >= 40 {
                events.push(event_ms);
                render.push(renderer_started.elapsed().as_secs_f64() * 1000.0);
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
                paints += output
                    .diagnostics
                    .widget_timings
                    .iter()
                    .filter(|s| s.phase.label() == "Paint")
                    .map(|s| s.calls)
                    .sum::<usize>();
                measures += output
                    .diagnostics
                    .widget_timings
                    .iter()
                    .filter(|s| s.phase.label() == "Measure")
                    .map(|s| s.calls)
                    .sum::<usize>();
            }
        }
        for values in [&mut samples, &mut events, &mut render] {
            values.sort_by(f64::total_cmp);
        }
        eprintln!(
            "{name}: 120 hover frames, median={:.3}ms p95={:.3}ms, UI={:.3}ms renderer={:.3}ms, paint calls/frame={:.1} measure calls/frame={:.1}",
            samples[60],
            samples[114],
            events[60],
            render[60],
            paints as f64 / 120.0,
            measures as f64 / 120.0
        );
    }
    Ok(())
}
