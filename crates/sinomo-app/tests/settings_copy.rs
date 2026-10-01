use sinomo_app::state::Tab;
use sinomo_app::ui::settings::SettingsSection;
use sinomo_app::{boot, build_runtime};
use sui::{
    Event, KeyState, KeyboardEvent, Point, PointerButton, PointerButtons, PointerEvent,
    PointerEventKind, SemanticsAction, SemanticsRole,
};
use sui_testing::TestApp;

fn command_key(target: &sui_testing::Locator, key: &str) -> sui::Result<()> {
    let mut event = KeyboardEvent::new(key, KeyState::Pressed);
    event.modifiers.control = true;
    target.dispatch_event(Event::Keyboard(event))
}

fn drag_primary(target: &sui_testing::Locator, start: Point, end: Point) -> sui::Result<()> {
    let mut down = PointerEvent::new(PointerEventKind::Down, start);
    down.pointer_id = 1;
    down.button = Some(PointerButton::Primary);
    down.buttons = PointerButtons::new(1);
    target.dispatch_event(Event::Pointer(down))?;

    let mut moved = PointerEvent::new(PointerEventKind::Move, end);
    moved.pointer_id = 1;
    moved.buttons = PointerButtons::new(1);
    target.dispatch_event(Event::Pointer(moved))?;

    let mut up = PointerEvent::new(PointerEventKind::Up, end);
    up.pointer_id = 1;
    up.button = Some(PointerButton::Primary);
    target.dispatch_event(Event::Pointer(up))
}

fn right_click(target: &sui_testing::Locator, position: Point) -> sui::Result<()> {
    let mut down = PointerEvent::new(PointerEventKind::Down, position);
    down.pointer_id = 1;
    down.button = Some(PointerButton::Secondary);
    down.buttons = PointerButtons::new(2);
    target.dispatch_event(Event::Pointer(down))?;

    let mut up = PointerEvent::new(PointerEventKind::Up, position);
    up.pointer_id = 1;
    up.button = Some(PointerButton::Secondary);
    target.dispatch_event(Event::Pointer(up))
}

fn click_primary(target: &sui_testing::Locator, position: Point) -> sui::Result<()> {
    let mut down = PointerEvent::new(PointerEventKind::Down, position);
    down.pointer_id = 1;
    down.button = Some(PointerButton::Primary);
    down.buttons = PointerButtons::new(1);
    target.dispatch_event(Event::Pointer(down))?;

    let mut up = PointerEvent::new(PointerEventKind::Up, position);
    up.pointer_id = 1;
    up.button = Some(PointerButton::Primary);
    target.dispatch_event(Event::Pointer(up))
}

#[test]
fn connection_low_level_detail_supports_pointer_selection_and_both_copy_paths() -> sui::Result<()> {
    let shared = boot(Tab::Settings);
    shared.settings_mut().open(SettingsSection::System);
    let detail = "chat route unavailable\ndaemon address 127.0.0.1:21353";
    shared.chat_mut().notice = Some(detail.to_string());

    let runtime = build_runtime(shared)?;
    let clipboard = runtime.clipboard();
    let app = TestApp::from_runtime(runtime)?;
    let window = app.main_window()?;
    let root = window.root();

    window
        .get_by_role(SemanticsRole::Button)
        .with_name("Show detail")
        .click()?;
    let field = window
        .get_by_role(SemanticsRole::TextInput)
        .with_name("connection-chat-detail");
    field.expect().to_have_value(detail)?;

    let field_node = window
        .snapshot()?
        .accessibility
        .nodes
        .into_iter()
        .find(|node| {
            node.role == SemanticsRole::TextInput
                && node.name.as_deref() == Some("connection-chat-detail")
        })
        .expect("expanded connection detail semantics");
    assert!(field_node.actions.contains(&SemanticsAction::SetSelection));
    assert!(field_node.actions.contains(&SemanticsAction::Copy));
    assert!(
        field_node
            .editable_text
            .as_ref()
            .is_some_and(|editable| editable.readonly),
        "connection detail should expose read-only editable-text semantics"
    );
    let bounds = field_node.bounds;
    let start = Point::new(bounds.x() + 8.0, bounds.y() + 8.0);
    let end = Point::new(bounds.max_x() - 8.0, bounds.max_y() - 8.0);
    drag_primary(&root, start, end)?;

    clipboard.set_text("pointer selection was not copied");
    command_key(&root, "c")?;
    assert_eq!(
        clipboard.text().as_deref(),
        Some(detail),
        "Ctrl+C should copy the text selected by pointer drag"
    );

    clipboard.set_text("context menu did not copy");
    right_click(
        &root,
        Point::new(
            bounds.x() + bounds.width() * 0.5,
            bounds.y() + bounds.height() * 0.5,
        ),
    )?;
    let reopened = window.snapshot()?;
    let copy_item = reopened
        .accessibility
        .nodes
        .into_iter()
        .find(|node| node.role == SemanticsRole::MenuItem && node.name.as_deref() == Some("Copy"))
        .expect("the connection detail context menu should expose Copy");
    assert!(
        !copy_item.state.disabled,
        "Copy should be enabled for the live pointer selection"
    );
    root.dispatch_event(Event::Keyboard(KeyboardEvent::new(
        "Enter",
        KeyState::Pressed,
    )))?;
    app.run_until_idle()?;
    assert_eq!(
        clipboard.text().as_deref(),
        Some(detail),
        "the focused context-menu Copy action should use the live selection"
    );

    clipboard.set_text("context menu pointer did not copy");
    right_click(
        &root,
        Point::new(
            bounds.x() + bounds.width() * 0.5,
            bounds.y() + bounds.height() * 0.5,
        ),
    )?;
    let copy_item = window
        .snapshot()?
        .accessibility
        .nodes
        .into_iter()
        .find(|node| node.role == SemanticsRole::MenuItem && node.name.as_deref() == Some("Copy"))
        .expect("the reopened context menu should expose Copy");
    click_primary(
        &root,
        Point::new(
            copy_item.bounds.x() + copy_item.bounds.width() * 0.5,
            copy_item.bounds.y() + copy_item.bounds.height() * 0.5,
        ),
    )?;
    app.run_until_idle()?;
    assert_eq!(
        clipboard.text().as_deref(),
        Some(detail),
        "the right-click Copy item should preserve and copy the pointer selection"
    );

    Ok(())
}
