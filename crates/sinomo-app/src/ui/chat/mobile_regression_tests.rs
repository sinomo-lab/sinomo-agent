#[test]
fn mobile_p1_drawer_search_keeps_focus_while_filtering() -> sui::Result<()> {
    let root = sinomo_testdir::TestDir::new("app-mobile-p1");
    let backend = Backend::local_fallback_for_data_dir(sinomo_runtime::local::SinomoDataDir::new(
        root.to_path_buf(),
    ));
    let shared = Shared::new(AppState::ready(backend));
    {
        let mut chat = shared.chat_mut();
        seed_default_provider(&mut chat);
        seed_test_agents(&mut chat);
        chat.models_loading = false;
        chat.session_list_loading = false;
        chat.notice = None;
        chat.activity.clear();
        chat.sessions = vec![SessionRef {
            id: SessionId::generate().to_string(),
            title: "audit chat".into(),
            kind: ConversationKind::Chat,
            updated_unix: 0,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
    }
    let window = test_window(
        "Mobile search",
        view(shared.clone()),
        Size::new(380.0, 780.0),
    )?;
    window
        .get_by_role(SemanticsRole::Button)
        .with_name("Open conversations and agents")
        .click()?;
    window.advance_time(0.5)?;
    assert!(shared.chat().compact_drawer_open, "drawer opened");
    let input = window
        .get_by_role(SemanticsRole::TextInput)
        .with_name("chat-session-filter");
    input.focus()?;
    for (index, ch) in "audit".chars().enumerate() {
        window
            .root()
            .dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
                text: ch.to_string(),
            }))?;
        window.run_until_idle()?;
        assert_eq!(shared.chat().session_filter, "audit"[..index + 1]);
        assert!(
            window
                .snapshot()?
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("chat-session-filter")
                    && node.state.focused),
            "filtering must retain the focused search editor"
        );
    }
    Ok(())
}

#[test]
fn mobile_p1_draft_stays_visible_across_responsive_layouts() -> sui::Result<()> {
    let root = sinomo_testdir::TestDir::new("app-mobile-p1");
    let backend = Backend::local_fallback_for_data_dir(sinomo_runtime::local::SinomoDataDir::new(
        root.to_path_buf(),
    ));
    let shared = Shared::new(AppState::ready(backend));
    {
        let mut chat = shared.chat_mut();
        seed_default_provider(&mut chat);
        chat.models_loading = false;
    }
    let window = test_window(
        "Mobile draft",
        view(shared.clone()),
        Size::new(380.0, 780.0),
    )?;
    window
        .get_by_role(SemanticsRole::TextInput)
        .with_name("chat-input")
        .fill("Keep this draft visible")?;
    for size in [
        Size::new(1100.0, 700.0),
        Size::new(380.0, 780.0),
        Size::new(1100.0, 700.0),
    ] {
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(size)))?;
        window.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let editor = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.name.as_deref() == Some("chat-input") && node.role == SemanticsRole::TextInput
            })
            .expect("visible composer");
        assert_eq!(
            editor.value,
            Some(sui::SemanticsValue::Text(shared.chat().input.clone())),
            "the visible editor must contain exactly the draft used by Send"
        );
    }
    Ok(())
}
