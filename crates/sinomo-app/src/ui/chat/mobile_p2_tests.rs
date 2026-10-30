fn mobile_p2_shared() -> (sinomo_testdir::TestDir, Shared) {
    let root = sinomo_testdir::TestDir::new("app-mobile-p2");
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
    }
    (root, shared)
}

#[test]
fn mobile_p2_manual_attachment_keeps_focus_while_typing_and_clearing_errors() -> sui::Result<()> {
    let (_root, shared) = mobile_p2_shared();
    shared
        .chat_mut()
        .set_composer_popup(Some(ComposerPopup::Attachments));
    shared.chat_mut().attachment_manual_path_open = true;
    let window = test_window(
        "Manual attachment",
        view(shared.clone()),
        Size::new(380.0, 780.0),
    )?;
    window
        .get_by_role(SemanticsRole::ScrollView)
        .with_name("Attachment options")
        .scroll_pixels(sui::Vector::new(0.0, -200.0))?;
    let input = window
        .get_by_role(SemanticsRole::TextInput)
        .with_name("chat-attachment-path");
    input.focus()?;
    for ch in "/p2-missing.txt".chars() {
        window
            .root()
            .dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
                text: ch.to_string(),
            }))?;
        window.run_until_idle()?;
        assert!(
            window
                .snapshot()?
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("chat-attachment-path")
                    && node.state.focused)
        );
    }
    assert_eq!(shared.chat().attachment_path_input, "/p2-missing.txt");
    shared.chat_mut().attachment_picker_error = Some("File not found".into());
    window.run_until_idle()?;
    window
        .root()
        .dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
            text: "x".into(),
        }))?;
    window.run_until_idle()?;
    assert_eq!(shared.chat().attachment_path_input, "/p2-missing.txtx");
    assert!(shared.chat().attachment_picker_error.is_none());
    assert!(
        window
            .snapshot()?
            .accessibility
            .nodes
            .iter()
            .any(|node| node.name.as_deref() == Some("chat-attachment-path") && node.state.focused)
    );
    Ok(())
}

#[test]
fn mobile_p2_untitled_history_has_searchable_time_and_agent_context() -> sui::Result<()> {
    let (_root, shared) = mobile_p2_shared();
    shared.set_text_scale(AppTextScale::ExtraLarge);
    let first = SessionRef {
        id: SessionId::generate().to_string(),
        title: "New chat".into(),
        kind: ConversationKind::Chat,
        updated_unix: 1_700_000_000,
        participant_count: 1,
        participant_names: vec!["coding-agent".into()],
    };
    shared.chat_mut().sessions = vec![
        first.clone(),
        SessionRef {
            id: SessionId::generate().to_string(),
            updated_unix: first.updated_unix + 10,
            participant_names: vec!["main-agent".into()],
            ..first.clone()
        },
    ];
    let reader: ThemeReader = {
        let shared = shared.clone();
        Rc::new(move || shared.theme())
    };
    let window = test_window(
        "Untitled history",
        sessions_scroll_rebuild(&shared, &reader, &ScrollState::new()),
        Size::new(320.0, 300.0),
    )?;
    for label in [
        "New chat · coding-agent",
        "2023-11-14 22:13:20 UTC",
        "2023-11-14 22:13:30 UTC",
    ] {
        window
            .get_by_role(SemanticsRole::Text)
            .with_name(label)
            .expect()
            .to_be_visible()?;
    }
    shared.chat_mut().set_session_filter("coding-agent".into());
    window.run_until_idle()?;
    window
        .get_by_role(SemanticsRole::Text)
        .with_name("New chat · coding-agent")
        .expect()
        .to_be_visible()?;
    assert_eq!(
        window
            .get_by_role(SemanticsRole::Text)
            .with_name("New chat · main-agent")
            .count()?,
        0
    );
    assert_eq!(
        shared.chat().sessions[0].title,
        "New chat",
        "display context must not rename stored chats"
    );
    Ok(())
}

#[test]
fn mobile_p2_expanded_tool_reserves_all_code_lines_at_extra_large_text() -> sui::Result<()> {
    let (_root, shared) = mobile_p2_shared();
    shared.set_text_scale(AppTextScale::ExtraLarge);
    {
        let mut chat = shared.chat_mut();
        chat.messages = vec![
            ChatMessage::user("Read this file"),
            ChatMessage::model("Finished"),
        ];
        chat.messages[1].work_trace = Some(tool_trace(vec![ChatToolActivity {
            tool_call_id: None,
            step_index: 0,
            call_index: Some(0),
            call_id: Some("p2".into()),
            tool: "read_file".into(),
            input: "{\n  \"path\":\n  \"example.txt\"\n}".into(),
            input_preview: "example.txt".into(),
            status: "completed".into(),
            process_command: None,
            process_status: None,
            exit_code: None,
            elapsed_ms: None,
            stdout: "first\nsecond\nthird\nfourth".into(),
            stderr: String::new(),
            output_files: vec![],
        }]));
        chat.stick_bottom = false;
        chat.scroll = 0.0;
    }
    let window = test_window(
        "Large tool activity",
        view(shared.clone()),
        Size::new(380.0, 1000.0),
    )?;
    window
        .get_by_role(SemanticsRole::Button)
        .with_name("Toggle response work trace")
        .click()?;
    let snapshot = window.snapshot()?;
    let tool = snapshot
        .accessibility
        .nodes
        .iter()
        .find(|node| {
            node.role == SemanticsRole::ListItem
                && node
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("Tool:"))
        })
        .expect("expanded tool card is rendered");
    let theme = shared.theme();
    let readable_content = 8.0 * theme.text.sm.line_height
        + theme.text.sm.line_height
        + 3.0 * theme.text.xs.line_height
        + 48.0;
    assert!(
        tool.bounds.height() >= readable_content,
        "tool card must reserve eight code lines, its title/subtitle and Input/Output labels: {:?}, minimum {readable_content}",
        tool.bounds
    );
    window
        .get_by_role(SemanticsRole::Button)
        .with_name("Toggle response work trace")
        .click()?;
    assert!(shared.chat().expanded_traces.is_empty());
    Ok(())
}
