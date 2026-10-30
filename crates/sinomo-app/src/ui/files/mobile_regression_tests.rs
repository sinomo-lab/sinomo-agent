#[test]
fn mobile_p1_back_protects_active_and_hidden_unsaved_documents() -> sui::Result<()> {
    let root = sinomo_testdir::TestDir::new("app-mobile-p1");
    let backend = Backend::local_fallback_for_data_dir(sinomo_runtime::local::SinomoDataDir::new(
        root.to_path_buf(),
    ));
    let shared = Shared::new(AppState::ready(backend));
    let id = shared
        .open_document_tab(
            FilesSourceKind::Sifs,
            "/audit.txt".into(),
            "audit.txt".into(),
        )
        .unwrap();
    let viewer = shared.workspace_scope(id).unwrap();
    {
        let mut files = viewer.files_mut();
        files.pending_document_request_id = None;
        let document = files.document.as_mut().unwrap();
        document.loading = false;
        document.load_error = None;
        document.saved_text = "saved".into();
        document.text = "unsaved changes".into();
    }
    let app = sui_testing::TestApp::from_runtime(crate::build_runtime(shared.clone())?)?;
    let window = app.main_window()?;
    window
        .root()
        .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(380.0, 780.0))))?;
    window
        .root()
        .dispatch_event(Event::Keyboard(sui::KeyboardEvent::new(
            "BrowserBack",
            sui::KeyState::Pressed,
        )))?;
    assert!(
        viewer.files().document_close_confirmation,
        "Android Back must use the dirty-document close guard"
    );
    window
        .root()
        .dispatch_event(Event::Keyboard(sui::KeyboardEvent::new(
            "BrowserBack",
            sui::KeyState::Pressed,
        )))?;
    window.run_until_idle()?;
    assert!(
        !viewer.files().document_close_confirmation,
        "Back dismisses confirmation while preserving the draft"
    );
    assert_eq!(
        viewer.files().document.as_ref().unwrap().text,
        "unsaved changes"
    );
    shared.set_active_tab(crate::state::Tab::Sinomo);
    window.run_until_idle()?;
    window
        .root()
        .dispatch_event(Event::Keyboard(sui::KeyboardEvent::new(
            "BrowserBack",
            sui::KeyState::Pressed,
        )))?;
    assert_eq!(
        shared.active_scope().workspace_id(),
        Some(id),
        "root Back must reveal a hidden dirty document before exiting"
    );
    assert!(viewer.files().document_close_confirmation);
    Ok(())
}
