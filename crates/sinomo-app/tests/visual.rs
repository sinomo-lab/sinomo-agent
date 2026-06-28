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

struct VisualTempDataDir {
    root: std::path::PathBuf,
}

impl Drop for VisualTempDataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn visual_temp_data_dir(slug: &str) -> (sinomo_runtime::local::SinomoDataDir, VisualTempDataDir) {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "sinomo-app-visual-{slug}-{}-{unique}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    (
        sinomo_runtime::local::SinomoDataDir::new(root.clone()),
        VisualTempDataDir { root },
    )
}

fn boot_with_visual_data_dir(
    initial_tab: Tab,
    slug: &str,
) -> (sinomo_app::state::Shared, VisualTempDataDir) {
    let (data_dir, cleanup) = visual_temp_data_dir(slug);
    let backend = sinomo_app::backend::Backend::local_fallback_for_data_dir(data_dir);
    let mut state = sinomo_app::state::AppState::ready(backend);
    state.active_tab = initial_tab;
    (sinomo_app::state::Shared::new(state), cleanup)
}

fn boot_settings_with_provider_accounts(
    slug: &str,
) -> (sinomo_app::state::Shared, VisualTempDataDir) {
    let (data_dir, cleanup) = visual_temp_data_dir(slug);
    let backend = sinomo_app::backend::Backend::local_fallback_for_data_dir(data_dir);
    let config = backend.config.clone();
    sinomo_app::backend::config::add_provider_account(
        config.as_ref(),
        "openai",
        "work",
        "sk-visual-openai",
        "gpt-5",
        "",
    )
    .expect("seed OpenAI provider");
    sinomo_app::backend::config::add_provider_account(
        config.as_ref(),
        "deepseek",
        "default",
        "sk-visual-deepseek",
        "deepseek-v4-flash",
        "",
    )
    .expect("seed DeepSeek provider");

    let mut state = sinomo_app::state::AppState::ready(backend);
    state.active_tab = Tab::Settings;
    let shared = sinomo_app::state::Shared::new(state);
    shared.settings_mut().selected = Some(sinomo_app::ui::settings::SettingsSection::Providers);
    (shared, cleanup)
}

#[test]
fn capture_tabs() {
    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    for (tab, name) in [
        (Tab::Sinomo, "sinomo"),
        (Tab::Evolution, "evolution"),
        (Tab::Cluster, "cluster"),
        (Tab::Files, "files"),
        (Tab::Settings, "settings"),
    ] {
        let shared = boot(tab);
        shared.borrow_mut().theme_mode = AppThemeMode::Dark;
        capture(shared, name);
    }

    // Light-theme variants to validate the light palette.
    for (tab, name) in [
        (Tab::Sinomo, "sinomo-light"),
        (Tab::Evolution, "evolution-light"),
        (Tab::Cluster, "cluster-light"),
        (Tab::Settings, "settings-light"),
    ] {
        let shared = boot(tab);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        capture(shared, name);
    }
}

#[test]
fn capture_evolution_seeded() {
    use sinomo_app::backend::evolution::{
        EvolutionDashboard, EvolutionDecisionSummary, EvolutionJobSummary, EvolutionPhase,
    };

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let shared = boot(Tab::Evolution);
    {
        let mut evolution = shared.evolution_mut();
        evolution.objective_draft = "Improve DFS performance".to_string();
        evolution.scope_draft = "crates/sinomo-dfs/**".to_string();
        evolution.target_ring_index = 1;
        evolution.dashboard = EvolutionDashboard {
            source: "/home/ix/.sinomo/evolution".to_string(),
            jobs: vec![
                EvolutionJobSummary {
                    id: "01j9candidate-ready-for-review".to_string(),
                    short_id: "01j9candidate".to_string(),
                    objective: "Tighten compaction resume evidence for long-running coding turns"
                        .to_string(),
                    status_key: "awaiting-approval".to_string(),
                    status_label: "awaiting approval".to_string(),
                    phase: EvolutionPhase::AwaitingDecision,
                    target_ring: "R2".to_string(),
                    created_at: "2026-06-15T11:34:20Z".to_string(),
                    workspace_root: "/home/ix/.sinomo/source/sinomo".to_string(),
                    candidate_workspace:
                        "/home/ix/.sinomo/evolution/worktrees/01j9candidate-ready-for-review"
                            .to_string(),
                    candidate_branch: "evolution/compaction-resume-evidence".to_string(),
                    source_revision: Some("7f4d2c05".to_string()),
                    verification_outcome: Some("passed".to_string()),
                    required_gate_count: 4,
                    verification_command_count: 3,
                    module_replay_count: 1,
                    changed_files: vec![
                        "crates/sinomo-agent/src/compaction.rs".to_string(),
                        "crates/sinomo-agent/tests/compaction_resume.rs".to_string(),
                        "docs/agent-system-architecture.md".to_string(),
                    ],
                    allowed_paths: vec![
                        "crates/sinomo-agent".to_string(),
                        "docs/agent-system-architecture.md".to_string(),
                    ],
                    forbidden_paths: vec!["crates/sinomo-dfs".to_string()],
                    recovery_strategy: "git branch reset".to_string(),
                    recovery_note: "Reset the candidate worktree and preserve scorecard evidence"
                        .to_string(),
                    rollback_policy: "Manual".to_string(),
                    bundle_path:
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-ready-for-review"
                            .to_string(),
                    mutation_packet_path:
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-ready-for-review/mutation-packet.json"
                            .to_string(),
                    diff_path: Some(
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-ready-for-review/diff.patch"
                            .to_string(),
                    ),
                    scorecard_path: Some(
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-ready-for-review/scorecard.json"
                            .to_string(),
                    ),
                    candidate_summary_path: Some(
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-ready-for-review/candidate-summary.md"
                            .to_string(),
                    ),
                    artifact_ref_count: 7,
                    command_log_count: 3,
                    ..EvolutionJobSummary::default()
                },
                EvolutionJobSummary {
                    id: "01j9candidate-failed".to_string(),
                    short_id: "01j9failed".to_string(),
                    objective: "Move rollout prompt overflow classification into sinomo-lm"
                        .to_string(),
                    status_key: "eval-failed".to_string(),
                    status_label: "eval failed".to_string(),
                    phase: EvolutionPhase::Failed,
                    target_ring: "R2".to_string(),
                    created_at: "2026-06-14T22:18:04Z".to_string(),
                    required_gate_count: 5,
                    verification_command_count: 4,
                    changed_files: vec!["crates/sinomo-lm/src/providers.rs".to_string()],
                    bundle_path: "/home/ix/.sinomo/evolution/candidates/01j9candidate-failed"
                        .to_string(),
                    mutation_packet_path:
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-failed/mutation-packet.json"
                            .to_string(),
                    ..EvolutionJobSummary::default()
                },
                EvolutionJobSummary {
                    id: "01j9candidate-promoted".to_string(),
                    short_id: "01j9promoted".to_string(),
                    objective: "Expose Hermes-style skill disclosure in agent package inspection"
                        .to_string(),
                    status_key: "promoted".to_string(),
                    status_label: "promoted".to_string(),
                    phase: EvolutionPhase::Passed,
                    target_ring: "R2".to_string(),
                    created_at: "2026-06-13T16:05:48Z".to_string(),
                    decision: Some(EvolutionDecisionSummary {
                        outcome: "approved".to_string(),
                        decided_at: "2026-06-13T18:12:02Z".to_string(),
                        decided_by: "operator".to_string(),
                        note: "Promoted after shadow evidence stayed clean".to_string(),
                        preflight_passed: Some(true),
                    }),
                    bundle_path:
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-promoted"
                            .to_string(),
                    mutation_packet_path:
                        "/home/ix/.sinomo/evolution/candidates/01j9candidate-promoted/mutation-packet.json"
                            .to_string(),
                    ..EvolutionJobSummary::default()
                },
            ],
            error: None,
        };
        evolution.selected = Some(0);
    }
    capture(shared, "evolution-seeded");
}

#[test]
fn cluster_node_click_opens_detail_selection() {
    use sinomo_app::backend::cluster::{ClusterNodeInfo, ClusterSnapshot};
    use sinomo_app::ui::cluster::ClusterSelection;

    fn node(id: &str) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: id.to_string(),
            display_name: id.to_string(),
            process: "daemon-main".to_string(),
            node_role: "workstation".to_string(),
            member_role: "voter".to_string(),
            member_state: "voter".to_string(),
            availability: "online".to_string(),
            daemon_version: "test".to_string(),
            commit: None,
            platform: None,
            pid: None,
            address: None,
            bind_address: None,
            rollout: None,
            observed_at: "now".to_string(),
            public_reachable: false,
            endpoints: Vec::new(),
        }
    }

    let shared = boot(Tab::Cluster);
    shared.cluster_mut().snapshot = ClusterSnapshot {
        nodes: vec![node("node-a"), node("node-b"), node("node-c")],
        source: "test".to_string(),
        observed_at: "now".to_string(),
        ..ClusterSnapshot::default()
    };
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle cluster layout");

    click_at(&root, 450.0, 232.0);
    app.run_until_idle().expect("settle detail panel");

    assert_eq!(
        shared.cluster().selected,
        Some(ClusterSelection::Node("node-a".to_string())),
        "clicking a graph node selects it for the floating detail panel"
    );
}

#[test]
fn capture_cluster_node_diagnostics() {
    use sinomo_app::backend::cluster::{ClusterEndpointInfo, ClusterNodeInfo, ClusterSnapshot};
    use sinomo_app::theme::AppThemeMode;
    use sinomo_app::ui::cluster::ClusterSelection;

    fn node(id: &str, availability: &str, endpoint: &str) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: id.to_string(),
            display_name: "sinomo-node".to_string(),
            process: "daemon-main".to_string(),
            node_role: "workstation".to_string(),
            member_role: "voter".to_string(),
            member_state: "voter".to_string(),
            availability: availability.to_string(),
            daemon_version: "test".to_string(),
            commit: None,
            platform: Some("linux/aarch64".to_string()),
            pid: None,
            address: Some("127.0.0.1:21353".to_string()),
            bind_address: None,
            rollout: None,
            observed_at: "now".to_string(),
            public_reachable: false,
            endpoints: vec![ClusterEndpointInfo {
                endpoint_id: format!("{id}-lan"),
                transport: "lan-tcp".to_string(),
                address: endpoint.to_string(),
                source: "cluster-connectivity".to_string(),
                privacy: "lan".to_string(),
                priority: 10,
                latency_ms: None,
                public_ip: false,
                reachable: availability == "online",
            }],
        }
    }

    let dir = target_shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let shared = boot(Tab::Cluster);
    shared.borrow_mut().theme_mode = AppThemeMode::Light;
    {
        let mut cluster = shared.cluster_mut();
        cluster.snapshot = ClusterSnapshot {
            nodes: vec![
                node(
                    "01kr4w089vqgdxpb84t6bq1qxn",
                    "offline",
                    "iroh://e6e7579b45088dc226274eadd8dec9782eb042c1c753ba5be249c96c1c8c0e2e?relay=https://use1-1.relay.no.iroh-canary.iroh.link/",
                ),
                node("01kr3d54d45zkm0p0xgps8k7mt", "online", "10.0.0.5:21353"),
            ],
            source: "daemon connected".to_string(),
            observed_at: "now".to_string(),
            ..ClusterSnapshot::default()
        };
        cluster.selected = Some(ClusterSelection::Node(
            "01kr4w089vqgdxpb84t6bq1qxn".to_string(),
        ));
        cluster.diagnostics_open = true;
    }

    let runtime = build_runtime(shared).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle()
        .expect("settle cluster diagnostics layout");
    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-cluster-node-diagnostics.png");
    shot.write_png(&path).expect("write png");
    println!(
        "wrote {} ({}x{})",
        path.display(),
        shot.width(),
        shot.height()
    );
}

#[test]
fn capture_settings_responsive() {
    use sui::{Event, Size, WindowEvent};

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let capture = |name: &str, window: &sui_testing::TestWindow| {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join(format!("sinomo-app-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    // Wide: master/detail — the section menu stays pinned as a sidebar.
    {
        let runtime = build_runtime(boot(Tab::Settings)).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        capture("settings-wide", &window);
    }

    // Narrow: collapses to a single drill-down column.
    {
        let runtime = build_runtime(boot(Tab::Settings)).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        let root = window.root();

        // Settle the initial (wide) frame before resizing so the resize re-measures cleanly.
        app.run_until_idle().expect("settle initial layout");
        root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))
            .expect("resize narrow");
        app.run_until_idle().expect("settle narrow layout");
        capture("settings-narrow-menu", &window);

        // Drill into the "Connection" row → detail page with a back affordance. (Row geometry: the
        // composed menu ListView is inset from the top bar; the Connection row sits near y=170.)
        click_at(&root, 120.0, 170.0);
        app.run_until_idle().expect("settle drill-in");
        capture("settings-narrow-detail", &window);
    }
}

/// Deterministic responsive captures for the rebuilt (composed) Settings tab: wide master/detail
/// in dark + light, and the narrow drill-down menu + detail. Sets `selected` directly rather than
/// relying on click coordinates so the captures stay stable as the composed layout evolves.
#[test]
fn capture_settings_composed() {
    use sinomo_app::theme::AppThemeMode;
    use sinomo_app::ui::settings::SettingsSection;
    use sui::{Event, Size, WindowEvent};

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let capture = |name: &str, window: &sui_testing::TestWindow| {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join(format!("sinomo-app-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    // Wide, dark: master/detail with the Providers section showing in the detail pane.
    {
        let shared = boot(Tab::Settings);
        shared.borrow_mut().theme_mode = AppThemeMode::Dark;
        shared.settings_mut().selected = Some(SettingsSection::Providers);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle wide-dark layout");
        capture("settings-composed-wide-dark", &window);
    }

    // Wide, light: the same master/detail restyled live by the theme reader.
    {
        let shared = boot(Tab::Settings);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        shared.settings_mut().selected = Some(SettingsSection::Appearance);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle wide-light layout");
        capture("settings-composed-wide-light", &window);
    }

    // Narrow menu: the drill-down menu (no section selected).
    {
        let shared = boot(Tab::Settings);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle initial layout");
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))
            .expect("resize narrow");
        app.run_until_idle().expect("settle narrow menu layout");
        capture("settings-composed-narrow-menu", &window);
    }

    // Narrow detail: drilled into the Connection section (back affordance + detail body).
    {
        let shared = boot(Tab::Settings);
        shared.settings_mut().selected = Some(SettingsSection::Connection);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle initial layout");
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))
            .expect("resize narrow");
        app.run_until_idle().expect("settle narrow detail layout");
        capture("settings-composed-narrow-detail", &window);
    }
}

/// Wide master/detail: clicking a sidebar row navigates the detail pane (the inner `SwitchView`
/// relayouts to the newly-selected section, driven by the host's relayout-on-pointer-up).
#[test]
fn settings_wide_sidebar_navigates() {
    use sinomo_app::ui::settings::SettingsSection;

    let shared = boot(Tab::Settings);
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle wide layout");
    assert!(
        shared.settings().selected.is_none(),
        "wide layout starts with no explicit selection (defaults to Appearance)"
    );

    // The sidebar ListView rows sit ~y=128 (Appearance), stepping by ~40px. Click "Providers" (5th).
    click_at(&root, 120.0, 288.0);
    app.run_until_idle().expect("settle nav");
    assert_eq!(
        shared.settings().selected,
        Some(SettingsSection::Providers),
        "clicking a sidebar row selects that section"
    );
}

/// Narrow drill-down: clicking a menu row drills into its detail; the back button returns.
#[test]
fn settings_narrow_click_navigates() {
    use sinomo_app::ui::settings::SettingsSection;
    use sui::{Event, Size, WindowEvent};

    let shared = boot(Tab::Settings);
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle initial layout");
    root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))
        .expect("resize narrow");
    app.run_until_idle().expect("settle narrow");
    assert!(shared.settings().selected.is_none(), "starts at the menu");

    // Drill into Connection.
    click_at(&root, 120.0, 170.0);
    app.run_until_idle().expect("settle drill-in");
    assert_eq!(
        shared.settings().selected,
        Some(SettingsSection::Connection),
        "clicking the Connection menu row drills in"
    );

    // The back button (top-left of the detail) returns to the menu.
    click_at(&root, 50.0, 67.0);
    app.run_until_idle().expect("settle back");
    assert!(
        shared.settings().selected.is_none(),
        "the back button returns to the menu level"
    );
}

fn click_at(root: &sui_testing::Locator, x: f32, y: f32) {
    use sui::{Event, Point, PointerButton, PointerButtons, PointerEvent, PointerEventKind};
    let pos = Point::new(x, y);
    root.dispatch_event(Event::Pointer(PointerEvent::new(
        PointerEventKind::Move,
        pos,
    )))
    .expect("move");
    let mut down = PointerEvent::new(PointerEventKind::Down, pos);
    down.button = Some(PointerButton::Primary);
    down.buttons = PointerButtons::new(1);
    root.dispatch_event(Event::Pointer(down)).expect("down");
    let mut up = PointerEvent::new(PointerEventKind::Up, pos);
    up.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(up)).expect("up");
}

fn seed_chat_providers(shared: &sinomo_app::state::Shared) {
    use sinomo_cli::app_api::ProviderSummary;

    let mut chat = shared.chat_mut();
    chat.providers = vec![
        ProviderSummary {
            id: "openai".to_string(),
            account: "default".to_string(),
            name: "OpenAI".to_string(),
            models: vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()],
        },
        ProviderSummary {
            id: "anthropic".to_string(),
            account: "default".to_string(),
            name: "Anthropic".to_string(),
            models: vec!["claude-opus".to_string(), "claude-sonnet".to_string()],
        },
    ];
    chat.provider_idx = 0;
    chat.model_idx = 0;
    chat.models_loading = false;
}

fn seed_healthy_chat_runtime(shared: &sinomo_app::state::Shared) {
    use sinomo_app::backend::chat_engine::RuntimeInfo;

    let mut chat = shared.chat_mut();
    chat.info = RuntimeInfo {
        agent: "main-agent".to_string(),
        provider: "openai".to_string(),
        model: "gpt-4o".to_string(),
        account: Some("default".to_string()),
        effort: None,
    };
    chat.notice = None;
    chat.activity.clear();
}

fn open_chat_provider_select(window: &sui_testing::TestWindow) -> sui::Rect {
    use sui::SemanticsRole;

    window
        .get_by_role(SemanticsRole::ComboBox)
        .with_name("chat-provider")
        .click()
        .expect("open provider select");

    let snapshot = window
        .snapshot()
        .expect("snapshot after opening provider select");
    let provider = snapshot
        .accessibility
        .nodes
        .iter()
        .find(|node| {
            node.role == SemanticsRole::ComboBox && node.name.as_deref() == Some("chat-provider")
        })
        .expect("provider select semantics");
    assert_eq!(
        provider.state.expanded,
        Some(true),
        "provider Select must be expanded before dropdown capture"
    );
    let provider_bounds = provider.bounds;
    window
        .advance_time(sui::DefaultTheme::default().motion.entrance_duration() + 0.01)
        .expect("settle provider select entrance animation");
    provider_bounds
}

/// Deterministic captures for the rebuilt (composed) Files tab: the root listing + empty inspector
/// in dark and light, a selected-entry state with the inspector populated, and narrow widths where
/// the inspector collapses away. Drives selection via the public `FilesState::selected` field and a
/// pointer click for navigation, so the captures stay stable as the composed layout evolves.
#[test]
fn capture_files_composed() {
    use sinomo_app::theme::AppThemeMode;
    use sinomo_cli::app_api::{SifsEntry, SifsObjectDetails, SifsObjectHealth, SifsPinDetails};
    use sui::{Event, Size, WindowEvent};

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let capture = |name: &str, window: &sui_testing::TestWindow| {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join(format!("sinomo-app-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    let seed_listing = |shared: &sinomo_app::state::Shared| {
        let mut files = shared.files_mut();
        files.loaded = true;
        files.loading = false;
        files.error = None;
        files.clustered = true;
        files.node_id = "node-desktop-a".to_string();
        files.path = "/services".to_string();
        files.entries = vec![
            SifsEntry {
                name: "logs".to_string(),
                path: "/services/logs".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            },
            SifsEntry {
                name: "artifact.bin".to_string(),
                path: "/services/artifact.bin".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(4_194_304),
            },
            SifsEntry {
                name: "release-notes.md".to_string(),
                path: "/services/release-notes.md".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(8_192),
            },
        ];
        files.selected = None;
        files.selected_details = None;
        files.details_loading = false;
        files.details_error = None;
    };

    let seed_selected_file = |shared: &sinomo_app::state::Shared| {
        seed_listing(shared);
        let mut files = shared.files_mut();
        files.selected = Some(1);
        files.selected_details = Some(SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj-artifact-bin".to_string(),
            path: "/services/artifact.bin".to_string(),
            kind: "file".to_string(),
            version: "version-42".to_string(),
            content_hash: Some("sha256:8df4e83a7f2d0c1b".to_string()),
            size_bytes: Some(4_194_304),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1_766_000_000_000_000_000,
            modified_at_unix_nanos: 1_766_000_030_000_000_000,
            created_at_unix_nanos: 1_766_000_000_000_000_000,
            changed_at_unix_nanos: 1_766_000_030_000_000_000,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-desktop-a".to_string(), "node-home-server".to_string()],
            local_pins: vec![SifsPinDetails {
                pinned_by_node_id: "node-desktop-a".to_string(),
                mode: "content".to_string(),
                created_at_unix_nanos: Some(1_766_000_040_000_000_000),
            }],
            health: Some(SifsObjectHealth {
                agent_state: "healthy".to_string(),
                version_count: 1,
                receipt_count: 2,
                intent_count: 0,
                open_conflict_count: 0,
                pending_repair_count: 0,
                missing_replica_nodes: Vec::new(),
            }),
        });
    };

    // Dark: root listing + empty inspector ("Select a file to see its SIFS placement.").
    {
        let shared = boot(Tab::Files);
        seed_listing(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle files-dark layout");
        capture("files-composed-dark", &window);
    }

    // Light: the same root listing restyled live by the theme reader.
    {
        let shared = boot(Tab::Files);
        seed_listing(&shared);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle files-light layout");
        capture("files-composed-light", &window);
    }

    // Selected entry: pick a file so the inspector populates placement and pin panels.
    {
        let shared = boot(Tab::Files);
        seed_selected_file(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle files-selected layout");
        capture("files-composed-selected", &window);
    }

    // Narrow: collapse to a list-only column (the inspector is hidden below WIDE_THRESHOLD).
    {
        let shared = boot(Tab::Files);
        seed_listing(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle initial layout");
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))
            .expect("resize narrow");
        app.run_until_idle().expect("settle narrow layout");
        capture("files-composed-narrow", &window);
    }

    // Narrow selected: row actions must stay reachable when the inspector is hidden.
    {
        let shared = boot(Tab::Files);
        seed_selected_file(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle()
            .expect("settle initial selected layout");
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(520.0, 720.0))))
            .expect("resize narrow selected");
        app.run_until_idle().expect("settle narrow selected layout");
        capture("files-composed-narrow-selected", &window);
    }
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

    let shared = boot(Tab::Sinomo);
    seed_chat_providers(&shared);
    let runtime = build_runtime(shared).expect("build runtime");
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

#[test]
fn capture_chat_dropdown() {
    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let shared = boot(Tab::Sinomo);
    seed_chat_providers(&shared);
    let runtime = build_runtime(shared).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle initial layout");

    let before = window
        .capture_screenshot()
        .expect("capture before dropdown");

    // Open the provider selector chip through semantics so the capture stays stable as layout moves.
    let provider_bounds = open_chat_provider_select(&window);

    let shot = window.capture_screenshot().expect("capture screenshot");
    assert_provider_dropdown_visible(&before, &shot, provider_bounds, 2);
    let path = dir.join("sinomo-app-chat-dropdown.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

#[test]
fn new_chat_row_clears_visible_transcript() {
    use sinomo_app::ui::chat::ChatMessage;
    use sui::SemanticsRole;

    let shared = boot(Tab::Sinomo);
    {
        let mut chat = shared.chat_mut();
        chat.messages = vec![
            ChatMessage::user("Keep this out of the next chat"),
            ChatMessage::model("Old answer"),
        ];
        chat.input = "discarded draft".to_string();
        chat.cursor = chat.input.len();
        chat.notice = Some("old notice".to_string());
    }

    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle chat layout");

    window
        .get_by_role(SemanticsRole::ListItem)
        .with_name("New chat")
        .click()
        .expect("click New chat row");
    app.run_until_idle().expect("settle after New chat");

    let chat = shared.chat();
    assert!(chat.messages.is_empty(), "New chat clears visible messages");
    assert!(chat.input.is_empty(), "New chat clears the composer draft");
    assert!(chat.notice.is_none(), "New chat clears the visible notice");
}

/// The Providers settings section: per-provider cards (enabled toggle, API key, default model,
/// base URL, reasoning effort).
#[test]
fn capture_providers_settings() {
    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let (shared, _cleanup) = boot_settings_with_provider_accounts("providers-settings");
    let runtime = build_runtime(shared).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle providers layout");

    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-settings-providers.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

/// The device-login providers (codex / github-copilot): captured in a tall window so the
/// Authentication row (status + Sign-in button) is visible, not just the API-key providers.
#[test]
fn capture_providers_device_login() {
    use sinomo_app::ui::settings::SettingsSection;
    use sui::{Event, Size, WindowEvent};

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let shared = boot(Tab::Settings);
    shared.settings_mut().selected = Some(SettingsSection::Providers);
    let runtime = build_runtime(shared).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle providers layout");
    window
        .root()
        .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(
            1280.0, 1660.0,
        ))))
        .expect("resize tall");
    app.run_until_idle().expect("settle tall layout");

    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-providers-devicelogin.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

/// The Advanced settings section: the generic, type-aware editor listing every `sinomo-config`
/// key (bool → toggle, others → click-to-edit value).
#[test]
fn capture_advanced_settings() {
    use sinomo_app::ui::settings::SettingsSection;

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let shared = boot(Tab::Settings);
    shared.settings_mut().selected = Some(SettingsSection::Configuration);
    let runtime = build_runtime(shared).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle advanced layout");

    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-settings-advanced.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

/// The non-blocking launch: the window opens in a *connecting* state (loading screen), then a
/// background thread's external wake transitions it to the interactive tabs. Asserts the wake
/// mechanism actually drives the UI to ready — not just a screenshot dump.
#[test]
fn capture_connecting_then_ready() {
    use sinomo_app::backend::Backend;
    use sinomo_app::state::{AppState, ConnectEvent, ReadyState, Shared};
    use sui::{CustomEvent, Event};

    let dir = shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    // Launch in the connecting phase: no backend yet, a live status line on a loading screen.
    let shared = Shared::new(AppState::connecting());
    shared.borrow_mut().status = "Starting a local daemon…".to_string();
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle connecting layout");
    assert!(!shared.is_ready(), "app starts in the connecting phase");
    {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join("sinomo-app-connecting.png");
        shot.write_png(&path).expect("write png");
        println!("wrote {}", path.display());
    }

    // The backend arrives: queue the Connected event and deliver the external wake the connect
    // thread would send. The shell drains it, builds the tabs, and becomes interactive.
    let (tx, rx) = std::sync::mpsc::channel();
    shared.borrow_mut().connect_rx = Some(rx);
    tx.send(ConnectEvent::Connected(Box::new(ReadyState::build(
        Backend::local_fallback(),
    ))))
    .expect("queue connected");
    window
        .root()
        .dispatch_event(Event::Custom(CustomEvent::new(sui::EXTERNAL_WAKE_KIND)))
        .expect("dispatch external wake");
    app.run_until_idle().expect("settle ready layout");

    assert!(
        shared.is_ready(),
        "the external wake should drain the connect channel and make the app ready"
    );
    let shot = window.capture_screenshot().expect("capture screenshot");
    let path = dir.join("sinomo-app-connected.png");
    shot.write_png(&path).expect("write png");
    println!("wrote {}", path.display());
}

/// The composed `TabBar` drives the active tab and the Shell relayouts the content to the
/// newly-selected tab. Clicking the "Settings" tab in the strip must flip `active_tab` (the TabBar
/// `on_change` writeback) AND swap the visible content (the Shell's diff-on-redraw relayout, which
/// re-measures/arranges the content `SwitchView`'s newly-active child) — the same behavior the old
/// hand-painted `switch_to` provided.
#[test]
fn shell_tabbar_switches_tab_and_relayouts_content() {
    use sinomo_app::state::Tab;
    use sui::SemanticsRole;

    let shared = boot(Tab::Sinomo);
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    app.run_until_idle().expect("settle initial layout");
    assert_eq!(shared.active_tab(), Tab::Sinomo, "starts on the Sinomo tab");

    let before = window.capture_screenshot().expect("capture before");

    window
        .get_by_role(SemanticsRole::TabBar)
        .with_name("shell-tabs")
        .press("End")
        .expect("activate Settings tab");
    app.run_until_idle().expect("settle tab switch");

    assert_eq!(
        shared.active_tab(),
        Tab::Settings,
        "clicking the Settings tab in the TabBar flips the active tab"
    );

    let after = window.capture_screenshot().expect("capture after");
    let diff = pixel_diff(&before, &after);
    assert!(
        diff > 0,
        "switching tabs must relayout the content to the newly-selected tab's view; got {diff} \
         changed pixels"
    );
}

/// Deterministic captures for the rebuilt (composed) Shell chrome: the ready shell in dark + light
/// (showing the composed `TabBar` tab strip, the elevated/hairlined title bar, and the status pill),
/// plus a title-bar close-up crop. The Shell is a custom widget hosting the chrome as child pods; the
/// content below is whichever tab's view — here the chrome is what we're inspecting. Captures land in
/// `target/shots` so the paths are stable.
#[test]
fn capture_shell_chrome() {
    use sinomo_app::theme::AppThemeMode;
    use sui::Rect;

    let dir = target_shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let write = |shot: &sui_testing::Screenshot, name: &str| {
        let path = dir.join(format!("sinomo-app-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    // Ready shell, dark: TabBar tab strip + elevated title bar + status pill over the tab content.
    {
        let shared = boot(Tab::Sinomo);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle shell-dark layout");
        let shot = window.capture_screenshot().expect("capture screenshot");
        write(&shot, "shell-ready-dark");
        // Title-bar close-up (top strip): the brand mark, TabBar, status pill + the bar shadow.
        let bar = shot
            .crop(Rect::new(0.0, 0.0, shot.width() as f32, 56.0))
            .expect("crop title bar");
        write(&bar, "shell-titlebar-dark");
    }

    // Ready shell, light: the same chrome restyled live by the theme reader.
    {
        let shared = boot(Tab::Sinomo);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle shell-light layout");
        let shot = window.capture_screenshot().expect("capture screenshot");
        write(&shot, "shell-ready-light");
        let bar = shot
            .crop(Rect::new(0.0, 0.0, shot.width() as f32, 56.0))
            .expect("crop title bar");
        write(&bar, "shell-titlebar-light");
    }
}

/// Deterministic captures for the rebuilt (composed) Chat tab: the empty state in dark + light, a
/// populated transcript with a few message bubbles, a mid-stream state (partial agent reply +
/// "thinking…" indicator), and the provider selector open. The transcript is seeded directly via
/// the public `ChatState.messages` field so the captures stay independent of any live model backend.
#[test]
fn capture_chat_composed() {
    use sinomo_app::theme::AppThemeMode;
    use sinomo_app::ui::chat::{ChatMessage, ConversationKind, SessionRef};
    use sui::SemanticsRole;

    // Write these alongside the other shots, but also mirror them under `target/shots` so they are
    // easy to find regardless of the SINOMO_APP_SHOTS override.
    let dir = target_shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let capture = |name: &str, window: &sui_testing::TestWindow| {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join(format!("sinomo-app-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    // A small helper to seed a sample transcript.
    let seed_conversation = |shared: &sinomo_app::state::Shared| {
        let mut chat = shared.chat_mut();
        chat.messages = vec![
            ChatMessage::user("What is SIFS and how does replication work across cluster nodes?"),
            ChatMessage::model(
                "SIFS is Sinomo's distributed filesystem. Objects are content-addressed and \
                       placed on nodes by a placement policy; when you join a cluster, replicas are \
                       spread across members so a single node failure never loses data.",
            ),
            ChatMessage::user("Can I tune the replication factor?"),
            ChatMessage::model(
                "Yes — the replication factor is part of the placement policy and can be set \
                       per-path once the node is clustered.",
            ),
        ];
        chat.stick_bottom = true;
        chat.scroll = f32::MAX;
    };

    let seed_sidebar_sessions = |shared: &sinomo_app::state::Shared| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_secs() as i64;
        let mut chat = shared.chat_mut();
        chat.sessions = vec![
            SessionRef {
                id: "visual-roadmap-review".to_string(),
                title: "Roadmap review".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: now - 5 * 60,
                participant_count: 1,
                participant_names: Vec::new(),
            },
            SessionRef {
                id: "visual-release-triage".to_string(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: now - 2 * 60 * 60,
                participant_count: 2,
                participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
            },
            SessionRef {
                id: "visual-provider-setup".to_string(),
                title: "Provider setup notes".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: now - 26 * 60 * 60,
                participant_count: 1,
                participant_names: Vec::new(),
            },
        ];
        chat.session_id = "visual-roadmap-review".to_string();
        chat.models_loading = false;
    };

    // Empty state, dark: "Chat with {agent}" + hint.
    {
        let shared = boot(Tab::Sinomo);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle chat-empty-dark layout");
        capture("chat-composed-empty-dark", &window);
    }

    // Empty state, light: the same restyled live by the theme reader.
    {
        let shared = boot(Tab::Sinomo);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle()
            .expect("settle chat-empty-light layout");
        capture("chat-composed-empty-light", &window);
    }

    // Sidebar sessions: several chats plus a channel, with the compact row treatment visible.
    {
        let shared = boot(Tab::Sinomo);
        seed_sidebar_sessions(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle()
            .expect("settle chat-sidebar-sessions layout");
        capture("chat-composed-sidebar-sessions", &window);
    }

    // Populated transcript: a few user/agent bubbles, not streaming.
    {
        let shared = boot(Tab::Sinomo);
        seed_conversation(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle chat-messages layout");
        capture("chat-composed-messages", &window);
    }

    // Populated transcript with the DEFAULT provider's models already loaded (Issue 2): the model
    // selector shows a real model, not the "No models" placeholder. Models are normally fetched
    // async at construction; here we seed a default provider + its model list directly so the
    // capture is deterministic and shows the populated picker.
    {
        use sinomo_cli::app_api::ProviderSummary;
        let shared = boot(Tab::Sinomo);
        seed_conversation(&shared);
        {
            let mut chat = shared.chat_mut();
            chat.providers = vec![ProviderSummary {
                id: "anthropic".to_string(),
                account: "default".to_string(),
                name: "Anthropic".to_string(),
                models: vec![
                    "claude-opus-4".to_string(),
                    "claude-sonnet-4".to_string(),
                    "claude-haiku-4".to_string(),
                ],
            }];
            chat.provider_idx = 0;
            chat.model_idx = 0;
            chat.models_loading = false;
        }
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle()
            .expect("settle chat-models-loaded layout");
        capture("chat-composed-messages-models", &window);
    }

    // Streaming: a partial agent reply with the "thinking…" indicator and the Send button swapped
    // for a Spinner.
    {
        let shared = boot(Tab::Sinomo);
        {
            let mut chat = shared.chat_mut();
            chat.messages = vec![
                ChatMessage::user("Summarize how the placement policy chooses nodes."),
                ChatMessage::model(
                    "The placement policy hashes the object key and walks the node ring, \
                           selecting the first healthy members up to the replication factor. It \
                           prefers",
                ),
            ];
            chat.streaming = true;
            chat.notice = Some("thinking…".to_string());
            chat.stick_bottom = true;
            chat.scroll = f32::MAX;
        }
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle chat-streaming layout");
        capture("chat-composed-streaming", &window);
    }

    // Provider/model selector open: seed a couple of providers + models so the menu has options,
    // then open the provider Select through semantics so the capture follows the widget identity
    // instead of a fragile coordinate.
    {
        let shared = boot(Tab::Sinomo);
        seed_conversation(&shared);
        seed_chat_providers(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle chat-selector layout");
        open_chat_provider_select(&window);
        capture("chat-composed-selector-open", &window);
    }

    // Chat edit popup: open through the row's semantic edit button so the capture covers the
    // rename/delete surface without relying on hover coordinates.
    {
        let shared = boot(Tab::Sinomo);
        shared.borrow_mut().theme_mode = AppThemeMode::Light;
        {
            let mut chat = shared.chat_mut();
            let session_id = "visual-chat-edit".to_string();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Roadmap review".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
                participant_names: Vec::new(),
            }];
            chat.session_id = session_id;
            chat.models_loading = false;
        }
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle chat-edit layout");
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Edit Roadmap review")
            .click()
            .expect("open edit dialog");
        app.run_until_idle().expect("settle edit dialog");
        capture("chat-composed-edit-dialog", &window);
    }

    // Channel edit popup: seed a real channel in an isolated SIFS data dir so the dialog shows the
    // editable agent list without mutating the operator's channel index during a visual capture.
    {
        use sinomo_agent::{AgentChannel, AgentChannelStore};
        use sinomo_cli::app_api::SifsAgentChannelStore;
        use sinomo_runtime::events::SessionId;

        let (shared, _visual_data_dir_cleanup) =
            boot_with_visual_data_dir(Tab::Sinomo, "channel-edit");
        let data_dir = shared.borrow().data_dir.clone();
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Release triage");
        channel.ensure_participant("main-agent", "main-agent");
        channel.ensure_participant("coding-agent", "coding-agent");
        let store = SifsAgentChannelStore::open(&data_dir).expect("open channel store");
        store.save_channel(&channel).expect("save visual channel");
        let stored = SifsAgentChannelStore::open(&data_dir)
            .expect("reopen channel store")
            .load_channel(&channel_id)
            .expect("load visual channel")
            .expect("visual channel persisted");
        assert_eq!(stored.enabled_participants_in_order().len(), 2);
        {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_secs() as i64;
            let mut chat = shared.chat_mut();
            let session_id = channel_id.to_string();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: now - 2 * 60 * 60,
                participant_count: 2,
                participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
            }];
            chat.session_id = session_id;
            chat.models_loading = false;
        }
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle channel-edit layout");
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Edit Release triage")
            .click()
            .expect("open channel edit dialog");
        app.run_until_idle().expect("settle channel edit dialog");
        let snapshot = window.snapshot().expect("channel edit snapshot");
        let button_names = snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|node| node.role == SemanticsRole::Button)
            .filter_map(|node| node.name.as_deref())
            .collect::<Vec<_>>()
            .join(", ");
        for label in ["Remove main-agent", "Remove coding-agent"] {
            assert!(
                snapshot.accessibility.nodes.iter().any(|node| {
                    node.role == SemanticsRole::Button && node.name.as_deref() == Some(label)
                }),
                "{label} control should be visible in the channel edit dialog; buttons: {button_names}"
            );
        }
        capture("chat-composed-channel-edit-dialog", &window);
    }
}

/// Deterministic capture for the runtime telemetry chrome: the context budget bar, model usage,
/// active background-task counts, and the right-side activity rail.
#[test]
fn capture_chat_runtime_activity() {
    use sinomo_app::backend::chat_engine::RuntimeInfo;
    use sinomo_app::theme::AppThemeMode;
    use sinomo_app::ui::chat::{ActivityTone, ChatActivityEntry, ChatMessage, ChatToolActivity};
    use sinomo_cli::app_api::{
        ChatContextLane, ChatContextSnapshot, ChatTaskSnapshot, ChatUsageSnapshot,
    };

    let dir = target_shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let capture = |name: &str, window: &sui_testing::TestWindow| {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join(format!("sinomo-app-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    let seed_runtime = |shared: &sinomo_app::state::Shared| {
        let mut chat = shared.chat_mut();
        chat.info = RuntimeInfo {
            agent: "main-agent".to_string(),
            provider: "codex".to_string(),
            model: "gpt-5.5-codex".to_string(),
            account: Some("default".to_string()),
            effort: Some("medium".to_string()),
        };
        chat.messages = vec![
            ChatMessage::user(
                "Audit the daemon startup path and keep running cluster checks in the background.",
            ),
            ChatMessage::model(
                "I am assembling context, checking the recent daemon events, and starting the \
                       focused verification commands now.",
            ),
        ];
        chat.streaming = true;
        chat.notice = Some("session save failed: DFS services shard replay delayed".to_string());
        chat.context = Some(ChatContextSnapshot {
            step_index: 4,
            provider: "codex".to_string(),
            model: "gpt-5.5-codex".to_string(),
            total_bytes: 62_400,
            max_context_bytes: Some(196_608),
            compaction_threshold_bytes: Some(147_456),
            lanes: vec![
                ChatContextLane {
                    kind: "fixed_prompt".to_string(),
                    label: "agent package".to_string(),
                    bytes: 18_200,
                    priority: 100,
                },
                ChatContextLane {
                    kind: "recent_tail".to_string(),
                    label: "recent turn tail".to_string(),
                    bytes: 21_700,
                    priority: 80,
                },
                ChatContextLane {
                    kind: "retrieved_memories".to_string(),
                    label: "retrieved memory".to_string(),
                    bytes: 8_400,
                    priority: 60,
                },
                ChatContextLane {
                    kind: "current_work".to_string(),
                    label: "repo evidence".to_string(),
                    bytes: 14_100,
                    priority: 90,
                },
            ],
            included_count: 17,
            omitted_count: 3,
        });
        chat.usage = Some(ChatUsageSnapshot {
            provider: "codex".to_string(),
            model: "gpt-5.5-codex".to_string(),
            input_tokens: Some(14_204),
            output_tokens: Some(1_168),
        });
        chat.control.active_turn = true;
        chat.control.task_count = 2;
        chat.control.terminal_result_count = 1;
        chat.control.steering_count = 1;
        chat.control.follow_up_count = 1;
        chat.control.runtime_attached = true;
        chat.control.tasks = vec![
            ChatTaskSnapshot {
                task_id: "task-daemon-check".to_string(),
                kind: "verification".to_string(),
                summary: "daemon route audit".to_string(),
                status: "running".to_string(),
                progress_summary: Some("cargo test still streaming".to_string()),
                result_ref: None,
                active_op_id: Some("op-daemon-check".to_string()),
            },
            ChatTaskSnapshot {
                task_id: "task-cluster-gates".to_string(),
                kind: "cluster-check".to_string(),
                summary: "cluster readiness gates".to_string(),
                status: "pending".to_string(),
                progress_summary: Some("queued after daemon audit".to_string()),
                result_ref: None,
                active_op_id: None,
            },
        ];
        chat.tool_calls = vec![
            ChatToolActivity {
                step_index: 4,
                call_index: Some(0),
                call_id: Some("call_exec_daemon".to_string()),
                tool: "exec_command".to_string(),
                input_preview: "cargo test -p sinomo-daemon service_adapter".to_string(),
                status: "completed".to_string(),
            },
            ChatToolActivity {
                step_index: 5,
                call_index: Some(0),
                call_id: Some("call_cluster_gate".to_string()),
                tool: "sinomo test run".to_string(),
                input_preview: "--tag agent-readiness --require-cluster".to_string(),
                status: "running".to_string(),
            },
        ];
        chat.activity = vec![
            ChatActivityEntry {
                title: "context assembled".to_string(),
                detail: "61.0 KiB across 17 model-visible sources".to_string(),
                tone: ActivityTone::Good,
            },
            ChatActivityEntry {
                title: "tool started: exec_command".to_string(),
                detail: "cargo test -p sinomo-daemon service_adapter".to_string(),
                tone: ActivityTone::Running,
            },
            ChatActivityEntry {
                title: "process completed".to_string(),
                detail: "exit 0 in 1842ms, stdout tail preserved".to_string(),
                tone: ActivityTone::Good,
            },
            ChatActivityEntry {
                title: "session save".to_string(),
                detail: "DFS services shard replay delayed".to_string(),
                tone: ActivityTone::Warn,
            },
            ChatActivityEntry {
                title: "model usage".to_string(),
                detail: "14204 input tokens, 1168 output tokens".to_string(),
                tone: ActivityTone::Neutral,
            },
        ];
        chat.stick_bottom = true;
        chat.scroll = f32::MAX;
    };

    for (mode, suffix) in [(AppThemeMode::Dark, "dark"), (AppThemeMode::Light, "light")] {
        let shared = boot(Tab::Sinomo);
        shared.borrow_mut().theme_mode = mode;
        seed_runtime(&shared);
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle()
            .expect("settle chat-runtime-activity layout");
        capture(&format!("chat-runtime-activity-{suffix}"), &window);
    }
}

/// FINAL polish captures (C3 icons + C4 elevation): a clean, clearly-named set so a reviewer can
/// see the finished state of every tab in BOTH themes. Names follow `sinomo-app-final-<area>-<dark|
/// light>.png` and land in `target/shots`.
///
/// Coverage:
/// * Sinomo/chat — a seeded transcript (real message bubbles, now with a subtle drop shadow) +
///   the empty state (real Lucide Sparkles mark) in dark + light.
/// * Files — the root listing + empty inspector, and a selected entry (the inspector node badge now
///   uses a real Lucide HardDrive icon) in dark + light.
/// * Settings — the Providers section and the Advanced section in dark + light.
/// * Chat selector OPEN — the provider `Select` dropdown menu, so the overlay shadow (B3) is visible
///   in the final shot, in dark + light.
#[test]
fn capture_final_polish() {
    use sinomo_app::theme::AppThemeMode;
    use sinomo_app::ui::chat::ChatMessage;
    use sinomo_app::ui::settings::SettingsSection;

    let dir = target_shots_dir();
    std::fs::create_dir_all(&dir).expect("create shots dir");

    let capture = |name: &str, window: &sui_testing::TestWindow| {
        let shot = window.capture_screenshot().expect("capture screenshot");
        let path = dir.join(format!("sinomo-app-final-{name}.png"));
        shot.write_png(&path).expect("write png");
        println!(
            "wrote {} ({}x{})",
            path.display(),
            shot.width(),
            shot.height()
        );
    };

    // Each capture runs from a fresh boot so the captures are independent. A small helper renders a
    // shared state (optionally light) to idle and snapshots it under `name`.
    let render = |name: &str, shared: sinomo_app::state::Shared| {
        let runtime = build_runtime(shared).expect("build runtime");
        let app = TestApp::from_runtime(runtime).expect("headless test app");
        let window = app.main_window().expect("main window");
        app.run_until_idle().expect("settle layout");
        capture(name, &window);
    };

    // A sample transcript so the chat captures show real bubbles (with the new subtle elevation).
    let seed_conversation = |shared: &sinomo_app::state::Shared| {
        let mut chat = shared.chat_mut();
        chat.messages = vec![
            ChatMessage::user("What is SIFS and how does replication work across cluster nodes?"),
            ChatMessage::model(
                "SIFS is Sinomo's distributed filesystem. Objects are content-addressed and \
                       placed on nodes by a placement policy; when you join a cluster, replicas are \
                       spread across members so a single node failure never loses data.",
            ),
            ChatMessage::user("Can I tune the replication factor?"),
            ChatMessage::model(
                "Yes — the replication factor is part of the placement policy and can be set \
                       per-path once the node is clustered.",
            ),
        ];
        chat.stick_bottom = true;
        chat.scroll = f32::MAX;
    };

    for (mode, suffix) in [(AppThemeMode::Dark, "dark"), (AppThemeMode::Light, "light")] {
        // -- Sinomo / chat: seeded transcript. --
        {
            let shared = boot(Tab::Sinomo);
            shared.borrow_mut().theme_mode = mode;
            seed_conversation(&shared);
            seed_chat_providers(&shared);
            seed_healthy_chat_runtime(&shared);
            render(&format!("sinomo-{suffix}"), shared);
        }

        // -- Sinomo / chat: empty state (real Lucide Sparkles mark). --
        {
            let shared = boot(Tab::Sinomo);
            shared.borrow_mut().theme_mode = mode;
            render(&format!("sinomo-empty-{suffix}"), shared);
        }

        // -- Files: root listing + empty inspector. --
        {
            let shared = boot(Tab::Files);
            shared.borrow_mut().theme_mode = mode;
            render(&format!("files-{suffix}"), shared);
        }

        // -- Files: a selected entry so the inspector populates (HardDrive node badge). --
        {
            let shared = boot(Tab::Files);
            shared.borrow_mut().theme_mode = mode;
            if !shared.files().entries.is_empty() {
                shared.files_mut().selected = Some(0);
            }
            render(&format!("files-selected-{suffix}"), shared);
        }

        // -- Settings: Providers section. --
        {
            let shared = boot(Tab::Settings);
            shared.borrow_mut().theme_mode = mode;
            shared.settings_mut().selected = Some(SettingsSection::Providers);
            render(&format!("settings-providers-{suffix}"), shared);
        }

        // -- Settings: Advanced (generic config editor). --
        {
            let shared = boot(Tab::Settings);
            shared.borrow_mut().theme_mode = mode;
            shared.settings_mut().selected = Some(SettingsSection::Configuration);
            render(&format!("settings-advanced-{suffix}"), shared);
        }

        // -- Chat provider Select OPEN: the dropdown overlay should cast a shadow (B3). --
        {
            let shared = boot(Tab::Sinomo);
            shared.borrow_mut().theme_mode = mode;
            seed_conversation(&shared);
            seed_chat_providers(&shared);
            seed_healthy_chat_runtime(&shared);
            let runtime = build_runtime(shared).expect("build runtime");
            let app = TestApp::from_runtime(runtime).expect("headless test app");
            let window = app.main_window().expect("main window");
            app.run_until_idle().expect("settle selector layout");
            // Open the provider Select chip through semantics so the dropdown screenshot is real.
            open_chat_provider_select(&window);
            capture(&format!("chat-dropdown-{suffix}"), &window);
        }
    }
}

/// `target/shots` under the crate (sibling to `target/debug`), so captures are easy to locate.
fn target_shots_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("target")
        .join("shots")
}

/// Count pixels that differ between two same-sized RGBA screenshots.
fn pixel_diff(a: &sui_testing::Screenshot, b: &sui_testing::Screenshot) -> usize {
    let (pa, pb) = (a.pixels(), b.pixels());
    if pa.len() != pb.len() {
        return pa.len().max(pb.len());
    }
    pa.chunks_exact(4)
        .zip(pb.chunks_exact(4))
        .filter(|(x, y)| x != y)
        .count()
}

fn assert_provider_dropdown_visible(
    before: &sui_testing::Screenshot,
    after: &sui_testing::Screenshot,
    select_bounds: sui::Rect,
    option_count: usize,
) {
    let row_height = select_bounds.height().max(1.0);
    let menu_height = row_height * option_count as f32;
    let below = sui::Rect::new(
        select_bounds.x(),
        select_bounds.max_y() + 6.0,
        select_bounds.width(),
        menu_height,
    );
    let above = sui::Rect::new(
        select_bounds.x(),
        select_bounds.y() - 6.0 - menu_height,
        select_bounds.width(),
        menu_height,
    );
    let changed_pixels =
        pixel_diff_in_rect(before, after, below).max(pixel_diff_in_rect(before, after, above));

    assert!(
        changed_pixels > 400,
        "expanded provider Select should visibly paint its menu outside the chip; \
         changed_pixels={changed_pixels}, select_bounds={select_bounds:?}"
    );
}

fn pixel_diff_in_rect(
    a: &sui_testing::Screenshot,
    b: &sui_testing::Screenshot,
    rect: sui::Rect,
) -> usize {
    if a.width() != b.width() || a.height() != b.height() {
        return a
            .pixels()
            .len()
            .max(b.pixels().len())
            .checked_div(4)
            .unwrap_or(0);
    }

    let left = rect.x().floor().max(0.0) as u32;
    let top = rect.y().floor().max(0.0) as u32;
    let right = rect.max_x().ceil().max(0.0) as u32;
    let bottom = rect.max_y().ceil().max(0.0) as u32;
    let right = right.min(a.width());
    let bottom = bottom.min(a.height());
    if left >= right || top >= bottom {
        return 0;
    }

    let mut changed = 0usize;
    let a_pixels = a.pixels();
    let b_pixels = b.pixels();
    for y in top..bottom {
        for x in left..right {
            let offset = ((y * a.width() + x) * 4) as usize;
            if a_pixels[offset..offset + 4] != b_pixels[offset..offset + 4] {
                changed += 1;
            }
        }
    }
    changed
}

/// Issue 1 (Enter-to-send): a plain Enter dispatched to the focused composer must start a turn
/// (the `TextArea::on_submit` -> `ChatState::submit` wiring), not merely insert a newline.
#[test]
fn enter_in_composer_sends_the_message() {
    use sui::{
        Event, KeyState, KeyboardEvent, Point, PointerButton, PointerEvent, PointerEventKind,
    };

    let shared = boot(Tab::Sinomo);
    seed_chat_providers(&shared);
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle initial layout");

    // Focus the composer input near the bottom of the window.
    let pos = Point::new(600.0, 675.0);
    let mut down = PointerEvent::new(PointerEventKind::Down, pos);
    down.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(down)).expect("down");
    let mut up = PointerEvent::new(PointerEventKind::Up, pos);
    up.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(up)).expect("up");

    // Type a short message.
    for key in ["H", "i"] {
        root.dispatch_event(Event::Keyboard(KeyboardEvent::new(key, KeyState::Pressed)))
            .expect("key");
    }
    app.run_until_idle().expect("settle typing");
    assert_eq!(
        shared.chat().input,
        "Hi",
        "the draft captured the keystrokes"
    );
    assert!(!shared.chat().streaming, "no turn until Enter is pressed");

    // A plain Enter (no Shift) submits.
    root.dispatch_event(Event::Keyboard(KeyboardEvent::new(
        "Enter",
        KeyState::Pressed,
    )))
    .expect("enter");

    // `submit()` synchronously pushes the user message and clears the draft (deterministic, before
    // the worker thread does anything). The streaming flag is set true synchronously too; the worker
    // may or may not have flipped it back by now, so the load-bearing assertion is that the message
    // was sent — not merely that a newline was inserted.
    assert_eq!(
        shared.chat().messages.first().map(|m| m.text.clone()),
        Some("Hi".to_string()),
        "plain Enter sends the typed draft as a user message (Enter-to-send)"
    );
    assert!(
        shared.chat().input.is_empty(),
        "the draft is cleared after Enter sends it (not a newline inserted)"
    );
}

/// Issue 1 (Shift+Enter): Shift+Enter must NOT send — it inserts a newline into the draft.
#[test]
fn shift_enter_in_composer_inserts_a_newline() {
    use sui::{
        Event, KeyState, KeyboardEvent, Point, PointerButton, PointerEvent, PointerEventKind,
    };

    let shared = boot(Tab::Sinomo);
    seed_chat_providers(&shared);
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle initial layout");

    let pos = Point::new(600.0, 675.0);
    let mut down = PointerEvent::new(PointerEventKind::Down, pos);
    down.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(down)).expect("down");
    let mut up = PointerEvent::new(PointerEventKind::Up, pos);
    up.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(up)).expect("up");

    for key in ["a", "b"] {
        root.dispatch_event(Event::Keyboard(KeyboardEvent::new(key, KeyState::Pressed)))
            .expect("key");
    }
    // Shift+Enter: a newline, not a submit.
    let mut shift_enter = KeyboardEvent::new("Enter", KeyState::Pressed);
    shift_enter.modifiers.shift = true;
    root.dispatch_event(Event::Keyboard(shift_enter))
        .expect("shift+enter");
    app.run_until_idle().expect("settle");

    assert!(!shared.chat().streaming, "Shift+Enter must not send");
    assert!(
        shared.chat().input.contains('\n'),
        "Shift+Enter inserts a newline into the draft (draft = {:?})",
        shared.chat().input
    );
}

/// When no provider is configured, the composer should read as setup-required rather than accepting
/// a draft that cannot be sent. Connection recovery may be visible at the same time, so the selector
/// still exposes provider setup as a separate prerequisite.
#[test]
fn composer_without_provider_is_read_only_and_opens_setup() {
    use sui::{
        Event, KeyState, KeyboardEvent, Point, PointerButton, PointerEvent, PointerEventKind,
        SemanticsRole,
    };

    let shared = boot(Tab::Sinomo);
    {
        let mut chat = shared.chat_mut();
        chat.providers.clear();
        chat.models_loading = false;
        chat.notice =
            Some("chat list failed: chat persistence requires a daemon connection".to_string());
    }

    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle initial layout");

    let pos = Point::new(600.0, 675.0);
    let mut down = PointerEvent::new(PointerEventKind::Down, pos);
    down.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(down)).expect("down");
    let mut up = PointerEvent::new(PointerEventKind::Up, pos);
    up.button = Some(PointerButton::Primary);
    root.dispatch_event(Event::Pointer(up)).expect("up");

    for key in ["H", "i"] {
        root.dispatch_event(Event::Keyboard(KeyboardEvent::new(key, KeyState::Pressed)))
            .expect("key");
    }
    root.dispatch_event(Event::Keyboard(KeyboardEvent::new(
        "Enter",
        KeyState::Pressed,
    )))
    .expect("enter");
    app.run_until_idle().expect("settle typing attempt");

    assert!(
        shared.chat().input.is_empty(),
        "no-provider composer must not accept a draft"
    );
    assert!(
        shared.chat().messages.is_empty(),
        "Enter on the no-provider composer must not send a turn"
    );
    assert!(
        !shared.chat().streaming,
        "Enter on the no-provider composer must not start streaming"
    );

    window
        .get_by_role(SemanticsRole::Button)
        .with_name("Setup provider")
        .click()
        .expect("click Setup provider");

    assert_eq!(shared.borrow().active_tab, Tab::Settings);
    let settings = shared.settings();
    assert_eq!(
        settings.selected,
        Some(sinomo_app::ui::settings::SettingsSection::Providers)
    );
    assert!(settings.provider_add_open);
}

/// Issue 3 (relayout outside a pointer gesture): when `streaming` flips while the request-animation
/// -frame pump is alive — as a `TurnDone` worker event does, with no pointer gesture — the composer
/// must relayout (Spinner -> Send), not just sit stale. Drive the pump with animation frames and
/// assert the composer pixels change across the streaming on/off transition.
#[test]
fn pump_relayouts_composer_when_streaming_flips() {
    use sinomo_app::ui::chat::ChatMessage;
    use sui::{Event, Point, PointerEvent, PointerEventKind};

    let shared = boot(Tab::Sinomo);
    {
        let mut chat = shared.chat_mut();
        chat.messages = vec![
            ChatMessage::user("Hello?"),
            ChatMessage::model("Working on it"),
        ];
        chat.streaming = true;
        chat.notice = Some("thinking…".to_string());
        chat.stick_bottom = true;
        chat.scroll = f32::MAX;
    }
    let runtime = build_runtime(shared.clone()).expect("build runtime");
    let app = TestApp::from_runtime(runtime).expect("headless test app");
    let window = app.main_window().expect("main window");
    let root = window.root();
    app.run_until_idle().expect("settle streaming layout");

    // The composer shows a Spinner while streaming.
    let streaming_shot = window.capture_screenshot().expect("capture streaming");

    // Flip streaming off the way a `TurnDone` worker event would — with NO pointer gesture on the
    // composer/sidebar. Then nudge the pump alive with a bare pointer move over the transcript (not
    // a click, so no selection changes and no `RebuildOnChange` rebuilds from the gesture itself);
    // the pump's next tick must notice the signature change and force a full relayout that rebuilds
    // the composer (Spinner -> Send). The move's dispatch runs the harness to idle, delivering that
    // pump frame.
    shared.chat_mut().streaming = false;
    root.dispatch_event(Event::Pointer(PointerEvent::new(
        PointerEventKind::Move,
        Point::new(600.0, 300.0),
    )))
    .expect("move");
    app.run_until_idle().expect("settle pump-driven relayout");

    let settled_shot = window.capture_screenshot().expect("capture settled");

    assert!(
        !shared.chat().streaming,
        "streaming was flipped off (TurnDone-style)"
    );
    let diff = pixel_diff(&streaming_shot, &settled_shot);
    assert!(
        diff > 0,
        "the pump must relayout the composer (Spinner -> Send) when streaming flips outside a \
         pointer gesture; got {diff} changed pixels"
    );
}
