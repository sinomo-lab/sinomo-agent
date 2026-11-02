//! Goal, revision-DAG, metric, agent, and resource monitoring surface.

use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::Duration;

use sinomo_client::evolution_contract as contract;
use sui::{
    Alignment, Axis, Button, ButtonAppearance, Flex, FlexItem, FontWeight, Insets, KeyedStack,
    Label, Presence, RebuildOnChange, ScrollState, ScrollView, SemanticTone, StatusBadge, Surface,
    SurfaceAppearance, SwitchView, Widget, WidgetPod,
};

use crate::backend::Backend;
use crate::state::{Shared, Tab};
use crate::ui::offline::{DaemonOfflinePanel, daemon_gate};
use crate::ui::widgets;

#[derive(Debug)]
pub struct EvolutionState {
    pub dashboard: contract::EvolutionDashboard,
    pub refreshing: bool,
    refresh_rx: Option<Receiver<Result<contract::EvolutionDashboard, String>>>,
    details_open: sui::Signal<bool>,
}

impl EvolutionState {
    pub fn new(_backend: &Backend) -> Self {
        Self {
            dashboard: contract::EvolutionDashboard::default(),
            refreshing: false,
            refresh_rx: None,
            details_open: sui::Signal::new(false),
        }
    }

    pub fn details_open(&self) -> sui::Signal<bool> {
        self.details_open.clone()
    }

    fn begin_refresh(&mut self, address: Option<String>, waker: Option<sui::UiHandle>) {
        if self.refreshing {
            return;
        }
        let Some(address) = address else {
            self.dashboard.error = Some(
                "Evolution monitoring requires a daemon connection and active evolution module"
                    .to_string(),
            );
            return;
        };
        let (tx, rx) = mpsc::channel();
        self.refresh_rx = Some(rx);
        self.refreshing = true;
        thread::spawn(move || {
            let result = sinomo_client::EvolutionClient::at_address(address)
                .with_timeout(Duration::from_secs(10))
                .dashboard()
                .map_err(|error| error.to_string());
            let _ = tx.send(result);
            if let Some(waker) = waker {
                waker.wake();
            }
        });
    }

    fn begin_control(
        &mut self,
        address: Option<String>,
        waker: Option<sui::UiHandle>,
        request: contract::EvolutionGoalControlRequest,
    ) {
        if self.refreshing {
            return;
        }
        let Some(address) = address else {
            self.dashboard.error = Some(
                "Evolution controls require a daemon connection and active evolution module"
                    .to_string(),
            );
            return;
        };
        let (tx, rx) = mpsc::channel();
        self.refresh_rx = Some(rx);
        self.refreshing = true;
        thread::spawn(move || {
            let client = sinomo_client::EvolutionClient::at_address(address)
                .with_timeout(Duration::from_secs(10));
            let result = client
                .control_goal(request)
                .and_then(|_| client.dashboard())
                .map_err(|error| error.to_string());
            let _ = tx.send(result);
            if let Some(waker) = waker {
                waker.wake();
            }
        });
    }

    fn pump_refresh(&mut self) -> bool {
        let Some(rx) = &self.refresh_rx else {
            return false;
        };
        let result = match rx.try_recv() {
            Ok(result) => result,
            Err(TryRecvError::Empty) => return false,
            Err(TryRecvError::Disconnected) => Err("evolution refresh stopped".to_string()),
        };
        self.refresh_rx = None;
        self.refreshing = false;
        match result {
            Ok(dashboard) => self.dashboard = dashboard,
            Err(error) => self.dashboard.error = Some(error),
        }
        true
    }
}

pub fn request_refresh(shared: &Shared) {
    // The tab shows the offline panel while the daemon is unreachable; polling it would only
    // collect errors nobody sees.
    if !shared.is_ready() || !shared.daemon_availability().is_available() {
        return;
    }
    let (address, waker) = {
        let state = shared.borrow();
        let ready = state.ready.as_ref().expect("app is ready");
        (
            ready
                .backend
                .connection
                .daemon_address()
                .map(str::to_string),
            state.waker.clone(),
        )
    };
    shared.evolution_mut().begin_refresh(address, waker);
}

pub fn pump_refresh(shared: &Shared) -> bool {
    if !shared.is_ready() {
        return false;
    }
    shared.evolution_mut().pump_refresh()
}

fn control_goal(shared: &Shared, goal_id: String, action: contract::EvolutionGoalControlAction) {
    if !shared.is_ready() {
        return;
    }
    let (address, waker) = {
        let state = shared.borrow();
        let ready = state.ready.as_ref().expect("app is ready");
        (
            ready
                .backend
                .connection
                .daemon_address()
                .map(str::to_string),
            state.waker.clone(),
        )
    };
    shared.evolution_mut().begin_control(
        address,
        waker,
        contract::EvolutionGoalControlRequest {
            goal_id,
            action,
            note: "application workflow control".to_string(),
        },
    );
}

/// What evolution needs the daemon for, shown on the offline panel.
pub(crate) const EVOLUTION_OFFLINE_SUMMARY: &str =
    "Evolution goals, revisions, and metrics come from the daemon.";
/// The command that proposes a goal; goals are proposed from a terminal for now.
pub(crate) const PROPOSE_GOAL_COMMAND: &str = "sinomo evolve propose \"<goal>\"";

/// The dashboard projection the retained page binds to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EvolutionView {
    pub dashboard: Arc<contract::EvolutionDashboard>,
    pub refreshing: bool,
}

impl EvolutionView {
    pub(crate) fn from_state(state: &EvolutionState) -> Self {
        Self {
            dashboard: Arc::new(state.dashboard.clone()),
            refreshing: state.refreshing,
        }
    }

    fn error(&self) -> Option<&str> {
        self.dashboard
            .error
            .as_deref()
            .filter(|error| !error.trim().is_empty())
    }

    fn summary(&self) -> String {
        if self.error().is_some() {
            return "Status unavailable".into();
        }
        let dashboard = &self.dashboard;
        format!(
            "{} goals · {} revisions · {} active goals",
            dashboard.goals.len(),
            dashboard
                .goals
                .iter()
                .map(|goal| goal.revisions.len())
                .sum::<usize>(),
            dashboard.active_goal_count()
        )
    }

    /// Which goals body shows: loading, the empty card, the goal list, or nothing beside an
    /// error.
    fn body(&self) -> usize {
        if !self.dashboard.goals.is_empty() {
            2
        } else if self.error().is_some() {
            3
        } else if self.refreshing {
            0
        } else {
            1
        }
    }
}

type ThemeReader = Rc<dyn Fn() -> sui::DefaultTheme>;

fn clone_reader(reader: &ThemeReader) -> impl Fn() -> sui::DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

pub fn view(shared: Shared) -> impl Widget {
    let reader: ThemeReader = {
        let shared = shared.clone();
        Rc::new(move || shared.theme())
    };
    let dashboard = build_dashboard(&shared, &reader);
    let offline = DaemonOfflinePanel::new(
        &shared,
        &reader,
        "Evolution offline",
        EVOLUTION_OFFLINE_SUMMARY,
    )
    .build();
    daemon_gate(&shared, dashboard, offline)
}

/// The page stays mounted: refreshes update its bindings, and only a goal whose own summary
/// changed rebuilds its card.
fn build_dashboard(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let view = shared.signals().evolution.clone();
    let details = shared.evolution().details_open();
    let refresh_shared = shared.clone();
    let refresh = widgets::Restyle::new(
        Button::new("Refresh")
            .theme_when(clone_reader(reader))
            .appearance(ButtonAppearance::Tonal)
            .on_press(move || request_refresh(&refresh_shared)),
        view.select_named("evolution refreshing", |view| view.refreshing),
        |mut button: Button, refreshing: &bool| {
            button.set_label(if *refreshing {
                "Refreshing…"
            } else {
                "Refresh"
            });
            button.enabled(!refreshing)
        },
    );
    let header = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(10.0)
        .with_item(
            Flex::vertical()
                .gap(2.0)
                .with_child(
                    weighted_label("Evolution", reader, FontWeight::SEMIBOLD)
                        .semantic_name("Evolution goals"),
                )
                .with_child(
                    Label::new(view.get().summary())
                        .text_from(view.select_named("evolution summary", EvolutionView::summary))
                        .theme_when(clone_reader(reader)),
                ),
            FlexItem::new().grow(1.0),
        )
        .with_child(refresh);

    let toggle_details = details.clone();
    let details_button = widgets::Restyle::new(
        Button::new("Technical details")
            .theme_when(clone_reader(reader))
            .on_press(move || {
                toggle_details.set(!toggle_details.get());
            }),
        details.clone(),
        |mut button: Button, open: &bool| {
            button.set_label(if *open {
                "Hide details"
            } else {
                "Technical details"
            });
            button
        },
    );
    let unavailable = Surface::panel(
        Flex::vertical()
            .gap(10.0)
            .with_child(weighted_label("Evolution unavailable", reader, FontWeight::SEMIBOLD))
            .with_child(
                Label::new(
                    "Evolution could not be loaded from this node. Check that a capable node is connected, then refresh.",
                )
                .theme_when(clone_reader(reader)),
            )
            .with_child(details_button)
            .with_child(
                Presence::new(
                    Label::new("")
                        .text_from(view.select_named("evolution error", |view| {
                            view.error().unwrap_or_default().to_string()
                        }))
                        .theme_when(clone_reader(reader)),
                )
                .transition(crate::ui::widgets::reveal_transition())
                .shown_from(details)
                .collapse(Axis::Vertical),
            ),
    )
    .appearance(SurfaceAppearance::Raised)
    .padding(Insets::all(12.0))
    .theme_when(clone_reader(reader));

    let goals_shared = shared.clone();
    let goals_reader = Rc::clone(reader);
    let goals = KeyedStack::vertical(
        view.select_named("evolution goals", |view| view.dashboard.goals.clone()),
        |goal: &contract::EvolutionGoalSummary| goal.id.clone(),
        move |_, goal| {
            let shared = goals_shared.clone();
            let reader = Rc::clone(&goals_reader);
            RebuildOnChange::key_from(goal, move |goal| {
                WidgetPod::new(goal_card(goal.clone(), &reader, &shared))
            })
        },
    )
    .gap(12.0);
    let body = SwitchView::new()
        .selected_from(view.select_named("evolution body", EvolutionView::body))
        .with_child(
            Surface::panel(Label::new("Loading Evolution…").theme_when(clone_reader(reader)))
                .padding(Insets::all(16.0))
                .theme_when(clone_reader(reader)),
        )
        .with_child(empty_goals_card(reader))
        .with_child(goals)
        .with_child(sui::SizedBox::new());

    let content = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(12.0)
        .with_child(
            Surface::window(header)
                .padding(Insets::all(14.0))
                .fill()
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            Presence::new(unavailable)
                .transition(crate::ui::widgets::reveal_transition())
                .shown_from(
                    view.select_named("evolution unavailable", |view| view.error().is_some()),
                )
                .collapse(Axis::Vertical),
        )
        .with_child(body);
    ScrollView::vertical(
        Surface::window(content)
            .padding(Insets::all(16.0))
            .fill()
            .theme_when(clone_reader(reader)),
    )
    .state(ScrollState::new())
    .name("Evolution goal dashboard")
    .theme_when(clone_reader(reader))
    .viewport_width_hint(true)
}

fn empty_goals_card(reader: &ThemeReader) -> Surface {
    let style_reader = Rc::clone(reader);
    let command = Surface::panel(
        Label::new(PROPOSE_GOAL_COMMAND)
            .theme_when(clone_reader(reader))
            .single_line(true)
            .text_style_when(move || {
                let theme = style_reader();
                let mut style = theme.mono_text_style(theme.palette.text);
                style.font_size = theme.text.sm.size;
                style.line_height = theme.text.sm.line_height;
                style
            }),
    )
    .appearance(SurfaceAppearance::Soft)
    .padding(Insets {
        left: 10.0,
        top: 6.0,
        right: 10.0,
        bottom: 6.0,
    })
    .theme_when(clone_reader(reader));
    let copy = Button::new("Copy command")
        .theme_when(clone_reader(reader))
        .appearance(ButtonAppearance::Tonal)
        .on_press_with_ctx(|ctx| ctx.set_clipboard_text(PROPOSE_GOAL_COMMAND));
    Surface::panel(
        Flex::vertical()
            .gap(8.0)
            .with_child(weighted_label(
                "No evolution goals yet",
                reader,
                FontWeight::SEMIBOLD,
            ))
            .with_child(
                Label::new(
                    "Propose a goal from a terminal. Its revisions and metrics appear here.",
                )
                .theme_when(clone_reader(reader)),
            )
            .with_child(
                Flex::horizontal()
                    .gap(8.0)
                    .align_items(Alignment::Center)
                    .with_child(command)
                    .with_child(copy),
            ),
    )
    .appearance(SurfaceAppearance::Raised)
    .padding(Insets::all(16.0))
    .theme_when(clone_reader(reader))
}

fn goal_card(
    goal: contract::EvolutionGoalSummary,
    reader: &ThemeReader,
    shared: &Shared,
) -> Surface {
    let mut body = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
    body.push(
        Flex::horizontal()
            .align_items(Alignment::Center)
            .gap(8.0)
            .with_item(
                Flex::vertical()
                    .gap(2.0)
                    .with_child(weighted_label(
                        goal.objective.clone(),
                        reader,
                        FontWeight::SEMIBOLD,
                    ))
                    .with_child(
                        Label::new(format!(
                            "{} · governor {} · {} events",
                            goal.id, goal.evolution_agent_id, goal.event_count
                        ))
                        .theme_when(clone_reader(reader)),
                    ),
                FlexItem::new().grow(1.0),
            )
            .with_child(
                StatusBadge::new(goal.status.clone())
                    .tone(status_tone(&goal.status))
                    .theme_when(clone_reader(reader)),
            ),
    );

    let mut revision_rows = Flex::vertical().align_items(Alignment::Stretch).gap(5.0);
    revision_rows.push(weighted_label("Revision DAG", reader, FontWeight::SEMIBOLD));
    for revision in &goal.revisions {
        let parents = if revision.parent_ids.is_empty() {
            "root".to_string()
        } else {
            revision.parent_ids.join(" + ")
        };
        let head = if goal.head_revision_ids.contains(&revision.id) {
            " · head"
        } else {
            ""
        };
        revision_rows.push(
            Flex::horizontal()
                .align_items(Alignment::Center)
                .gap(8.0)
                .with_child(
                    StatusBadge::new(revision.status.clone())
                        .tone(status_tone(&revision.status))
                        .theme_when(clone_reader(reader)),
                )
                .with_item(
                    Label::new(format!(
                        "{} → {}{} · {} · {} artifacts · {} metrics",
                        parents,
                        revision.id,
                        head,
                        revision.content_kind,
                        revision.artifacts.len(),
                        revision.metrics.len()
                    ))
                    .theme_when(clone_reader(reader)),
                    FlexItem::new().grow(1.0),
                ),
        );
        for metric in &revision.metrics {
            revision_rows.push(
                Label::new(format!(
                    "    {} = {}{}",
                    metric.metric_id,
                    metric.value,
                    metric
                        .sample_count
                        .map(|count| format!(" (n={count})"))
                        .unwrap_or_default()
                ))
                .theme_when(clone_reader(reader)),
            );
        }
    }
    body.push(
        Surface::panel(revision_rows)
            .padding(Insets::all(10.0))
            .theme_when(clone_reader(reader)),
    );

    if !goal.tasks.is_empty() || !goal.agent_runs.is_empty() {
        let mut workflow = Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(5.0)
            .with_child(weighted_label(
                "Workflow and agents",
                reader,
                FontWeight::SEMIBOLD,
            ));
        for task in &goal.tasks {
            workflow.push(
                Flex::horizontal()
                    .align_items(Alignment::Center)
                    .gap(8.0)
                    .with_child(
                        StatusBadge::new(task.status.clone())
                            .tone(status_tone(&task.status))
                            .theme_when(clone_reader(reader)),
                    )
                    .with_item(
                        Label::new(format!(
                            "Task {} · {} · agent {}",
                            task.id,
                            task.title,
                            task.assigned_agent_run_id
                                .as_deref()
                                .unwrap_or("unassigned")
                        ))
                        .theme_when(clone_reader(reader)),
                        FlexItem::new().grow(1.0),
                    ),
            );
        }
        for run in &goal.agent_runs {
            workflow.push(
                Flex::horizontal()
                    .align_items(Alignment::Center)
                    .gap(8.0)
                    .with_child(
                        StatusBadge::new(run.status.clone())
                            .tone(status_tone(&run.status))
                            .theme_when(clone_reader(reader)),
                    )
                    .with_item(
                        Label::new(format!(
                            "{} · {} · {}{}",
                            run.role,
                            run.agent_id,
                            run.summary,
                            run.session_ref
                                .as_deref()
                                .map(|session| format!(" · {session}"))
                                .unwrap_or_default()
                        ))
                        .theme_when(clone_reader(reader)),
                        FlexItem::new().grow(1.0),
                    ),
            );
        }
        body.push(
            Surface::panel(workflow)
                .padding(Insets::all(10.0))
                .theme_when(clone_reader(reader)),
        );
    }

    let cost = goal
        .resources
        .iter()
        .map(|record| format!("{} {} {}", record.quantity, record.unit, record.resource))
        .collect::<Vec<_>>()
        .join(" · ");
    body.push(
        Label::new(format!(
            "{} tasks · {} agent runs · resources: {}",
            goal.tasks.len(),
            goal.agent_runs.len(),
            if cost.is_empty() {
                "none recorded"
            } else {
                &cost
            }
        ))
        .theme_when(clone_reader(reader)),
    );
    if let Some(event) = goal.recent_events.last() {
        body.push(
            Label::new(format!(
                "Latest activity: {} · {} · {}",
                event.kind, event.actor, event.occurred_at
            ))
            .theme_when(clone_reader(reader)),
        );
    }
    let pause_action = if goal.status == "paused" {
        contract::EvolutionGoalControlAction::Resume
    } else {
        contract::EvolutionGoalControlAction::Pause
    };
    let pause_label = if goal.status == "paused" {
        "Resume"
    } else {
        "Pause"
    };
    let pause_shared = shared.clone();
    let pause_goal_id = goal.id.clone();
    let complete_shared = shared.clone();
    let complete_goal_id = goal.id.clone();
    let cancel_shared = shared.clone();
    let cancel_goal_id = goal.id.clone();
    let terminal = matches!(goal.status.as_str(), "complete" | "cancelled" | "failed");
    let chat_shared = shared.clone();
    body.push(
        Flex::horizontal()
            .gap(8.0)
            .with_child(
                Button::new("Open governor chat")
                    .theme_when(clone_reader(reader))
                    .appearance(ButtonAppearance::Ghost)
                    .on_press(move || {
                        chat_shared.chat_mut().start_agent_chat("evolution-agent");
                        chat_shared.set_active_tab(Tab::Sinomo);
                    }),
            )
            .with_child(
                Button::new(pause_label)
                    .theme_when(clone_reader(reader))
                    .appearance(ButtonAppearance::Tonal)
                    .enabled(!terminal)
                    .on_press(move || {
                        control_goal(&pause_shared, pause_goal_id.clone(), pause_action)
                    }),
            )
            .with_child(
                Button::new("Complete")
                    .theme_when(clone_reader(reader))
                    .appearance(ButtonAppearance::Tonal)
                    .tone(SemanticTone::Success)
                    .enabled(!terminal)
                    .on_press(move || {
                        control_goal(
                            &complete_shared,
                            complete_goal_id.clone(),
                            contract::EvolutionGoalControlAction::Complete,
                        )
                    }),
            )
            .with_child(
                Button::new("Cancel")
                    .theme_when(clone_reader(reader))
                    .appearance(ButtonAppearance::Outline)
                    .tone(SemanticTone::Danger)
                    .enabled(!terminal)
                    .on_press(move || {
                        control_goal(
                            &cancel_shared,
                            cancel_goal_id.clone(),
                            contract::EvolutionGoalControlAction::Cancel,
                        )
                    }),
            ),
    );
    Surface::panel(body)
        .name(format!("Evolution goal {}", goal.id))
        .appearance(SurfaceAppearance::Raised)
        .padding(Insets::all(14.0))
        .theme_when(clone_reader(reader))
}

fn status_tone(status: &str) -> SemanticTone {
    match status {
        "complete" | "selected" | "baseline" => SemanticTone::Success,
        "failed" | "rejected" | "cancelled" | "rolled back" => SemanticTone::Danger,
        "awaiting decision" | "paused" | "waiting" => SemanticTone::Warning,
        "running" | "evaluating" => SemanticTone::Accent,
        _ => SemanticTone::Neutral,
    }
}

fn weighted_label(text: impl Into<String>, reader: &ThemeReader, weight: FontWeight) -> Label {
    let style_reader = Rc::clone(reader);
    Label::new(text)
        .theme_when(clone_reader(reader))
        .text_style_when(move || {
            let mut style = style_reader().body_text_style();
            style.weight = weight;
            style
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_tones_keep_failures_and_active_work_distinct() {
        assert_eq!(status_tone("failed"), SemanticTone::Danger);
        assert_eq!(status_tone("running"), SemanticTone::Accent);
        assert_eq!(status_tone("awaiting decision"), SemanticTone::Warning);
    }

    fn evolution_app(shared: &Shared) -> sui::Result<sui_testing::TestApp> {
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Evolution")
                    .root(view(shared.clone())),
            )
            .build()?;
        sui_testing::TestApp::from_runtime(runtime)
    }

    #[test]
    fn offline_evolution_shows_the_daemon_panel_and_stops_polling() -> sui::Result<()> {
        let shared = crate::state::Shared::new(crate::state::AppState::ready(
            crate::ui::offline::offline_test_backend(),
        ));
        let app = evolution_app(&shared)?;
        let window = app.main_window()?;
        window
            .get_by_role(sui::SemanticsRole::Text)
            .with_name(EVOLUTION_OFFLINE_SUMMARY)
            .expect()
            .to_be_visible()?;

        request_refresh(&shared);
        let evolution = shared.evolution();
        assert!(!evolution.refreshing, "offline refreshes are skipped");
        assert_eq!(
            evolution.dashboard.error, None,
            "a skipped refresh does not pile an error behind the offline panel"
        );
        Ok(())
    }

    #[test]
    fn empty_goals_offer_the_propose_command() -> sui::Result<()> {
        let shared = crate::state::Shared::new(crate::state::AppState::ready(
            crate::backend::Backend::local_fallback(),
        ));
        let app = evolution_app(&shared)?;
        let window = app.main_window()?;
        window
            .get_by_role(sui::SemanticsRole::Text)
            .with_name(PROPOSE_GOAL_COMMAND)
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy command")
            .expect()
            .to_be_visible()?;
        Ok(())
    }
}
