//! Goal, revision-DAG, metric, agent, and resource monitoring surface.

use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::Duration;

use sinomo_client::evolution_contract as contract;
use sui::{
    Alignment, Button, ButtonAppearance, Flex, FlexItem, FontWeight, Insets, Label,
    RebuildOnChange, ScrollView, SemanticTone, StatusBadge, Surface, SurfaceAppearance, Widget,
    WidgetPod,
};

use crate::backend::Backend;
use crate::state::{Shared, Tab};

#[derive(Debug)]
pub struct EvolutionState {
    pub dashboard: contract::EvolutionDashboard,
    pub refreshing: bool,
    pub revision: u64,
    refresh_rx: Option<Receiver<Result<contract::EvolutionDashboard, String>>>,
}

impl EvolutionState {
    pub fn new(_backend: &Backend) -> Self {
        Self {
            dashboard: contract::EvolutionDashboard::default(),
            refreshing: false,
            revision: 1,
            refresh_rx: None,
        }
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
            self.revision = self.revision.saturating_add(1);
            return;
        };
        let (tx, rx) = mpsc::channel();
        self.refresh_rx = Some(rx);
        self.refreshing = true;
        self.revision = self.revision.saturating_add(1);
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
            self.revision = self.revision.saturating_add(1);
            return;
        };
        let (tx, rx) = mpsc::channel();
        self.refresh_rx = Some(rx);
        self.refreshing = true;
        self.revision = self.revision.saturating_add(1);
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
        self.revision = self.revision.saturating_add(1);
        true
    }
}

pub fn request_refresh(shared: &Shared) {
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

pub fn view(shared: Shared) -> impl Widget {
    let source = shared
        .signals()
        .evolution_revision
        .select_named("evolution dashboard revision", |revision| *revision);
    let build_shared = shared.clone();
    RebuildOnChange::new_observable(source, move |_revision| build_dashboard(&build_shared))
}

fn build_dashboard(shared: &Shared) -> WidgetPod {
    let theme = shared.theme();
    let dashboard = shared.evolution().dashboard.clone();
    let refreshing = shared.evolution().refreshing;
    let refresh_shared = shared.clone();
    let header = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(10.0)
        .with_item(
            Flex::vertical()
                .gap(2.0)
                .with_child(
                    weighted_label("Evolution", theme, FontWeight::SEMIBOLD)
                        .semantic_name("Evolution goals"),
                )
                .with_child(Label::new(format!(
                    "{} goals · {} revisions · {} active goals",
                    dashboard.goals.len(),
                    dashboard
                        .goals
                        .iter()
                        .map(|goal| goal.revisions.len())
                        .sum::<usize>(),
                    dashboard.active_goal_count()
                ))),
            FlexItem::new().grow(1.0),
        )
        .with_child(
            Button::new(if refreshing {
                "Refreshing…"
            } else {
                "Refresh"
            })
            .appearance(ButtonAppearance::Tonal)
            .enabled(!refreshing)
            .on_press(move || request_refresh(&refresh_shared)),
        );

    let mut content = Flex::vertical().align_items(Alignment::Stretch).gap(12.0);
    content.push(
        Surface::window(header)
            .padding(Insets::all(14.0))
            .fill()
            .theme(theme),
    );
    if let Some(error) = dashboard.error.filter(|error| !error.trim().is_empty()) {
        content.push(
            Surface::panel(Label::new(error).semantic_name("Evolution error"))
                .appearance(SurfaceAppearance::Raised)
                .padding(Insets::all(12.0))
                .theme(theme),
        );
    }
    if dashboard.goals.is_empty() {
        content.push(
            Surface::panel(
                Flex::vertical()
                    .gap(6.0)
                    .with_child(weighted_label(
                        "No evolution goals yet",
                        theme,
                        FontWeight::SEMIBOLD,
                    ))
                    .with_child(Label::new(
                        "Create one with `sinomo evolve propose <goal>`; revisions and metrics will appear here.",
                    )),
            )
            .appearance(SurfaceAppearance::Raised)
            .padding(Insets::all(16.0))
            .theme(theme),
        );
    } else {
        for goal in dashboard.goals {
            content.push(goal_card(goal, theme, shared));
        }
    }
    WidgetPod::new(
        ScrollView::vertical(
            Surface::window(content)
                .padding(Insets::all(16.0))
                .fill()
                .theme(theme),
        )
        .name("Evolution goal dashboard")
        .theme(theme)
        .viewport_width_hint(true),
    )
}

fn goal_card(
    goal: contract::EvolutionGoalSummary,
    theme: sui::DefaultTheme,
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
                        theme,
                        FontWeight::SEMIBOLD,
                    ))
                    .with_child(Label::new(format!(
                        "{} · governor {} · {} events",
                        goal.id, goal.evolution_agent_id, goal.event_count
                    ))),
                FlexItem::new().grow(1.0),
            )
            .with_child(
                StatusBadge::new(goal.status.clone())
                    .tone(status_tone(&goal.status))
                    .theme(theme),
            ),
    );

    let mut revision_rows = Flex::vertical().align_items(Alignment::Stretch).gap(5.0);
    revision_rows.push(weighted_label("Revision DAG", theme, FontWeight::SEMIBOLD));
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
                        .theme(theme),
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
                    )),
                    FlexItem::new().grow(1.0),
                ),
        );
        for metric in &revision.metrics {
            revision_rows.push(Label::new(format!(
                "    {} = {}{}",
                metric.metric_id,
                metric.value,
                metric
                    .sample_count
                    .map(|count| format!(" (n={count})"))
                    .unwrap_or_default()
            )));
        }
    }
    body.push(
        Surface::panel(revision_rows)
            .padding(Insets::all(10.0))
            .theme(theme),
    );

    if !goal.tasks.is_empty() || !goal.agent_runs.is_empty() {
        let mut workflow = Flex::vertical()
            .align_items(Alignment::Stretch)
            .gap(5.0)
            .with_child(weighted_label(
                "Workflow and agents",
                theme,
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
                            .theme(theme),
                    )
                    .with_item(
                        Label::new(format!(
                            "Task {} · {} · agent {}",
                            task.id,
                            task.title,
                            task.assigned_agent_run_id
                                .as_deref()
                                .unwrap_or("unassigned")
                        )),
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
                            .theme(theme),
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
                        )),
                        FlexItem::new().grow(1.0),
                    ),
            );
        }
        body.push(
            Surface::panel(workflow)
                .padding(Insets::all(10.0))
                .theme(theme),
        );
    }

    let cost = goal
        .resources
        .iter()
        .map(|record| format!("{} {} {}", record.quantity, record.unit, record.resource))
        .collect::<Vec<_>>()
        .join(" · ");
    body.push(Label::new(format!(
        "{} tasks · {} agent runs · resources: {}",
        goal.tasks.len(),
        goal.agent_runs.len(),
        if cost.is_empty() {
            "none recorded"
        } else {
            &cost
        }
    )));
    if let Some(event) = goal.recent_events.last() {
        body.push(Label::new(format!(
            "Latest activity: {} · {} · {}",
            event.kind, event.actor, event.occurred_at
        )));
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
                    .appearance(ButtonAppearance::Ghost)
                    .on_press(move || {
                        chat_shared.chat_mut().start_agent_chat("evolution-agent");
                        chat_shared.set_active_tab(Tab::Sinomo);
                    }),
            )
            .with_child(
                Button::new(pause_label)
                    .appearance(ButtonAppearance::Tonal)
                    .enabled(!terminal)
                    .on_press(move || {
                        control_goal(&pause_shared, pause_goal_id.clone(), pause_action)
                    }),
            )
            .with_child(
                Button::new("Complete")
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
        .theme(theme)
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

fn weighted_label(text: impl Into<String>, theme: sui::DefaultTheme, weight: FontWeight) -> Label {
    let mut style = theme.body_text_style();
    style.weight = weight;
    Label::new(text).style(style)
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
}
