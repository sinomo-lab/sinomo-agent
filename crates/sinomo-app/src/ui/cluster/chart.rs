//! Time-based plots with a fixed trailing window, explicit gaps, and pointer readouts.
use super::*;
use std::cell::Cell;

pub(super) struct MetricChart {
    name: String,
    pub series: Vec<MetricSeries>,
    theme: DefaultTheme,
    pub colors: [Color; 6],
    unit: Option<ObserveMetricUnit>,
    transform: ExplorerTransform,
    kind: ObserveMetricKind,
    pub window: ExplorerWindow,
    pub end_at: OffsetDateTime,
    pub paused: bool,
    pub dashboard_metric: Option<DashboardMetric>,
    pub height: f32,
    hovered: Rc<Cell<Option<f32>>>,
}

impl MetricChart {
    pub fn new(
        name: String,
        series: Vec<MetricSeries>,
        theme: DefaultTheme,
        colors: [Color; 6],
        unit: Option<ObserveMetricUnit>,
        transform: ExplorerTransform,
        kind: ObserveMetricKind,
    ) -> Self {
        let window = dashboard::metric_window(&name, kind);
        Self {
            name,
            series,
            theme,
            colors,
            unit,
            transform,
            kind,
            window,
            end_at: OffsetDateTime::now_utc(),
            paused: false,
            dashboard_metric: None,
            height: EXPLORER_CHART_H,
            hovered: Rc::new(Cell::new(None)),
        }
    }

    pub fn hover_state(mut self, state: Rc<Cell<Option<f32>>>) -> Self {
        self.hovered = state;
        self
    }

    pub fn time_range(mut self, end: OffsetDateTime, window: ExplorerWindow, paused: bool) -> Self {
        self.end_at = end;
        self.window = window;
        self.paused = paused;
        self
    }

    fn chart_rect(&self, bounds: Rect) -> Rect {
        let scale = self.theme.text.xs.size / DefaultTheme::default().text.xs.size;
        let left = match self.dashboard_metric.map(DashboardMetric::unit) {
            Some("B/s") => 100.0,
            Some("ms" | "%" | "/s") => 72.0,
            Some(_) => 54.0,
            None => 100.0,
        } * scale;
        Rect::new(
            bounds.x() + left,
            bounds.y() + 8.0,
            (bounds.width() - left - 8.0).max(0.0),
            (bounds.height() - 34.0).max(0.0),
        )
    }

    pub fn x_fraction(&self, at: OffsetDateTime) -> f32 {
        ((at - (self.end_at - self.window.duration())).as_seconds_f64()
            / self.window.duration().as_seconds_f64()) as f32
    }

    fn value_label(&self, value: Option<f64>) -> String {
        self.dashboard_metric
            .map(|metric| format_dashboard_value(metric, value))
            .unwrap_or_else(|| format_metric_value(value, self.unit, self.transform, self.kind))
    }

    fn finite_values(&self) -> impl Iterator<Item = f64> + '_ {
        self.series
            .iter()
            .flat_map(|series| &series.points)
            .filter(|point| {
                point.at >= self.end_at - self.window.duration() && point.at <= self.end_at
            })
            .filter_map(|point| point.value.filter(|value| value.is_finite()))
    }

    fn description(&self) -> String {
        let values = self.finite_values().collect::<Vec<_>>();
        format!(
            "{}; fixed {} rolling window; {} series; {} samples; minimum {}; maximum {}; missing samples are gaps{}",
            self.name,
            self.window.label(),
            self.series.len(),
            values.len(),
            self.value_label(values.iter().copied().reduce(f64::min)),
            self.value_label(values.iter().copied().reduce(f64::max)),
            if self.paused { "; paused" } else { "" }
        )
    }

    fn axis_label(&self, value: f64) -> String {
        if self
            .dashboard_metric
            .is_some_and(|metric| matches!(metric.unit(), "" | "entries"))
        {
            if value.abs() >= 1_000_000.0 {
                return format!("{:.1}M", value / 1_000_000.0);
            }
            if value.abs() >= 1_000.0 {
                return format!("{:.1}k", value / 1_000.0);
            }
            return if value.fract().abs() < 0.001 {
                format!("{value:.0}")
            } else {
                format!("{value:.2}")
            };
        }
        self.value_label(Some(value))
    }
}

impl Widget for MetricChart {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                640.0
            },
            self.height,
        ))
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Pointer(pointer) = event {
            match pointer.kind {
                PointerEventKind::Move => {
                    let plot = self.chart_rect(ctx.bounds());
                    self.hovered.set(
                        (plot.contains(pointer.position) && plot.width() > 0.0)
                            .then(|| (pointer.position.x - plot.x()) / plot.width()),
                    );
                    ctx.request_paint();
                }
                PointerEventKind::Leave => {
                    self.hovered.set(None);
                    ctx.request_paint();
                }
                _ => {}
            }
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let bounds = ctx.bounds();
        let plot = self.chart_rect(bounds);
        if plot.is_empty() {
            return;
        }
        let values = self.finite_values().collect::<Vec<_>>();
        let percentages = self
            .dashboard_metric
            .is_some_and(|metric| metric.unit() == "%");
        let (min, max) = if percentages {
            (0.0, 100.0)
        } else if self.dashboard_metric.is_some() {
            let high = values.iter().copied().reduce(f64::max).unwrap_or(0.0);
            let high = if high <= 0.0 { 1.0 } else { high * 1.05 };
            let magnitude = 10.0_f64.powf(high.log10().floor());
            let scaled = high / magnitude;
            let ceiling = if scaled <= 1.0 {
                1.0
            } else if scaled <= 2.0 {
                2.0
            } else if scaled <= 5.0 {
                5.0
            } else {
                10.0
            };
            (0.0, ceiling * magnitude)
        } else {
            let low = values.iter().copied().reduce(f64::min).unwrap_or(0.0);
            let high = values.iter().copied().reduce(f64::max).unwrap_or(1.0);
            let padding = (high - low).max(high.abs().max(1.0) * 0.1) * 0.1;
            (
                if low >= 0.0 {
                    (low - padding).max(0.0)
                } else {
                    low - padding
                },
                high + padding,
            )
        };
        let span = (max - min).max(f64::EPSILON);
        let pal = Palette::from_theme(&self.theme, self.theme.surfaces.dark);
        let axis = pal.text_style(
            &self.theme,
            self.theme.text.xs.size,
            FontWeight::NORMAL,
            pal.text_muted,
        );
        for tick in 0..=4 {
            let fraction = tick as f32 / 4.0;
            let y = plot.y() + plot.height() * fraction;
            let mut line = PathBuilder::new();
            line.move_to(Point::new(plot.x(), y));
            line.line_to(Point::new(plot.max_x(), y));
            ctx.stroke(
                line.build(),
                pal.border.with_alpha(0.45),
                StrokeStyle::new(1.0),
            );
            let label_rect = Rect::new(
                bounds.x(),
                y - axis.line_height * 0.5,
                plot.x() - bounds.x() - 6.0,
                axis.line_height,
            );
            widgets::draw_text_in(
                ctx,
                label_rect,
                &self.axis_label(max - span * f64::from(fraction)),
                axis.clone(),
            );
        }
        for tick in 0..=2 {
            let x = plot.x() + plot.width() * tick as f32 / 2.0;
            let seconds = self.window.duration().whole_seconds() * (2 - tick) / 2;
            let label = if tick == 2 {
                if self.paused {
                    "Paused".into()
                } else {
                    "Now".into()
                }
            } else {
                format!("−{}", format_metric_span(time::Duration::seconds(seconds)))
            };
            let width = (label.chars().count() as f32 * axis.font_size * 0.65 + 4.0)
                .min(plot.width() / 2.0);
            let left = if tick == 2 {
                plot.max_x() - width
            } else if tick == 1 {
                x - width * 0.5
            } else {
                x
            };
            widgets::draw_text_in(
                ctx,
                Rect::new(left, plot.max_y() + 6.0, width, axis.line_height),
                &label,
                axis.clone(),
            );
        }
        ctx.push_clip_rect(plot);
        for (index, series) in self.series.iter().enumerate() {
            let color = self.colors[index % self.colors.len()];
            let mut path = PathBuilder::new();
            let mut drawing = false;
            let mut previous_at: Option<OffsetDateTime> = None;
            let mut last = None;
            for point in &series.points {
                let fraction = self.x_fraction(point.at);
                if !(0.0..=1.0).contains(&fraction) {
                    continue;
                }
                let Some(value) = point.value.filter(|value| value.is_finite()) else {
                    drawing = false;
                    previous_at = None;
                    last = None;
                    continue;
                };
                if previous_at.is_some_and(|at| (point.at - at).whole_seconds() > 20) {
                    drawing = false;
                }
                let p = Point::new(
                    plot.x() + plot.width() * fraction,
                    plot.max_y() - plot.height() * ((value - min) / span).clamp(0.0, 1.0) as f32,
                );
                if drawing {
                    path.line_to(p);
                } else {
                    path.move_to(p);
                    drawing = true;
                }
                previous_at = Some(point.at);
                last = Some(p);
            }
            ctx.stroke(path.build(), color, StrokeStyle::new(2.0));
            if let Some(point) = last {
                ctx.fill(Path::circle(point, 2.3), color);
            }
        }
        if values.is_empty() {
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    plot.x() + 8.0,
                    plot.y() + plot.height() * 0.4,
                    (plot.width() - 16.0).max(0.0),
                    axis.line_height * 2.0,
                ),
                "Waiting for samples",
                axis.clone(),
            );
        }
        ctx.pop_clip();
        if let Some(fraction) = self.hovered.get() {
            let x = plot.x() + fraction * plot.width();
            let at = self.end_at - self.window.duration()
                + time::Duration::seconds_f64(
                    self.window.duration().as_seconds_f64() * f64::from(fraction),
                );
            let mut guide = PathBuilder::new();
            guide.move_to(Point::new(x, plot.y()));
            guide.line_to(Point::new(x, plot.max_y()));
            ctx.stroke(
                guide.build(),
                pal.text_muted.with_alpha(0.65),
                StrokeStyle::new(1.0),
            );
            let lines = self
                .series
                .iter()
                .take(6)
                .enumerate()
                .map(|(index, series)| {
                    let point = series
                        .points
                        .iter()
                        .filter(|point| (point.at - at).whole_seconds().abs() <= 15)
                        .min_by_key(|point| {
                            (
                                (point.at - at).whole_nanoseconds().abs(),
                                point.value.is_none(),
                            )
                        });
                    format!(
                        "{}{}",
                        if self.series.len() > 1 {
                            format!("S{} · ", index + 1)
                        } else {
                            String::new()
                        },
                        self.value_label(point.and_then(|point| point.value))
                    )
                })
                .collect::<Vec<_>>();
            let caption = format!(
                "−{} · {}",
                format_metric_span(self.end_at - at),
                lines.join("  ")
            );
            let width = (bounds.width() - 8.0).min(280.0);
            let rect = Rect::new(
                (x + 8.0).min(bounds.max_x() - width).max(bounds.x()),
                plot.y() + 6.0,
                width,
                axis.line_height * 2.0 + 8.0,
            );
            ctx.fill_rect(rect, pal.overlay);
            ctx.stroke_rect(rect, pal.border, StrokeStyle::new(1.0));
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    rect.x() + 6.0,
                    rect.y() + 4.0,
                    rect.width() - 12.0,
                    rect.height() - 8.0,
                ),
                &caption,
                axis,
            );
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Image, ctx.bounds());
        node.name = Some(format!("{} live history", self.name));
        node.description = Some(self.description());
        ctx.push(node);
    }
}
