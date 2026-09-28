use std::cell::Cell;
use std::time::Duration;

use canopy_core::services::usage_history::{
    ApiMetrics, SessionMetrics, SkillMetricCollection, TokenMetrics, ToolMetricCollection,
};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs, Wrap};

use super::{ChatTerminal, RawMode};

const STATS_REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const MAX_STATS_ROWS: usize = 100;
const MAX_LABEL_CHARS: usize = 96;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum StatsTab {
    #[default]
    Session,
    Models,
    Tools,
    Skills,
}

impl StatsTab {
    const ALL: [Self; 4] = [Self::Session, Self::Models, Self::Tools, Self::Skills];

    fn label(self) -> &'static str {
        match self {
            Self::Session => "Session",
            Self::Models => "Models",
            Self::Tools => "Tools",
            Self::Skills => "Skills",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Session => 0,
            Self::Models => 1,
            Self::Tools => 2,
            Self::Skills => 3,
        }
    }

    fn from_index(index: usize) -> Self {
        Self::ALL[index % Self::ALL.len()]
    }

    fn next(self, step: isize) -> Self {
        let next = (self.index() as isize + step).rem_euclid(Self::ALL.len() as isize);
        Self::from_index(next as usize)
    }
}

#[derive(Default)]
struct StatsViewState {
    tab: StatsTab,
    scroll: usize,
}

impl ChatTerminal {
    /// Opens a refreshable stats view. The provider is sampled while the view is
    /// open so the counters stay live without retaining a separate metrics copy.
    pub fn show_live_stats<F>(&mut self, mut snapshot: F) -> Result<(), String>
    where
        F: FnMut() -> Option<SessionMetrics>,
    {
        let raw_mode = RawMode::enter()?;
        let result = self.stats_event_loop(&mut snapshot);
        drop(raw_mode);
        let redraw_result = self.draw(None);
        match (result, redraw_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn stats_event_loop<F>(&mut self, snapshot: &mut F) -> Result<(), String>
    where
        F: FnMut() -> Option<SessionMetrics>,
    {
        let mut state = StatsViewState::default();
        loop {
            let metrics = snapshot();
            let session_id = self.session_id.clone();
            let tab = state.tab;
            let scroll = state.scroll;
            let scroll_limit = Cell::new(0);
            self.terminal
                .draw(|frame| {
                    scroll_limit.set(render_stats(
                        frame,
                        &session_id,
                        tab,
                        scroll,
                        metrics.as_ref(),
                    ));
                })
                .map_err(|error| format!("could not render usage stats: {error}"))?;
            state.scroll = state.scroll.min(scroll_limit.get());

            if !event::poll(STATS_REFRESH_INTERVAL)
                .map_err(|error| format!("could not read stats input: {error}"))?
            {
                continue;
            }
            let event =
                event::read().map_err(|error| format!("could not read stats input: {error}"))?;
            let Event::Key(key) = event else {
                continue;
            };
            if key.kind == crossterm::event::KeyEventKind::Release {
                continue;
            }
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                break;
            }

            let page_size = usize::from(self.terminal.size().map(|size| size.height).unwrap_or(12))
                .saturating_sub(7)
                .max(1);
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => break,
                KeyCode::Tab => {
                    state.tab = state
                        .tab
                        .next(if key.modifiers.contains(KeyModifiers::SHIFT) {
                            -1
                        } else {
                            1
                        });
                    state.scroll = 0;
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    state.tab = state.tab.next(-1);
                    state.scroll = 0;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    state.tab = state.tab.next(1);
                    state.scroll = 0;
                }
                KeyCode::Char('1') => select_tab(&mut state, StatsTab::Session),
                KeyCode::Char('2') => select_tab(&mut state, StatsTab::Models),
                KeyCode::Char('3') => select_tab(&mut state, StatsTab::Tools),
                KeyCode::Char('4') => select_tab(&mut state, StatsTab::Skills),
                KeyCode::Up | KeyCode::Char('k') => {
                    state.scroll = state.scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    state.scroll = state.scroll.saturating_add(1);
                }
                KeyCode::PageUp => {
                    state.scroll = state.scroll.saturating_sub(page_size);
                }
                KeyCode::PageDown => {
                    state.scroll = state.scroll.saturating_add(page_size);
                }
                KeyCode::Home => state.scroll = 0,
                KeyCode::End => state.scroll = usize::MAX,
                _ => {}
            }
        }
        Ok(())
    }
}

fn select_tab(state: &mut StatsViewState, tab: StatsTab) {
    state.tab = tab;
    state.scroll = 0;
}

fn render_stats(
    frame: &mut Frame<'_>,
    session_id: &str,
    tab: StatsTab,
    scroll: usize,
    metrics: Option<&SessionMetrics>,
) -> usize {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let header = Block::default()
        .title(format!(" Usage · Session {} ", safe_label(session_id, 60)))
        .borders(Borders::ALL);
    let header_inner = header.inner(chunks[0]);
    frame.render_widget(header, chunks[0]);
    let tabs = Tabs::new(
        StatsTab::ALL
            .iter()
            .map(|tab| Line::from(format!(" {} ", tab.label())))
            .collect::<Vec<_>>(),
    )
    .select(tab.index())
    .highlight_style(
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
    .style(Style::default().fg(Color::Gray))
    .divider(Span::styled("  ", Style::default().fg(Color::DarkGray)));
    frame.render_widget(tabs, header_inner);

    let rows = stats_lines(tab, metrics, session_id);
    let content_block = Block::default()
        .title(format!(" {} ", tab.label()))
        .borders(Borders::ALL);
    let content_area = content_block.inner(chunks[1]);
    let max_scroll = estimate_max_scroll(&rows, content_area);
    let paragraph = Paragraph::new(rows)
        .block(content_block)
        .wrap(Wrap { trim: false })
        .scroll((scroll.min(max_scroll).min(u16::MAX as usize) as u16, 0));
    frame.render_widget(paragraph, chunks[1]);

    let footer = Paragraph::new(
        " Tab/←→ switch · 1–4 select · ↑↓ scroll · PgUp/PgDn · Home/End · Esc/q close ",
    )
    .style(Style::default().fg(Color::Gray));
    frame.render_widget(footer, chunks[2]);
    max_scroll
}

fn stats_lines(
    tab: StatsTab,
    metrics: Option<&SessionMetrics>,
    session_id: &str,
) -> Vec<Line<'static>> {
    let Some(metrics) = metrics else {
        return vec![Line::styled(
            "Session metrics are unavailable for this session.",
            Style::default().fg(Color::Yellow),
        )];
    };
    match tab {
        StatsTab::Session => session_lines(metrics, session_id),
        StatsTab::Models => model_lines(metrics),
        StatsTab::Tools => tool_lines(metrics),
        StatsTab::Skills => skill_lines(metrics.skills.as_ref()),
    }
}

fn session_lines(metrics: &SessionMetrics, session_id: &str) -> Vec<Line<'static>> {
    let mut requests = 0.0;
    let mut errors = 0.0;
    let mut latency_ms = 0.0;
    let mut input_tokens = 0.0;
    let mut output_tokens = 0.0;
    let mut cached_tokens = 0.0;
    let mut thoughts_tokens = 0.0;
    let mut total_tokens = 0.0;
    let mut active_models = 0usize;
    for model in metrics.models.values() {
        if model.api.total_requests > 0.0 {
            active_models = active_models.saturating_add(1);
        }
        requests += finite_nonnegative(model.api.total_requests);
        errors += finite_nonnegative(model.api.total_errors);
        latency_ms += finite_nonnegative(model.api.total_latency_ms);
        input_tokens += finite_nonnegative(model.tokens.prompt_tokens);
        output_tokens += finite_nonnegative(model.tokens.candidates);
        cached_tokens += finite_nonnegative(model.tokens.cached_tokens);
        thoughts_tokens += finite_nonnegative(model.tokens.thoughts_tokens);
        total_tokens += finite_nonnegative(model.tokens.total_tokens);
    }
    let api_error_rate = percentage(errors, requests);
    let api_latency = if requests > 0.0 {
        format_duration(latency_ms / requests)
    } else {
        "—".to_owned()
    };
    let tool_success_rate = percentage(metrics.tools.total_success, metrics.tools.total_calls);
    let cache_rate = percentage(cached_tokens, input_tokens);

    let mut lines = vec![
        section_line("Interaction Summary"),
        key_value("Session ID", safe_label(session_id, MAX_LABEL_CHARS)),
        key_value(
            "API requests",
            format!("{} across {active_models} models", format_metric(requests)),
        ),
        key_value(
            "API errors",
            format!("{} ({api_error_rate:.1}%)", format_metric(errors)),
        ),
        key_value("Average API latency", api_latency),
        key_value(
            "Tool calls",
            format!(
                "{} · {} ok · {} failed · {tool_success_rate:.1}% success",
                format_metric(metrics.tools.total_calls),
                format_metric(metrics.tools.total_success),
                format_metric(metrics.tools.total_fail),
            ),
        ),
        key_value(
            "Code changes",
            format!(
                "+{} / −{} lines",
                format_metric(metrics.files.total_lines_added),
                format_metric(metrics.files.total_lines_removed),
            ),
        ),
        Line::raw(""),
        section_line("Tokens"),
        key_value("Input", format_metric(input_tokens)),
        key_value("Output", format_metric(output_tokens)),
        key_value(
            "Cached",
            format!(
                "{} ({cache_rate:.1}% of input)",
                format_metric(cached_tokens)
            ),
        ),
        key_value("Thoughts", format_metric(thoughts_tokens)),
        key_value("Total", format_metric(total_tokens)),
        Line::raw(""),
        section_line("Tracking"),
    ];
    match metrics.skills.as_ref() {
        Some(skills) => lines.push(key_value(
            "Skills",
            format!("{} calls tracked", format_metric(skills.total_calls)),
        )),
        None => lines.push(key_value("Skills", "Unavailable in the native runtime")),
    }
    lines
}

fn model_lines(metrics: &SessionMetrics) -> Vec<Line<'static>> {
    let total_requests = metrics
        .models
        .values()
        .map(|model| finite_nonnegative(model.api.total_requests))
        .sum::<f64>();
    let total_errors = metrics
        .models
        .values()
        .map(|model| finite_nonnegative(model.api.total_errors))
        .sum::<f64>();
    let mut lines = vec![
        key_value(
            "Totals",
            format!(
                "{} requests · {} errors",
                format_metric(total_requests),
                format_metric(total_errors),
            ),
        ),
        Line::raw(""),
    ];
    let has_non_main_source = metrics
        .models
        .values()
        .flat_map(|model| model.by_source.keys())
        .any(|source| source != "main");
    let mut shown = 0usize;
    'models: for (model_name, model) in &metrics.models {
        let display_name = model_name.strip_suffix("-001").unwrap_or(model_name);
        if has_non_main_source && !model.by_source.is_empty() {
            for (source, source_metrics) in &model.by_source {
                if source_metrics.api.total_requests <= 0.0 {
                    continue;
                }
                if shown >= MAX_STATS_ROWS {
                    break 'models;
                }
                append_model_row(
                    &mut lines,
                    format!("{display_name} ({source})"),
                    &source_metrics.api,
                    &source_metrics.tokens,
                );
                shown += 1;
            }
        } else if model.api.total_requests > 0.0 {
            if shown >= MAX_STATS_ROWS {
                break;
            }
            let (api, tokens) = if model.by_source.is_empty() {
                (&model.api, &model.tokens)
            } else if let Some(main) = model.by_source.get("main") {
                (&main.api, &main.tokens)
            } else {
                (&model.api, &model.tokens)
            };
            append_model_row(&mut lines, display_name.to_owned(), api, tokens);
            shown += 1;
        }
    }
    if shown == 0 {
        lines.push(Line::raw(
            "No API calls have been recorded in this session.",
        ));
    } else if shown >= MAX_STATS_ROWS {
        lines.push(Line::styled(
            format!("List capped at {MAX_STATS_ROWS} model rows."),
            Style::default().fg(Color::DarkGray),
        ));
    }
    lines
}

fn append_model_row(
    lines: &mut Vec<Line<'static>>,
    name: String,
    api: &ApiMetrics,
    tokens: &TokenMetrics,
) {
    let calls = finite_nonnegative(api.total_requests);
    let errors = finite_nonnegative(api.total_errors);
    let avg_latency = if calls > 0.0 {
        format_duration(api.total_latency_ms / calls)
    } else {
        "—".to_owned()
    };
    lines.push(Line::styled(
        safe_label(&name, MAX_LABEL_CHARS),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    lines.push(Line::raw(format!(
        "  Requests {} · errors {} ({:.1}%) · average latency {}",
        format_metric(calls),
        format_metric(errors),
        percentage(errors, calls),
        avg_latency,
    )));
    lines.push(Line::raw(format!(
        "  Tokens {} total · input {} · cached {} · thoughts {} · output {}",
        format_metric(tokens.total_tokens),
        format_metric(tokens.prompt_tokens),
        format_metric(tokens.cached_tokens),
        format_metric(tokens.thoughts_tokens),
        format_metric(tokens.candidates),
    )));
    lines.push(Line::raw(""));
}

fn tool_lines(metrics: &SessionMetrics) -> Vec<Line<'static>> {
    collection_tool_lines(&metrics.tools)
}

fn collection_tool_lines(tools: &ToolMetricCollection) -> Vec<Line<'static>> {
    let total = finite_nonnegative(tools.total_calls);
    let success = finite_nonnegative(tools.total_success);
    let failure = finite_nonnegative(tools.total_fail);
    let mut lines = vec![
        key_value(
            "Tool calls",
            format!(
                "{} · {} ok · {} failed · {:.1}% success",
                format_metric(total),
                format_metric(success),
                format_metric(failure),
                percentage(success, total),
            ),
        ),
        key_value("Total duration", format_duration(tools.total_duration_ms)),
        Line::raw(""),
    ];
    let mut shown = 0usize;
    for (name, stats) in &tools.by_name {
        if stats.count <= 0.0 {
            continue;
        }
        if shown >= MAX_STATS_ROWS {
            break;
        }
        lines.push(Line::styled(
            safe_label(name, MAX_LABEL_CHARS),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        lines.push(Line::raw(format!(
            "  Calls {} · {} ok · {} failed · {:.1}% success",
            format_metric(stats.count),
            format_metric(stats.success),
            format_metric(stats.fail),
            percentage(stats.success, stats.count),
        )));
        let avg_duration = if stats.count > 0.0 {
            format_duration(stats.duration_ms / stats.count)
        } else {
            "—".to_owned()
        };
        lines.push(Line::raw(format!(
            "  Average duration {} · total {}",
            avg_duration,
            format_duration(stats.duration_ms),
        )));
        lines.push(Line::raw(""));
        shown += 1;
    }
    if shown == 0 {
        lines.push(Line::raw(
            "No tool calls have been recorded in this session.",
        ));
    } else if shown >= MAX_STATS_ROWS {
        lines.push(Line::styled(
            format!("List capped at {MAX_STATS_ROWS} tools."),
            Style::default().fg(Color::DarkGray),
        ));
    }
    lines
}

fn skill_lines(skills: Option<&SkillMetricCollection>) -> Vec<Line<'static>> {
    let Some(skills) = skills else {
        return vec![Line::styled(
            "Skill statistics are unavailable in the native runtime.",
            Style::default().fg(Color::Yellow),
        )];
    };
    let total = finite_nonnegative(skills.total_calls);
    let success = finite_nonnegative(skills.total_success);
    let failure = finite_nonnegative(skills.total_fail);
    let mut lines = vec![
        key_value(
            "Skill calls",
            format!(
                "{} · {} ok · {} failed · {:.1}% success",
                format_metric(total),
                format_metric(success),
                format_metric(failure),
                percentage(success, total),
            ),
        ),
        Line::raw(""),
    ];
    let mut shown = 0usize;
    for (name, stats) in &skills.by_name {
        if stats.count <= 0.0 {
            continue;
        }
        if shown >= MAX_STATS_ROWS {
            break;
        }
        lines.push(Line::styled(
            safe_label(name, MAX_LABEL_CHARS),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        lines.push(Line::raw(format!(
            "  Calls {} · {} ok · {} failed · {:.1}% success",
            format_metric(stats.count),
            format_metric(stats.success),
            format_metric(stats.fail),
            percentage(stats.success, stats.count),
        )));
        lines.push(Line::raw(""));
        shown += 1;
    }
    if shown == 0 {
        lines.push(Line::raw(
            "No skill calls have been recorded in this session.",
        ));
    } else if shown >= MAX_STATS_ROWS {
        lines.push(Line::styled(
            format!("List capped at {MAX_STATS_ROWS} skills."),
            Style::default().fg(Color::DarkGray),
        ));
    }
    lines
}

fn section_line(title: &str) -> Line<'static> {
    Line::styled(
        title.to_owned(),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn key_value(label: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label}: "), Style::default().fg(Color::Gray)),
        Span::raw(value.into()),
    ])
}

fn safe_label(value: &str, limit: usize) -> String {
    let sanitized = value
        .chars()
        .filter(|character| !character.is_control())
        .take(limit.saturating_add(1))
        .collect::<String>();
    if sanitized.chars().count() > limit {
        format!("{}…", sanitized.chars().take(limit).collect::<String>())
    } else {
        sanitized
    }
}

fn finite_nonnegative(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        0.0
    }
}

fn percentage(part: f64, total: f64) -> f64 {
    let total = finite_nonnegative(total);
    let part = finite_nonnegative(part);
    if total > 0.0 {
        (part / total * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    }
}

fn format_metric(value: f64) -> String {
    let value = finite_nonnegative(value);
    let rounded = value.round();
    if (value - rounded).abs() < 0.000_001 {
        let digits = format!("{rounded:.0}");
        return group_digits(&digits);
    }
    format!("{value:.1}")
}

fn group_digits(digits: &str) -> String {
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            output.push(',');
        }
        output.push(character);
    }
    output
}

fn format_duration(milliseconds: f64) -> String {
    let milliseconds = finite_nonnegative(milliseconds);
    if milliseconds < 1_000.0 {
        return format!("{}ms", milliseconds.round());
    }
    let seconds = milliseconds / 1_000.0;
    if seconds < 60.0 {
        return format!("{:.1}s", seconds);
    }
    let total_seconds = seconds.floor() as u64;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let remaining_seconds = total_seconds % 60;
    let mut parts = Vec::new();
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if remaining_seconds > 0 {
        parts.push(format!("{remaining_seconds}s"));
    }
    if parts.is_empty() {
        parts.push("0s".to_owned());
    }
    parts.join(" ")
}

fn estimate_max_scroll(lines: &[Line<'_>], area: Rect) -> usize {
    if area.width == 0 || area.height == 0 {
        return 0;
    }
    let width = usize::from(area.width);
    let visual_lines = lines
        .iter()
        .map(|line| {
            let characters = line
                .spans
                .iter()
                .map(|span| span.content.chars().count())
                .sum::<usize>();
            characters.max(1).div_ceil(width)
        })
        .sum::<usize>();
    visual_lines.saturating_sub(usize::from(area.height))
}
