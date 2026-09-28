//! Read-only interactive browser for configured hooks.
//!
//! The screen mirrors `packages/cli/src/ui/components/hooks` and consumes
//! snapshots from the native hook registry and session manager. It never
//! changes hook configuration or execution state.

use std::time::Duration;

use canopy_core::hooks::function_runner::FunctionHookConfig;
use canopy_core::hooks::planner::{HookEventName, hook_event_supports_matcher};
use canopy_core::hooks::registry::{HookRegistry, HookRegistryEntry, HooksConfigSource};
use canopy_core::hooks::session_manager::{
    SessionHookConfig, SessionHookEntry, SessionHooksManager,
};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use serde_json::{Value, json};

use super::{ChatTerminal, RawMode};

const REFRESH_INTERVAL: Duration = Duration::from_millis(250);

/// Immutable data shown by `/hooks`.
#[derive(Clone, Debug)]
pub(crate) struct HooksDialogModel {
    pub(crate) disable_all_hooks: bool,
    pub(crate) events: Vec<HookEventDisplay>,
}

#[derive(Clone, Debug)]
pub(crate) struct HookEventDisplay {
    event: HookEventName,
    short_description: &'static str,
    description: &'static str,
    exit_codes: &'static [ExitCodeDisplay],
    matcher_groups: Vec<HookMatcherDisplay>,
}

#[derive(Clone, Copy, Debug)]
struct ExitCodeDisplay {
    code: &'static str,
    description: &'static str,
}

#[derive(Clone, Debug)]
struct HookMatcherDisplay {
    matcher: String,
    sequential: bool,
    configs: Vec<HookConfigDisplay>,
}

#[derive(Clone, Debug)]
struct HookConfigDisplay {
    config: Value,
    source: HooksConfigSource,
    source_display: String,
    source_path: Option<String>,
    matcher: String,
    sequential: bool,
    enabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HooksView {
    List {
        selected: usize,
    },
    Event {
        event: usize,
        selected: usize,
    },
    Matcher {
        event: usize,
        matcher: usize,
        selected: usize,
    },
    Config {
        event: usize,
        matcher: usize,
        config: usize,
    },
}

impl HooksDialogModel {
    /// Build a stable UI snapshot from the same registries used for hook
    /// dispatch. Event order follows the TypeScript hook event enum.
    pub(crate) fn from_snapshots(
        registry: &HookRegistry,
        session_manager: &SessionHooksManager,
        session_id: &str,
        disable_all_hooks: bool,
    ) -> Self {
        let mut model = Self {
            disable_all_hooks,
            events: HOOK_EVENTS
                .iter()
                .copied()
                .map(HookEventDisplay::new)
                .collect(),
        };

        for entry in registry.get_all_hooks() {
            model.add_registry_entry(entry);
        }
        for entry in session_manager.get_all_session_hooks(session_id) {
            model.add_session_entry(entry);
        }
        model
    }

    pub(crate) fn configured_hook_count(&self) -> usize {
        self.events
            .iter()
            .flat_map(|event| &event.matcher_groups)
            .map(|group| group.configs.len())
            .sum()
    }

    /// Render the concise `/hooks` listing used when the terminal UI is not
    /// available. This mirrors the non-interactive TypeScript command.
    pub(crate) fn plain_text_summary(&self) -> String {
        if self.disable_all_hooks {
            return "Hooks are not enabled. Enable hooks in settings to use this feature."
                .to_owned();
        }

        let count = self.configured_hook_count();
        if count == 0 {
            return "No hooks configured. Add hooks in your settings.json file or invoke a skill with hooks."
                .to_owned();
        }

        let mut output = format!("**Configured Hooks ({count} total)**\n\n");
        for event in &self.events {
            if event.config_count() == 0 {
                continue;
            }
            output.push_str("### ");
            output.push_str(event_name(event.event));
            output.push_str("\n\n");
            if hook_event_supports_matcher(event.event) {
                for group in &event.matcher_groups {
                    output.push_str("#### Matcher: ");
                    output.push_str(&group.matcher);
                    output.push('\n');
                    for config in &group.configs {
                        output.push_str("- **");
                        output.push_str(&plain_hook_name(config));
                        output.push_str("** [");
                        output.push_str(plain_source_label(config.source));
                        output.push_str("]\n");
                    }
                    output.push('\n');
                }
            } else {
                for config in event.flat_configs() {
                    output.push_str("- **");
                    output.push_str(&plain_hook_name(config));
                    output.push_str("** [");
                    output.push_str(plain_source_label(config.source));
                    output.push_str("]\n");
                }
                output.push('\n');
            }
        }
        output
    }

    fn add_registry_entry(&mut self, entry: HookRegistryEntry) {
        self.add_config(
            entry.event_name,
            entry.config,
            entry.source,
            entry.matcher.as_deref(),
            entry.sequential,
            entry.enabled,
        );
    }

    fn add_session_entry(&mut self, entry: SessionHookEntry) {
        let config = match entry.config {
            SessionHookConfig::Json(config) => config,
            SessionHookConfig::Function(config) => function_config_for_display(&config),
        };
        self.add_config(
            entry.event_name,
            config,
            HooksConfigSource::Session,
            Some(&entry.matcher),
            entry.sequential,
            true,
        );
    }

    fn add_config(
        &mut self,
        event: HookEventName,
        config: Value,
        source: HooksConfigSource,
        matcher: Option<&str>,
        sequential: Option<bool>,
        enabled: bool,
    ) {
        let Some(event_info) = self.events.iter_mut().find(|info| info.event == event) else {
            return;
        };
        let matcher = if hook_event_supports_matcher(event) {
            safe_single_line(
                matcher
                    .map(str::trim)
                    .filter(|matcher| !matcher.is_empty())
                    .unwrap_or("*"),
            )
        } else {
            "*".to_owned()
        };
        let sequential = sequential.unwrap_or(false);
        let group = if let Some(index) = event_info
            .matcher_groups
            .iter()
            .position(|group| group.matcher == matcher)
        {
            &mut event_info.matcher_groups[index]
        } else {
            event_info.matcher_groups.push(HookMatcherDisplay {
                matcher: matcher.clone(),
                sequential,
                configs: Vec::new(),
            });
            event_info
                .matcher_groups
                .last_mut()
                .expect("matcher group was just inserted")
        };
        group.sequential |= sequential;
        let source_display = extension_display(&config, source);
        let source_path =
            string_field(&config, "sourcePath").or_else(|| string_field(&config, "extensionPath"));
        group.configs.push(HookConfigDisplay {
            config,
            source,
            source_display,
            source_path,
            matcher,
            sequential,
            enabled,
        });
    }
}

impl HookEventDisplay {
    fn new(event: HookEventName) -> Self {
        let (short_description, description, exit_codes) = event_metadata(event);
        Self {
            event,
            short_description,
            description,
            exit_codes,
            matcher_groups: Vec::new(),
        }
    }

    fn config_count(&self) -> usize {
        self.matcher_groups
            .iter()
            .map(|group| group.configs.len())
            .sum()
    }

    fn flat_configs(&self) -> Vec<&HookConfigDisplay> {
        self.matcher_groups
            .iter()
            .flat_map(|group| group.configs.iter())
            .collect()
    }
}

impl ChatTerminal {
    /// Display configured hooks without changing configuration or hook state.
    pub(crate) fn show_hooks_dialog(&mut self, model: &HooksDialogModel) -> Result<(), String> {
        let raw_mode = RawMode::enter()?;
        let result = self.hooks_event_loop(model);
        drop(raw_mode);
        let redraw_result = self.draw(None);
        match (result, redraw_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn hooks_event_loop(&mut self, model: &HooksDialogModel) -> Result<(), String> {
        let mut stack = vec![HooksView::List { selected: 0 }];
        let mut scroll = 0usize;
        loop {
            let view = *stack.last().unwrap_or(&HooksView::List { selected: 0 });
            let mut max_scroll = 0;
            self.terminal
                .draw(|frame| max_scroll = render_hooks(frame, model, view, scroll))
                .map_err(|error| format!("could not render hooks view: {error}"))?;
            scroll = scroll.min(max_scroll);

            if !event::poll(REFRESH_INTERVAL)
                .map_err(|error| format!("could not read hooks view input: {error}"))?
            {
                continue;
            }
            let event = event::read()
                .map_err(|error| format!("could not read hooks view input: {error}"))?;
            let Event::Key(key) = event else {
                continue;
            };
            if key.kind == crossterm::event::KeyEventKind::Release {
                continue;
            }
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                break;
            }

            if model.disable_all_hooks {
                if matches!(key.code, KeyCode::Esc | KeyCode::Char('q')) {
                    break;
                }
                continue;
            }

            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    if stack.len() > 1 {
                        stack.pop();
                        scroll = 0;
                    } else {
                        break;
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    update_selection(model, stack.last_mut(), -1);
                    scroll = stack
                        .last()
                        .copied()
                        .map(|view| selection_scroll(self, model, view))
                        .unwrap_or_default();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    update_selection(model, stack.last_mut(), 1);
                    scroll = stack
                        .last()
                        .copied()
                        .map(|view| selection_scroll(self, model, view))
                        .unwrap_or_default();
                }
                KeyCode::Enter => {
                    if let Some(next) = next_view(model, view) {
                        stack.push(next);
                        scroll = 0;
                    }
                }
                KeyCode::PageUp => scroll = scroll.saturating_sub(8),
                KeyCode::PageDown => scroll = scroll.saturating_add(8),
                KeyCode::Home => scroll = 0,
                KeyCode::End => scroll = usize::MAX,
                _ => {}
            }
        }
        Ok(())
    }
}

fn update_selection(model: &HooksDialogModel, view: Option<&mut HooksView>, delta: isize) {
    let Some(view) = view else { return };
    let (selected, count) = match view {
        HooksView::List { selected } => (selected, model.events.len()),
        HooksView::Event { event, selected } => {
            let Some(event) = model.events.get(*event) else {
                return;
            };
            let count = if hook_event_supports_matcher(event.event) {
                event.matcher_groups.len()
            } else {
                event.flat_configs().len()
            };
            (selected, count)
        }
        HooksView::Matcher {
            event,
            matcher,
            selected,
        } => {
            let Some(group) = model
                .events
                .get(*event)
                .and_then(|event| event.matcher_groups.get(*matcher))
            else {
                return;
            };
            (selected, group.configs.len())
        }
        HooksView::Config { .. } => return,
    };
    if count == 0 {
        *selected = 0;
    } else {
        *selected =
            (*selected as isize + delta).clamp(0, count.saturating_sub(1) as isize) as usize;
    }
}

fn next_view(model: &HooksDialogModel, view: HooksView) -> Option<HooksView> {
    match view {
        HooksView::List { selected } => model.events.get(selected).map(|_| HooksView::Event {
            event: selected,
            selected: 0,
        }),
        HooksView::Event { event, selected } => {
            let info = model.events.get(event)?;
            if hook_event_supports_matcher(info.event) {
                info.matcher_groups
                    .get(selected)
                    .map(|_| HooksView::Matcher {
                        event,
                        matcher: selected,
                        selected: 0,
                    })
            } else {
                (selected < info.flat_configs().len()).then_some(HooksView::Config {
                    event,
                    matcher: usize::MAX,
                    config: selected,
                })
            }
        }
        HooksView::Matcher {
            event,
            matcher,
            selected,
        } => model
            .events
            .get(event)?
            .matcher_groups
            .get(matcher)?
            .configs
            .get(selected)
            .map(|_| HooksView::Config {
                event,
                matcher,
                config: selected,
            }),
        HooksView::Config { .. } => None,
    }
}

fn selection_scroll(terminal: &ChatTerminal, model: &HooksDialogModel, view: HooksView) -> usize {
    let Ok(size) = terminal.terminal.size() else {
        return 0;
    };
    let visible = usize::from(size.height).saturating_sub(7).max(1);
    let (row, prelude) = match view {
        HooksView::List { selected } => (selected.saturating_add(3), 3),
        HooksView::Event { event, selected } => {
            let Some(info) = model.events.get(event) else {
                return 0;
            };
            let prelude = event_header(info).len();
            (prelude.saturating_add(selected), prelude)
        }
        HooksView::Matcher {
            event,
            matcher: _,
            selected,
        } => {
            let Some(info) = model.events.get(event) else {
                return 0;
            };
            let prelude = event_header(info).len().saturating_add(2);
            (prelude.saturating_add(selected), prelude)
        }
        HooksView::Config { .. } => return 0,
    };
    if row < prelude.saturating_add(visible) {
        0
    } else {
        row.saturating_sub(visible.saturating_sub(1))
    }
}

fn render_hooks(
    frame: &mut Frame<'_>,
    model: &HooksDialogModel,
    view: HooksView,
    scroll: usize,
) -> usize {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(area);
    let (title, lines) = render_view(model, view);
    frame.render_widget(
        Block::default()
            .title(Span::styled(
                format!(" {title} "),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ))
            .borders(Borders::ALL),
        chunks[0],
    );
    let body_block = Block::default().borders(Borders::LEFT | Borders::RIGHT);
    let body_area = body_block.inner(chunks[1]);
    frame.render_widget(body_block, chunks[1]);
    let body = Paragraph::new(lines.clone())
        .wrap(Wrap { trim: false })
        .scroll((scroll.min(u16::MAX as usize) as u16, 0));
    frame.render_widget(body, body_area);
    let footer = Paragraph::new(
        " ↑/↓ or j/k navigate · Enter select · Esc go back · q close · PgUp/PgDn scroll ",
    )
    .style(Style::default().fg(Color::Gray))
    .block(Block::default().borders(Borders::LEFT | Borders::RIGHT | Borders::BOTTOM));
    frame.render_widget(footer, chunks[2]);
    lines.len().saturating_sub(usize::from(body_area.height))
}

fn render_view(model: &HooksDialogModel, view: HooksView) -> (String, Vec<Line<'static>>) {
    match view {
        HooksView::List { selected } => render_list(model, selected),
        HooksView::Event { event, selected } => render_event(model, event, selected),
        HooksView::Matcher {
            event,
            matcher,
            selected,
        } => render_matcher(model, event, matcher, selected),
        HooksView::Config {
            event,
            matcher,
            config,
        } => render_config(model, event, matcher, config),
    }
}

fn render_disabled(model: &HooksDialogModel) -> (String, Vec<Line<'static>>) {
    let count = model.configured_hook_count();
    let hook_text = if count == 1 {
        "1 configured hook".to_owned()
    } else {
        format!("{count} configured hooks")
    };
    let mut lines = vec![
        Line::styled(
            "Hook Configuration - Disabled",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(format!(
            "All hooks are currently disabled. You have {hook_text} that are not running."
        )),
        Line::raw(""),
        Line::styled(
            "When hooks are disabled:",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Line::raw("  · No hook commands will execute"),
        Line::raw("  · StatusLine will not be displayed"),
        Line::raw("  · Tool operations will proceed without hook validation"),
        Line::raw(""),
        Line::raw(
            "To re-enable hooks, remove \"disableAllHooks\" from settings.json or ask Canopy.",
        ),
    ];
    if count == 0 {
        lines.insert(2, Line::raw("No hooks are configured."));
    }
    ("Hooks disabled".to_owned(), lines)
}

fn render_list(model: &HooksDialogModel, selected: usize) -> (String, Vec<Line<'static>>) {
    if model.disable_all_hooks {
        return render_disabled(model);
    }
    let count = model.configured_hook_count();
    let configured = if count == 1 {
        "1 hook configured".to_owned()
    } else {
        format!("{count} hooks configured")
    };
    let mut lines = vec![
        Line::from(vec![
            Span::styled("Hooks", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(" · {configured}")),
        ]),
        Line::raw(
            "This menu is read-only. To add or modify hooks, edit settings.json directly or ask Canopy Code.",
        ),
        Line::raw(""),
    ];
    for (index, event) in model.events.iter().enumerate() {
        let marker = if index == selected { "❯" } else { " " };
        let config_count = event.config_count();
        let count_label = if config_count == 0 {
            String::new()
        } else {
            format!(" ({config_count})")
        };
        let style = if index == selected {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        lines.push(Line::from(vec![
            Span::styled(
                format!("{marker} {:>2}. {}", index + 1, event_name(event.event)),
                style,
            ),
            Span::styled(count_label, Style::default().fg(Color::Green)),
            Span::raw(format!("  {}", event.short_description)),
        ]));
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("Enter to select · Esc to close"));
    ("Hooks".to_owned(), lines)
}

fn render_event(
    model: &HooksDialogModel,
    event_index: usize,
    selected: usize,
) -> (String, Vec<Line<'static>>) {
    let Some(event) = model.events.get(event_index) else {
        return (
            "Hooks".to_owned(),
            vec![Line::raw("Hook event is unavailable.")],
        );
    };
    let mut lines = event_header(event);
    if hook_event_supports_matcher(event.event) {
        if event.matcher_groups.is_empty() {
            lines.extend([
                Line::raw("No hooks configured for this event."),
                Line::raw("To add hooks, edit settings.json directly or ask Canopy."),
            ]);
        } else {
            for (index, group) in event.matcher_groups.iter().enumerate() {
                let marker = if index == selected { "❯" } else { " " };
                let source = unique_sources(&group.configs);
                let count = group.configs.len();
                let count_label = if count == 1 { "1 hook" } else { "hooks" };
                let style = selection_style(index == selected);
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{marker} {}. [{source}] {}", index + 1, group.matcher),
                        style,
                    ),
                    Span::raw(format!("  {count} {count_label}")),
                    if group.sequential {
                        Span::styled(" · sequential", Style::default().fg(Color::Yellow))
                    } else {
                        Span::raw("")
                    },
                ]));
            }
        }
    } else {
        let configs = event.flat_configs();
        if configs.is_empty() {
            lines.extend([
                Line::raw("No hooks configured for this event."),
                Line::raw("To add hooks, edit settings.json directly or ask Canopy."),
            ]);
        } else {
            lines.push(Line::styled(
                "Configured hooks:",
                Style::default().add_modifier(Modifier::BOLD),
            ));
            for (index, config) in configs.iter().enumerate() {
                lines.push(config_row(config, index, selected));
            }
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("Enter to select · Esc to go back"));
    (event_name(event.event).to_owned(), lines)
}

fn render_matcher(
    model: &HooksDialogModel,
    event_index: usize,
    matcher_index: usize,
    selected: usize,
) -> (String, Vec<Line<'static>>) {
    let Some(event) = model.events.get(event_index) else {
        return (
            "Hooks".to_owned(),
            vec![Line::raw("Hook event is unavailable.")],
        );
    };
    let Some(group) = event.matcher_groups.get(matcher_index) else {
        return (
            event_name(event.event).to_owned(),
            vec![Line::raw("Matcher is unavailable.")],
        );
    };
    let title = format!("{} · Matcher: {}", event_name(event.event), group.matcher);
    let mut lines = event_header(event);
    lines.push(Line::styled(
        format!(
            "Matcher: {}{}",
            group.matcher,
            if group.sequential {
                " · sequential"
            } else {
                ""
            }
        ),
        Style::default().add_modifier(Modifier::BOLD),
    ));
    lines.push(Line::raw(""));
    if group.configs.is_empty() {
        lines.push(Line::raw("No hooks configured for this matcher."));
    } else {
        lines.push(Line::styled(
            "Configured hooks:",
            Style::default().add_modifier(Modifier::BOLD),
        ));
        for (index, config) in group.configs.iter().enumerate() {
            lines.push(config_row(config, index, selected));
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("Enter to select · Esc to go back"));
    (title, lines)
}

fn render_config(
    model: &HooksDialogModel,
    event_index: usize,
    matcher_index: usize,
    config_index: usize,
) -> (String, Vec<Line<'static>>) {
    let Some(event) = model.events.get(event_index) else {
        return (
            "Hook details".to_owned(),
            vec![Line::raw("Hook event is unavailable.")],
        );
    };
    let config = if matcher_index == usize::MAX {
        event.flat_configs().get(config_index).copied()
    } else {
        event
            .matcher_groups
            .get(matcher_index)
            .and_then(|group| group.configs.get(config_index))
    };
    let Some(config) = config else {
        return (
            "Hook details".to_owned(),
            vec![Line::raw("Hook configuration is unavailable.")],
        );
    };
    let kind = config
        .config
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let mut lines = vec![
        Line::styled(
            "Hook details",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        labeled("Event:", event_name(event.event)),
    ];
    if hook_event_supports_matcher(event.event) {
        lines.push(labeled("Matcher:", &config.matcher));
    }
    lines.push(labeled("Type:", kind));
    lines.push(labeled("Source:", &config.source_display));
    if let Some(path) = config.source_path.as_deref() {
        lines.push(labeled("Path:", path));
    }
    if config.source == HooksConfigSource::Extensions {
        if let Some(extension) = string_field(&config.config, "extensionName")
            .or_else(|| string_field(&config.config, "sourceDisplay"))
        {
            lines.push(labeled("Extension:", &extension));
        }
    }
    if let Some(name) = string_field(&config.config, "name") {
        lines.push(labeled("Name:", &name));
    }
    if let Some(description) = string_field(&config.config, "description") {
        lines.push(labeled("Desc:", &description));
    }
    if !config.enabled {
        lines.push(Line::styled(
            "Runtime state: disabled",
            Style::default().fg(Color::Yellow),
        ));
    }
    if config.sequential {
        lines.push(Line::raw("Execution: sequential"));
    }
    for (key, label) in [
        ("command", "Command:"),
        ("prompt", "Prompt:"),
        ("url", "URL:"),
    ] {
        if let Some(value) = string_field(&config.config, key) {
            lines.push(Line::raw(""));
            lines.push(Line::styled(label, Style::default().fg(Color::Gray)));
            lines.extend(multiline_value(&value));
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw(
        "To modify or remove this hook, edit settings.json directly or ask Canopy to help.",
    ));
    lines.push(Line::raw("Esc to go back"));
    ("Hook details".to_owned(), lines)
}

fn event_header(event: &HookEventDisplay) -> Vec<Line<'static>> {
    let mut lines = vec![Line::raw(event.short_description)];
    if !event.description.is_empty() {
        lines.push(Line::raw(event.description));
    }
    if !event.exit_codes.is_empty() {
        lines.push(Line::raw(""));
        for exit in event.exit_codes {
            lines.push(Line::raw(format!("{} - {}", exit.code, exit.description)));
        }
    }
    lines.push(Line::raw(""));
    lines
}

fn config_row(config: &HookConfigDisplay, index: usize, selected: usize) -> Line<'static> {
    let kind = config
        .config
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let async_suffix = (kind == "command"
        && config.config.get("async").and_then(Value::as_bool) == Some(true))
    .then_some(" async")
    .unwrap_or("");
    let style = selection_style(index == selected);
    let description = config_description(config);
    let state = if config.enabled {
        ""
    } else {
        " · runtime disabled"
    };
    Line::from(vec![
        Span::styled(
            format!(
                "{} {:>2}. [{}{}] {}",
                if index == selected { "❯" } else { " " },
                index + 1,
                kind,
                async_suffix,
                description
            ),
            style,
        ),
        Span::styled(
            format!("  {}{}", source_label(config.source), state),
            Style::default().fg(Color::Gray),
        ),
    ])
}

fn config_description(config: &HookConfigDisplay) -> String {
    let config = &config.config;
    let kind = config
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let candidate = match kind {
        "command" => string_field(config, "command"),
        "http" => string_field(config, "name").or_else(|| string_field(config, "url")),
        "function" => string_field(config, "name").or_else(|| string_field(config, "id")),
        "prompt" => string_field(config, "name").or_else(|| string_field(config, "prompt")),
        _ => None,
    };
    candidate
        .map(|value| truncate_chars(&value, 64))
        .unwrap_or_else(|| "unnamed hook".to_owned())
}

fn labeled(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<12}"), Style::default().fg(Color::Gray)),
        Span::raw(safe_single_line(value)),
    ])
}

fn multiline_value(value: &str) -> Vec<Line<'static>> {
    value
        .lines()
        .map(|line| Line::raw(safe_single_line(line)))
        .collect()
}

fn selection_style(selected: bool) -> Style {
    if selected {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    }
}

fn source_label(source: HooksConfigSource) -> &'static str {
    match source {
        HooksConfigSource::Project => "Local Settings",
        HooksConfigSource::User => "User Settings",
        HooksConfigSource::System => "System Settings",
        HooksConfigSource::Extensions => "Extensions",
        HooksConfigSource::Session => "Session (temporary)",
    }
}

fn plain_source_label(source: HooksConfigSource) -> &'static str {
    match source {
        HooksConfigSource::Project => "Project",
        HooksConfigSource::User => "User",
        HooksConfigSource::System => "System",
        HooksConfigSource::Extensions => "Extension",
        HooksConfigSource::Session => "Session (temporary)",
    }
}

fn plain_hook_name(config: &HookConfigDisplay) -> String {
    string_field(&config.config, "name")
        .or_else(|| {
            (config.config.get("type").and_then(Value::as_str) == Some("command"))
                .then(|| string_field(&config.config, "command"))
                .flatten()
        })
        .or_else(|| {
            (config.config.get("type").and_then(Value::as_str) == Some("http"))
                .then(|| string_field(&config.config, "url"))
                .flatten()
        })
        .unwrap_or_else(|| "unnamed".to_owned())
}

fn extension_display(config: &Value, source: HooksConfigSource) -> String {
    if source == HooksConfigSource::Extensions {
        return string_field(config, "extensionName")
            .or_else(|| string_field(config, "sourceDisplay"))
            .unwrap_or_else(|| source_label(source).to_owned());
    }
    source_label(source).to_owned()
}

fn unique_sources(configs: &[HookConfigDisplay]) -> String {
    let mut sources = Vec::new();
    for config in configs {
        let label = if config.source == HooksConfigSource::Extensions
            && config.source_display != source_label(config.source)
        {
            format!("Extension ({})", safe_single_line(&config.source_display))
        } else {
            source_label(config.source).to_owned()
        };
        if !sources.contains(&label) {
            sources.push(label);
        }
    }
    sources.join(", ")
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(safe_single_line)
}

fn safe_single_line(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn function_config_for_display(config: &FunctionHookConfig) -> Value {
    let mut value = json!({
        "type": "function",
        "id": config.id,
        "name": config.name,
        "description": config.description,
        "statusMessage": config.status_message,
    });
    if let Value::Object(object) = &mut value {
        for (key, value) in &config.extra {
            object.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    value
}

const HOOK_EVENTS: [HookEventName; 22] = [
    HookEventName::PreToolUse,
    HookEventName::PostToolUse,
    HookEventName::PostToolUseFailure,
    HookEventName::PostToolBatch,
    HookEventName::Notification,
    HookEventName::UserPromptSubmit,
    HookEventName::UserPromptExpansion,
    HookEventName::SessionStart,
    HookEventName::Stop,
    HookEventName::MessageDisplay,
    HookEventName::SubagentStart,
    HookEventName::SubagentStop,
    HookEventName::PreCompact,
    HookEventName::PostCompact,
    HookEventName::SessionEnd,
    HookEventName::SessionDelete,
    HookEventName::PermissionRequest,
    HookEventName::PermissionDenied,
    HookEventName::StopFailure,
    HookEventName::TodoCreated,
    HookEventName::TodoCompleted,
    HookEventName::InstructionsLoaded,
];

fn event_name(event: HookEventName) -> &'static str {
    match event {
        HookEventName::PreToolUse => "PreToolUse",
        HookEventName::PostToolUse => "PostToolUse",
        HookEventName::PostToolUseFailure => "PostToolUseFailure",
        HookEventName::PostToolBatch => "PostToolBatch",
        HookEventName::Notification => "Notification",
        HookEventName::UserPromptSubmit => "UserPromptSubmit",
        HookEventName::UserPromptExpansion => "UserPromptExpansion",
        HookEventName::SessionStart => "SessionStart",
        HookEventName::Stop => "Stop",
        HookEventName::MessageDisplay => "MessageDisplay",
        HookEventName::SubagentStart => "SubagentStart",
        HookEventName::SubagentStop => "SubagentStop",
        HookEventName::PreCompact => "PreCompact",
        HookEventName::PostCompact => "PostCompact",
        HookEventName::SessionEnd => "SessionEnd",
        HookEventName::SessionDelete => "SessionDelete",
        HookEventName::PermissionRequest => "PermissionRequest",
        HookEventName::PermissionDenied => "PermissionDenied",
        HookEventName::StopFailure => "StopFailure",
        HookEventName::TodoCreated => "TodoCreated",
        HookEventName::TodoCompleted => "TodoCompleted",
        HookEventName::InstructionsLoaded => "InstructionsLoaded",
    }
}

fn event_metadata(
    event: HookEventName,
) -> (&'static str, &'static str, &'static [ExitCodeDisplay]) {
    use HookEventName::*;
    match event {
        PreToolUse => (
            "Before tool execution",
            "Input to command is JSON of tool call arguments.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "show stderr to model and block tool call",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only; continue with tool call",
                },
            ],
        ),
        PostToolUse => (
            "After tool execution",
            "Input includes tool arguments and the tool response.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown in transcript mode",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "show stderr to model immediately",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        PostToolUseFailure => (
            "After tool execution fails",
            "Input includes tool name, arguments, error details, and timeout state.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown in transcript mode",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "show stderr to model immediately",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        PostToolBatch => (
            "After all tool calls in a batch resolve",
            "Input contains the resolved tool calls in the batch.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown in transcript mode",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "show stderr to model immediately",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        Notification => (
            "When notifications are sent",
            "Input contains the notification message and type.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        UserPromptSubmit => (
            "When the user submits a prompt",
            "Input contains the model-bound prompt and, for interactive TUI text, the submitted prompt.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown to Canopy",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "block processing and show stderr to user",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        UserPromptExpansion => (
            "When a slash command expands into a prompt",
            "Input includes command name, arguments, and expanded prompt text.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown to Canopy",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "block expanded prompt submission",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        SessionStart => (
            "When a new session is started",
            "Input contains the session start source.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown to Canopy",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only; blocking errors ignored",
                },
            ],
        ),
        Stop => (
            "Right before Canopy Code concludes its response",
            "",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "show stderr to model and continue conversation",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        MessageDisplay => (
            "Repeatedly, as the assistant reply streams",
            "Input contains the displayed text so far and whether the message is final. This event is fire-and-forget.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "fire-and-forget; output and exit status are ignored",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "fire-and-forget; output and exit status are ignored",
                },
            ],
        ),
        SubagentStart => (
            "When a subagent (Agent tool call) is started",
            "Input contains the agent ID and agent type.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout shown to subagent",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only; blocking errors ignored",
                },
            ],
        ),
        SubagentStop => (
            "Right before a subagent concludes its response",
            "Input contains the agent ID, type, and transcript path.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "show stderr to subagent and continue having it run",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        PreCompact => (
            "Before conversation compaction",
            "Input contains compaction details.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout appended as custom compact instructions",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "block compaction",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only; continue compaction",
                },
            ],
        ),
        PostCompact => (
            "After conversation compaction",
            "Input contains the manual/automatic trigger and compact summary. Output does not affect control flow.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        SessionEnd => (
            "When a session is ending",
            "Input contains the session end reason.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "command completes successfully",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        SessionDelete => (
            "After an explicitly selected session is deleted",
            "Input contains the deleted session ID. This event is fire-and-forget.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "fire-and-forget; exit status is ignored",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "fire-and-forget; exit status is ignored",
                },
            ],
        ),
        PermissionRequest => (
            "When a permission dialog is displayed",
            "Input contains tool name, arguments, and tool call ID. Output JSON may provide an allow/deny decision.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "use hook decision if provided",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        PermissionDenied => (
            "When a tool call is denied before a permission dialog is displayed",
            "Input contains tool name, arguments, tool call ID, and denial reason.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        StopFailure => (
            "When the turn ends due to an API error (fires instead of Stop)",
            "Input contains the error category and optional details. This event is fire-and-forget.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "fire-and-forget; exit status is ignored",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "fire-and-forget; exit status is ignored",
                },
            ],
        ),
        TodoCreated => (
            "When a new todo item is created",
            "Input includes the todo, all todos, and phase. Validation output may allow or block creation.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "allow todo creation",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "block creation and show reason to model",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        TodoCompleted => (
            "When a todo item is marked as completed",
            "Input includes todo, previous status, all todos, and phase. Validation output may allow or block completion.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "allow todo completion",
                },
                ExitCodeDisplay {
                    code: "2",
                    description: "block completion and show reason to model",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
        InstructionsLoaded => (
            "When instruction files are loaded",
            "Input contains file path, memory type, load reason, and optional parent/trigger file paths.",
            &[
                ExitCodeDisplay {
                    code: "0",
                    description: "stdout/stderr not shown",
                },
                ExitCodeDisplay {
                    code: "Other",
                    description: "show stderr to user only",
                },
            ],
        ),
    }
}
