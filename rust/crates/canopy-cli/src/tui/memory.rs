use std::cell::Cell;

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::{ChatTerminal, RawMode};

const TOGGLE_LABELS: [&str; 4] = [
    "Auto-memory",
    "Auto-dream",
    "Auto-skill",
    "Confirm auto-skills before saving",
];

pub(crate) struct MemoryDialogModel {
    pub(crate) status_rows: Vec<String>,
    pub(crate) toggle_values: [bool; 4],
    pub(crate) target_labels: [String; 2],
}

#[derive(Clone, Copy)]
enum MemoryFocus {
    Toggle(usize),
    Target(usize),
}

impl ChatTerminal {
    /// Show the memory status and settings view. Toggle persistence is supplied
    /// by the caller so it can use the active workspace settings path.
    pub fn show_memory_dialog<F>(
        &mut self,
        model: &mut MemoryDialogModel,
        mut persist_toggle: F,
    ) -> Result<Option<usize>, String>
    where
        F: FnMut(usize, bool) -> Result<(), String>,
    {
        let raw_mode = RawMode::enter()?;
        let result = self.memory_event_loop(model, &mut persist_toggle);
        drop(raw_mode);
        let redraw_result = self.draw(None);
        match (result, redraw_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(action), Ok(())) => Ok(action),
        }
    }

    /// Run an editor or folder opener outside the full-screen terminal, then
    /// restore the chat view even when the external action reports an error.
    pub fn run_memory_external_action<F>(&mut self, action: F) -> Result<(), String>
    where
        F: FnOnce() -> Result<(), String>,
    {
        self.suspend_for_tool()?;
        let action_result = action();
        let resume_result = self.resume_from_tool();
        if let Err(error) = action_result {
            let _ = resume_result;
            return Err(error);
        }
        resume_result?;
        self.status = "Ready for your next message".to_owned();
        self.draw(None)
    }

    fn memory_event_loop<F>(
        &mut self,
        model: &mut MemoryDialogModel,
        persist_toggle: &mut F,
    ) -> Result<Option<usize>, String>
    where
        F: FnMut(usize, bool) -> Result<(), String>,
    {
        let mut focus = MemoryFocus::Target(0);
        let mut scroll = 0usize;
        let mut error = None;
        loop {
            let scroll_limit = Cell::new(0usize);
            self.terminal
                .draw(|frame| {
                    scroll_limit.set(render_memory(frame, model, focus, scroll, error.as_deref()));
                })
                .map_err(|error| format!("could not render memory view: {error}"))?;
            scroll = scroll.min(scroll_limit.get());

            if !event::poll(std::time::Duration::from_millis(250))
                .map_err(|error| format!("could not read memory view input: {error}"))?
            {
                continue;
            }
            let event = event::read()
                .map_err(|error| format!("could not read memory view input: {error}"))?;
            let Event::Key(key) = event else {
                continue;
            };
            if key.kind == crossterm::event::KeyEventKind::Release {
                continue;
            }
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                return Ok(None);
            }

            let page = usize::from(
                self.terminal
                    .size()
                    .map(|size| size.height.saturating_sub(6))
                    .unwrap_or(12),
            )
            .max(1);
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => return Ok(None),
                KeyCode::Up | KeyCode::Char('k') => {
                    focus = match focus {
                        MemoryFocus::Toggle(index) if index > 0 => MemoryFocus::Toggle(index - 1),
                        MemoryFocus::Toggle(_) => focus,
                        MemoryFocus::Target(0) => MemoryFocus::Toggle(3),
                        MemoryFocus::Target(index) => MemoryFocus::Target(index - 1),
                    };
                    scroll = 0;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    focus = match focus {
                        MemoryFocus::Toggle(index) if index + 1 < TOGGLE_LABELS.len() => {
                            MemoryFocus::Toggle(index + 1)
                        }
                        MemoryFocus::Toggle(_) => MemoryFocus::Target(0),
                        MemoryFocus::Target(index) => MemoryFocus::Target((index + 1) % 2),
                    };
                    scroll = 0;
                }
                KeyCode::PageUp => scroll = scroll.saturating_sub(page),
                KeyCode::PageDown => scroll = scroll.saturating_add(page),
                KeyCode::Home => scroll = 0,
                KeyCode::End => scroll = scroll_limit.get(),
                KeyCode::Char('1') | KeyCode::Char('2')
                    if matches!(focus, MemoryFocus::Target(_)) =>
                {
                    let target = usize::from(key.code == KeyCode::Char('2'));
                    return Ok(Some(target));
                }
                KeyCode::Enter => match focus {
                    MemoryFocus::Toggle(index) => {
                        let next_value = !model.toggle_values[index];
                        match persist_toggle(index, next_value) {
                            Ok(()) => {
                                model.toggle_values[index] = next_value;
                                error = None;
                            }
                            Err(message) => error = Some(message),
                        }
                    }
                    MemoryFocus::Target(index) => return Ok(Some(index)),
                },
                _ => {}
            }
        }
    }
}

fn render_memory(
    frame: &mut Frame<'_>,
    model: &MemoryDialogModel,
    focus: MemoryFocus,
    scroll: usize,
    error: Option<&str>,
) -> usize {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let title = Paragraph::new(" Canopy · Memory ").block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    let mut lines = Vec::new();
    for (index, label) in TOGGLE_LABELS.iter().enumerate() {
        let selected = matches!(focus, MemoryFocus::Toggle(focused) if focused == index);
        let marker = if selected { "› " } else { "  " };
        let state = if model.toggle_values[index] {
            "on"
        } else {
            "off"
        };
        let style = if selected {
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        };
        lines.push(Line::from(Span::styled(
            format!("{marker}{label}: {state}"),
            style,
        )));
    }
    lines.push(Line::raw(""));
    for (index, label) in model.target_labels.iter().enumerate() {
        let selected = matches!(focus, MemoryFocus::Target(focused) if focused == index);
        lines.push(Line::from(Span::styled(
            format!("{}{}", if selected { "› " } else { "  " }, label),
            if selected {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            },
        )));
    }
    lines.push(Line::raw(""));
    lines.extend(model.status_rows.iter().cloned().map(Line::raw));
    if let Some(error) = error {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            error.to_owned(),
            Style::default().fg(Color::Red),
        ));
    }

    let body_block = Block::default()
        .title(" Current workspace memory ")
        .borders(Borders::ALL);
    let body_area = body_block.inner(chunks[1]);
    let max_scroll =
        visual_row_count(&lines, body_area.width).saturating_sub(usize::from(body_area.height));
    let body = Paragraph::new(lines)
        .block(body_block)
        .wrap(Wrap { trim: false })
        .scroll((scroll.min(max_scroll).min(u16::MAX as usize) as u16, 0));
    frame.render_widget(body, chunks[1]);

    let footer = Paragraph::new(
        " ↑/↓ or j/k navigate · Enter toggle/open · 1/2 open · PgUp/PgDn scroll · Esc/q close ",
    )
    .style(Style::default().fg(Color::Gray));
    frame.render_widget(footer, chunks[2]);
    max_scroll
}

fn visual_row_count(lines: &[Line<'_>], width: u16) -> usize {
    let width = usize::from(width.max(1));
    lines
        .iter()
        .map(|line| line.width().max(1).div_ceil(width))
        .sum()
}
