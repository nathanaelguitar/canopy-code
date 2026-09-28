use std::cell::Cell;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::{ChatTerminal, RawMode};

const HELP_POLL_INTERVAL: Duration = Duration::from_millis(250);

impl ChatTerminal {
    /// Show the native full-screen command and key guide.
    pub fn show_help(&mut self) -> Result<(), String> {
        let raw_mode = RawMode::enter()?;
        let result = self.help_event_loop();
        drop(raw_mode);
        let redraw_result = self.draw(None);
        match (result, redraw_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn help_event_loop(&mut self) -> Result<(), String> {
        let lines = help_lines();
        let mut scroll = 0usize;
        loop {
            let scroll_limit = Cell::new(0usize);
            self.terminal
                .draw(|frame| {
                    scroll_limit.set(render_help(frame, &lines, scroll));
                })
                .map_err(|error| format!("could not render help: {error}"))?;
            scroll = scroll.min(scroll_limit.get());

            if !event::poll(HELP_POLL_INTERVAL)
                .map_err(|error| format!("could not read help input: {error}"))?
            {
                continue;
            }
            let event =
                event::read().map_err(|error| format!("could not read help input: {error}"))?;
            let Event::Key(key) = event else {
                continue;
            };
            if key.kind == crossterm::event::KeyEventKind::Release {
                continue;
            }
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                break;
            }

            let page = usize::from(
                self.terminal
                    .size()
                    .map(|size| size.height.saturating_sub(6))
                    .unwrap_or(12),
            )
            .max(1);
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => break,
                KeyCode::Up | KeyCode::Char('k') => scroll = scroll.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    scroll = scroll.saturating_add(1).min(scroll_limit.get());
                }
                KeyCode::PageUp => scroll = scroll.saturating_sub(page),
                KeyCode::PageDown => {
                    scroll = scroll.saturating_add(page).min(scroll_limit.get());
                }
                KeyCode::Home => scroll = 0,
                KeyCode::End => scroll = scroll_limit.get(),
                _ => {}
            }
        }
        Ok(())
    }
}

fn render_help(frame: &mut Frame<'_>, lines: &[Line<'static>], scroll: usize) -> usize {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " Canopy ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("· Help"),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, chunks[0]);

    let body_block = Block::default()
        .title(" Commands and keyboard controls ")
        .borders(Borders::ALL);
    let body_area = body_block.inner(chunks[1]);
    let max_scroll =
        visual_row_count(lines, body_area.width).saturating_sub(usize::from(body_area.height));
    let body = Paragraph::new(lines.to_vec())
        .block(body_block)
        .wrap(Wrap { trim: false })
        .scroll((scroll.min(u16::MAX as usize) as u16, 0));
    frame.render_widget(body, chunks[1]);

    let footer = Paragraph::new(" ↑/↓ or j/k scroll · PgUp/PgDn page · Home/End · Esc/q close ")
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

fn help_lines() -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    push_section(&mut lines, "Commands");
    push_entry(&mut lines, "/help", "Show this help screen.");
    push_entry(&mut lines, "/exit, /quit", "End the interactive session.");
    push_entry(
        &mut lines,
        "/memory",
        "Show managed-memory settings, paths, topic counts, and recent tasks.",
    );
    push_entry(
        &mut lines,
        "/hooks",
        "Browse configured hooks by event, matcher, and source.",
    );
    push_entry(
        &mut lines,
        "/doctor",
        "Check the native runtime, configuration, tools, MCP connections, and Git.",
    );
    push_entry(
        &mut lines,
        "/doctor memory [--sample] [--json]",
        "Show native process and system memory diagnostics.",
    );
    push_entry(
        &mut lines,
        "/doctor rollback",
        "Restore the previous standalone installation.",
    );
    push_entry(
        &mut lines,
        "/stats, /usage",
        "No args opens live tabs; /usage is an alias for /stats.",
    );
    push_entry(
        &mut lines,
        "/stats model|tools|skills",
        "Show session metrics in the conversation.",
    );
    push_entry(
        &mut lines,
        "/stats daily|day [date]",
        "Show daily token usage; date is YYYY-MM-DD.",
    );
    push_entry(
        &mut lines,
        "/stats monthly|month [month]",
        "Show monthly token usage; month is YYYY-MM.",
    );
    push_entry(
        &mut lines,
        "/stats export <period> …",
        "Export daily/monthly history; supports --format and --output.",
    );

    push_section(&mut lines, "Message editor and conversation");
    push_entry(&mut lines, "Enter", "Send the message.");
    push_entry(&mut lines, "Ctrl+Enter", "Insert a newline.");
    push_entry(
        &mut lines,
        "Left/Right, Home/End",
        "Move the cursor; Home/End stay on the current line.",
    );
    push_entry(
        &mut lines,
        "Up/Down",
        "Move between lines; at the first/last line, browse prompt history.",
    );
    push_entry(&mut lines, "Backspace/Delete", "Delete text at the cursor.");
    push_entry(&mut lines, "PageUp/PageDown", "Scroll the conversation.");
    push_entry(
        &mut lines,
        "Ctrl+D",
        "Exit on empty input; otherwise delete at the cursor, if any.",
    );
    push_entry(
        &mut lines,
        "Ctrl+C",
        "Exit the prompt; request response cancellation; cancel dictation.",
    );
    push_entry(&mut lines, "Paste", "Multiline paste is supported.");
    push_entry(
        &mut lines,
        "Space (when enabled)",
        "Start voice dictation; hold/tap behavior follows voice settings.",
    );
    push_entry(
        &mut lines,
        "Esc during dictation",
        "Cancel the active voice recording.",
    );

    push_section(&mut lines, "Live usage view");
    push_entry(
        &mut lines,
        "Tab/Shift+Tab, ←/→, h/l",
        "Switch between Session, Models, Tools, and Skills.",
    );
    push_entry(&mut lines, "1–4", "Select a usage tab directly.");
    push_entry(&mut lines, "↑/↓, j/k, PgUp/PgDn", "Scroll the current tab.");
    push_entry(&mut lines, "Home/End", "Jump to the top or bottom.");
    push_entry(&mut lines, "Esc/q/Ctrl+C", "Close the usage view.");

    push_section(&mut lines, "Help view");
    push_entry(&mut lines, "Esc/q/Ctrl+C", "Return to the conversation.");
    lines
}

fn push_section(lines: &mut Vec<Line<'static>>, title: &'static str) {
    lines.push(Line::from(Span::styled(
        title,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
}

fn push_entry(lines: &mut Vec<Line<'static>>, keys: &'static str, description: &'static str) {
    lines.push(Line::from(vec![
        Span::styled(
            format!("  {keys:<27}"),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(description),
    ]));
}
