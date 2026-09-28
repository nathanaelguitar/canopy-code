//! Bounded Markdown presentation for the native conversation view.
//!
//! This intentionally handles the common prose, list, quote, link, and code
//! forms used in model replies without interpreting embedded HTML or terminal
//! control sequences. The transcript itself is already sanitized and capped;
//! this renderer also caps inline spans per source line to bound redraw memory.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

const MAX_INLINE_SPANS_PER_LINE: usize = 128;
const MAX_TABLE_COLUMNS: usize = 16;

enum FenceLine<'a> {
    Open(&'a str),
    Close,
    Code,
}

pub(super) fn render_suffix(markdown: &str, max_lines: usize) -> (Vec<Line<'static>>, bool) {
    if markdown.is_empty() {
        return (vec![Line::raw("")], false);
    }
    let source_lines = markdown.lines().count();
    if max_lines == 0 {
        return (Vec::new(), source_lines > 0);
    }
    let skipped = source_lines.saturating_sub(max_lines);

    let mut lines = Vec::new();
    let mut fence: Option<(u8, usize)> = None;
    for source_line in markdown.lines().take(skipped) {
        let _ = classify_fence(source_line, &mut fence);
    }

    for source_line in markdown.lines().skip(skipped) {
        match classify_fence(source_line, &mut fence) {
            Some(FenceLine::Open(info)) => {
                let mut spans = vec![Span::styled("┌─", Style::default().fg(Color::DarkGray))];
                let language = info.trim();
                if !language.is_empty() {
                    spans.push(Span::styled(
                        format!(" {language}"),
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    ));
                }
                lines.push(Line::from(spans));
            }
            Some(FenceLine::Close) => lines.push(Line::from(Span::styled(
                "└─",
                Style::default().fg(Color::DarkGray),
            ))),
            Some(FenceLine::Code) => lines.push(code_line(source_line)),
            None => lines.push(render_block_line(source_line)),
        }
    }

    if fence.is_some() && lines.len() < max_lines {
        lines.push(Line::from(Span::styled(
            "└─",
            Style::default().fg(Color::DarkGray),
        )));
    }

    (lines, skipped > 0)
}

fn classify_fence<'a>(
    source_line: &'a str,
    fence: &mut Option<(u8, usize)>,
) -> Option<FenceLine<'a>> {
    let trimmed = source_line.trim_start();
    if let Some((marker, width, info)) = opening_fence(trimmed) {
        return match *fence {
            Some((active_marker, active_width))
                if active_marker == marker && closing_fence(trimmed, marker, active_width) =>
            {
                *fence = None;
                Some(FenceLine::Close)
            }
            Some(_) => Some(FenceLine::Code),
            None => {
                *fence = Some((marker, width));
                Some(FenceLine::Open(info))
            }
        };
    }

    let (marker, width) = (*fence)?;
    if closing_fence(trimmed, marker, width) {
        *fence = None;
        Some(FenceLine::Close)
    } else {
        Some(FenceLine::Code)
    }
}

fn opening_fence(line: &str) -> Option<(u8, usize, &str)> {
    let bytes = line.as_bytes();
    let marker = *bytes.first()?;
    if marker != b'`' && marker != b'~' {
        return None;
    }
    let width = bytes.iter().take_while(|byte| **byte == marker).count();
    (width >= 3).then(|| (marker, width, &line[width..]))
}

fn closing_fence(line: &str, marker: u8, minimum_width: usize) -> bool {
    let bytes = line.as_bytes();
    let width = bytes.iter().take_while(|byte| **byte == marker).count();
    width >= minimum_width && bytes[width..].iter().all(u8::is_ascii_whitespace)
}

fn code_line(source_line: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled("│ ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            source_line.to_owned(),
            Style::default().fg(Color::Yellow).bg(Color::Black),
        ),
    ])
}

fn render_block_line(source_line: &str) -> Line<'static> {
    let trimmed = source_line.trim_start();

    if let Some(cells) = markdown_table_cells(source_line) {
        if is_markdown_table_separator(&cells) {
            let separators = cells
                .iter()
                .map(|cell| "─".repeat(cell.chars().count().clamp(3, 24)))
                .collect::<Vec<_>>()
                .join("┼");
            return Line::from(Span::styled(
                format!("├{separators}┤"),
                Style::default().fg(Color::DarkGray),
            ));
        }

        let mut spans = vec![Span::styled("│ ", Style::default().fg(Color::DarkGray))];
        for (index, cell) in cells.iter().enumerate() {
            if index > 0 {
                spans.push(Span::styled(" │ ", Style::default().fg(Color::DarkGray)));
            }
            spans.push(Span::raw((*cell).to_owned()));
        }
        spans.push(Span::styled(" │", Style::default().fg(Color::DarkGray)));
        return Line::from(spans);
    }

    if is_horizontal_rule(trimmed) {
        return Line::from(Span::styled(
            "────────────────",
            Style::default().fg(Color::DarkGray),
        ));
    }

    if let Some((level, content)) = heading(trimmed) {
        let style = Style::default()
            .fg(if level <= 2 { Color::Cyan } else { Color::Blue })
            .add_modifier(Modifier::BOLD);
        return inline_line(content, style);
    }

    if let Some(content) = trimmed
        .strip_prefix("> ")
        .or_else(|| (trimmed == ">").then_some(""))
    {
        let mut spans = vec![Span::styled("│ ", Style::default().fg(Color::DarkGray))];
        spans.extend(
            inline_line(
                content,
                Style::default()
                    .fg(Color::Gray)
                    .add_modifier(Modifier::ITALIC),
            )
            .spans,
        );
        return Line::from(spans);
    }

    if let Some((indent, marker, content)) = list_item(source_line) {
        let mut spans = vec![Span::raw(" ".repeat(indent.min(32)))];
        spans.push(Span::styled(
            marker,
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ));
        spans.extend(inline_line(content, Style::default()).spans);
        return Line::from(spans);
    }

    inline_line(source_line, Style::default())
}

fn markdown_table_cells(line: &str) -> Option<Vec<&str>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') && !trimmed.ends_with('|') {
        return None;
    }

    let body = trimmed.strip_prefix('|').unwrap_or(trimmed);
    let body = body.strip_suffix('|').unwrap_or(body);
    let mut cells = body
        .split('|')
        .map(str::trim)
        .take(MAX_TABLE_COLUMNS + 1)
        .collect::<Vec<_>>();
    if cells.len() < 2 {
        return None;
    }
    if cells.len() > MAX_TABLE_COLUMNS {
        cells.truncate(MAX_TABLE_COLUMNS - 1);
        cells.push("…");
    }
    Some(cells)
}

fn is_markdown_table_separator(cells: &[&str]) -> bool {
    cells.len() >= 2
        && cells.iter().all(|cell| {
            let bytes = cell.as_bytes();
            *cell == "…"
                || (!bytes.is_empty()
                    && bytes.iter().any(|byte| *byte == b'-')
                    && bytes.iter().all(|byte| matches!(byte, b'-' | b':')))
        })
}

fn heading(line: &str) -> Option<(usize, &str)> {
    let count = line
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    if !(1..=6).contains(&count)
        || !line
            .as_bytes()
            .get(count)
            .is_some_and(u8::is_ascii_whitespace)
    {
        return None;
    }
    Some((count, line[count..].trim_start()))
}

fn is_horizontal_rule(line: &str) -> bool {
    let mut marker = None;
    let mut count = 0usize;
    for byte in line.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if !matches!(byte, b'-' | b'_' | b'*') || marker.is_some_and(|value| value != byte) {
            return false;
        }
        marker = Some(byte);
        count += 1;
    }
    count >= 3 && marker.is_some()
}

fn list_item(line: &str) -> Option<(usize, String, &str)> {
    let indent = line.len() - line.trim_start().len();
    let content = &line[indent..];
    let bytes = content.as_bytes();
    let marker = *bytes.first()?;
    if matches!(marker, b'-' | b'+' | b'*') && bytes.get(1).is_some_and(u8::is_ascii_whitespace) {
        return Some((indent, "• ".to_owned(), content[2..].trim_start()));
    }

    let digits = bytes
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits > 0
        && bytes.get(digits) == Some(&b'.')
        && bytes.get(digits + 1).is_some_and(u8::is_ascii_whitespace)
    {
        return Some((
            indent,
            format!("{} ", &content[..digits + 1]),
            content[digits + 2..].trim_start(),
        ));
    }
    None
}

fn inline_line(text: &str, base: Style) -> Line<'static> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut bold = false;
    let mut italic = false;
    let mut strike = false;
    let mut code = false;
    let mut cursor = 0usize;
    let mut run_start = 0usize;
    // Move these searches forward as the cursor advances so malformed links
    // with many `[` characters cannot repeatedly scan the same suffix.
    let mut next_label_close = text.find("](");
    let mut next_url_close = text.find(')');

    while cursor < bytes.len() {
        let marker_width = if code {
            text[cursor..].starts_with('`').then_some(1)
        } else if text[cursor..].starts_with("**") || text[cursor..].starts_with("__") {
            Some(2)
        } else if text[cursor..].starts_with("~~") {
            Some(2)
        } else if text[cursor..].starts_with('*') || text[cursor..].starts_with('_') {
            Some(1)
        } else if text[cursor..].starts_with('`') {
            Some(1)
        } else {
            None
        };

        if let Some(width) = marker_width {
            push_run(
                &mut spans,
                &text[run_start..cursor],
                inline_style(base, bold, italic, strike, code),
            );
            if spans.len() >= MAX_INLINE_SPANS_PER_LINE {
                if cursor < text.len() {
                    spans.push(Span::styled(text[cursor..].to_owned(), base));
                }
                run_start = text.len();
                break;
            }
            if text[cursor..].starts_with("**") || text[cursor..].starts_with("__") {
                bold = !bold;
            } else if text[cursor..].starts_with("~~") {
                strike = !strike;
            } else if text[cursor..].starts_with('`') {
                code = !code;
            } else {
                italic = !italic;
            }
            cursor += width;
            run_start = cursor;
            continue;
        }

        if !code && bytes[cursor] == b'[' {
            if let Some((end, label, url)) =
                link_at(text, cursor, &mut next_label_close, &mut next_url_close)
            {
                push_run(
                    &mut spans,
                    &text[run_start..cursor],
                    inline_style(base, bold, italic, strike, code),
                );
                if spans.len().saturating_add(2) >= MAX_INLINE_SPANS_PER_LINE {
                    spans.push(Span::styled(text[cursor..].to_owned(), base));
                    run_start = text.len();
                    break;
                }
                spans.push(Span::styled(
                    label.to_owned(),
                    inline_style(base, true, italic, strike, false)
                        .add_modifier(Modifier::UNDERLINED),
                ));
                spans.push(Span::styled(
                    format!(" <{url}>"),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::UNDERLINED),
                ));
                cursor = end;
                run_start = end;
                continue;
            }
        }

        let character = text[cursor..]
            .chars()
            .next()
            .expect("cursor is within UTF-8 text");
        cursor += character.len_utf8();
    }

    if run_start < text.len() {
        push_run(
            &mut spans,
            &text[run_start..],
            inline_style(base, bold, italic, strike, code),
        );
    }
    if spans.is_empty() {
        spans.push(Span::styled(String::new(), base));
    }
    Line::from(spans)
}

fn link_at<'a>(
    text: &'a str,
    start: usize,
    next_label_close: &mut Option<usize>,
    next_url_close: &mut Option<usize>,
) -> Option<(usize, &'a str, &'a str)> {
    while next_label_close.is_some_and(|position| position <= start) {
        let after = (*next_label_close)?.checked_add(2)?;
        *next_label_close = text[after..].find("](").map(|offset| after + offset);
    }
    let label_start = start.checked_add(1)?;
    let label_end = (*next_label_close)?;
    let url_start = label_end.checked_add(2)?;

    while next_url_close.is_some_and(|position| position < url_start) {
        let after = (*next_url_close)?.checked_add(1)?;
        *next_url_close = text[after..].find(')').map(|offset| after + offset);
    }
    let url_end = (*next_url_close)?;
    Some((
        url_end.checked_add(1)?,
        &text[label_start..label_end],
        &text[url_start..url_end],
    ))
}

fn inline_style(base: Style, bold: bool, italic: bool, strike: bool, code: bool) -> Style {
    if code {
        return Style::default().fg(Color::Yellow).bg(Color::Black);
    }
    let mut style = base;
    if bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    if italic {
        style = style.add_modifier(Modifier::ITALIC);
    }
    if strike {
        style = style.add_modifier(Modifier::CROSSED_OUT);
    }
    style
}

fn push_run(spans: &mut Vec<Span<'static>>, value: &str, style: Style) {
    if !value.is_empty() {
        spans.push(Span::styled(value.to_owned(), style));
    }
}
