use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedAutoMemoryEntry {
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub how_to_apply: Option<String>,
}

/// Returns the first trimmed `# Heading` line, or the source default.
pub fn get_auto_memory_body_heading(body: &str) -> String {
    body.split('\n')
        .map(trim_js_whitespace)
        .find(|line| line.starts_with("# "))
        .unwrap_or("# Memory")
        .to_owned()
}

/// Parse both the newer one-entry body and legacy bullet-list documents.
pub fn parse_auto_memory_entries(body: &str) -> Vec<ManagedAutoMemoryEntry> {
    let mut entries = Vec::new();
    let mut current: Option<ManagedAutoMemoryEntry> = None;

    for raw_line in body.split('\n') {
        let trimmed = trim_js_whitespace(raw_line);
        if trimmed.is_empty() || trimmed == "_No entries yet._" || trimmed.starts_with("# ") {
            continue;
        }

        if let Some(current) = current.as_mut()
            && let Some((key, value)) = parse_indented_field(raw_line)
        {
            if !value.is_empty() {
                set_field(current, key, value);
            }
            continue;
        }

        if let Some((key, value)) = parse_named_field(trimmed, true) {
            if let Some(current) = current.as_mut()
                && !value.is_empty()
            {
                set_field(current, key, value);
            }
            continue;
        }

        if let Some(summary) = strip_legacy_bullet(trimmed) {
            if let Some(previous) = current.take() {
                entries.push(previous);
            }
            current = Some(ManagedAutoMemoryEntry {
                summary: normalize_text(summary),
                why: None,
                how_to_apply: None,
            });
            continue;
        }

        if let Some(previous) = current.take() {
            entries.push(previous);
        }
        current = Some(ManagedAutoMemoryEntry {
            summary: normalize_text(trimmed),
            why: None,
            how_to_apply: None,
        });
    }

    if let Some(current) = current {
        entries.push(current);
    }
    entries
}

pub fn render_auto_memory_body(_heading: &str, entries: &[ManagedAutoMemoryEntry]) -> String {
    if entries.is_empty() {
        return "_No entries yet._".to_owned();
    }

    let mut lines = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if index > 0 {
            lines.push(String::new());
        }
        lines.push(normalize_text(&entry.summary));
        if let Some(why) = entry.why.as_deref().filter(|value| !value.is_empty()) {
            lines.push(String::new());
            lines.push(format!("Why: {}", normalize_text(why)));
        }
        if let Some(how_to_apply) = entry
            .how_to_apply
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            lines.push(String::new());
            lines.push(format!("How to apply: {}", normalize_text(how_to_apply)));
        }
    }
    lines.join("\n")
}

pub fn merge_auto_memory_entry(
    current: &ManagedAutoMemoryEntry,
    incoming: &ManagedAutoMemoryEntry,
) -> ManagedAutoMemoryEntry {
    ManagedAutoMemoryEntry {
        summary: if incoming.summary.is_empty() {
            current.summary.clone()
        } else {
            incoming.summary.clone()
        },
        why: current.why.clone().or_else(|| incoming.why.clone()),
        how_to_apply: current
            .how_to_apply
            .clone()
            .or_else(|| incoming.how_to_apply.clone()),
    }
}

pub fn build_auto_memory_entry_search_text(entry: &ManagedAutoMemoryEntry) -> String {
    [
        Some(entry.summary.as_str()),
        entry.why.as_deref(),
        entry.how_to_apply.as_deref(),
    ]
    .into_iter()
    .flatten()
    .filter(|value| !value.is_empty())
    .collect::<Vec<_>>()
    .join(" ")
    .to_lowercase()
}

fn parse_indented_field(raw_line: &str) -> Option<(MemoryField, String)> {
    let indent_len = raw_line
        .chars()
        .take_while(|character| matches!(character, ' ' | '\t'))
        .count();
    if indent_len < 2 {
        return None;
    }
    let mut line = &raw_line[indent_len..];
    if let Some(after_marker) = line.strip_prefix('-').or_else(|| line.strip_prefix('*')) {
        let spacing = after_marker
            .chars()
            .take_while(|character| matches!(character, ' ' | '\t'))
            .count();
        if spacing > 0 {
            line = &after_marker[spacing..];
        }
    }
    parse_named_field(line, false)
}

fn parse_named_field(line: &str, bold_allowed: bool) -> Option<(MemoryField, String)> {
    let line = if bold_allowed {
        line.strip_prefix("**").unwrap_or(line)
    } else {
        line
    };

    for (name, field) in [
        ("Why", MemoryField::Why),
        ("How to apply", MemoryField::HowToApply),
        ("How_to_apply", MemoryField::HowToApply),
    ] {
        if line
            .get(..name.len())
            .is_none_or(|candidate| !candidate.eq_ignore_ascii_case(name))
        {
            continue;
        }
        let mut rest = &line[name.len()..];
        if bold_allowed {
            rest = rest.strip_prefix("**").unwrap_or(rest);
        }
        let Some(rest) = rest.strip_prefix(':') else {
            continue;
        };
        let rest = rest.trim_start_matches([' ', '\t']);
        let Some(first) = rest.chars().next() else {
            continue;
        };
        if is_js_whitespace(first) {
            continue;
        }
        return Some((field, normalize_text(rest)));
    }
    None
}

fn strip_legacy_bullet(line: &str) -> Option<&str> {
    let mut chars = line.char_indices();
    let (first_offset, first) = chars.next()?;
    if !matches!(first, '-' | '*') {
        return None;
    }
    let (_, whitespace) = chars.next()?;
    if !is_js_whitespace(whitespace) {
        return None;
    }
    let content_start = chars
        .find(|(_, character)| !is_js_whitespace(*character))
        .map(|(offset, _)| offset)
        .unwrap_or(line.len());
    debug_assert_eq!(first_offset, 0);
    Some(&line[content_start..])
}

#[derive(Clone, Copy)]
enum MemoryField {
    Why,
    HowToApply,
}

fn set_field(entry: &mut ManagedAutoMemoryEntry, field: MemoryField, value: String) {
    match field {
        MemoryField::Why => entry.why = Some(value),
        MemoryField::HowToApply => entry.how_to_apply = Some(value),
    }
}

fn normalize_text(text: &str) -> String {
    text.split(is_js_whitespace)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn trim_js_whitespace(text: &str) -> &str {
    text.trim_matches(is_js_whitespace)
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_renders_legacy_and_per_entry_formats() {
        let body = "# Feedback Memory\n\n- Use short responses when debugging\n  - Why: The user prefers brevity in debug sessions.\n  - How to apply: Keep replies to 3 sentences max.\n- Keep tests deterministic\n  Why: Nondeterminism obscures regressions.\n\nUse focused diffs\n\nWhy: Easier to review.\nHow_to_apply: Limit edits to requested files.";
        let entries = parse_auto_memory_entries(body);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].summary, "Use short responses when debugging");
        assert_eq!(
            entries[0].why.as_deref(),
            Some("The user prefers brevity in debug sessions.")
        );
        assert_eq!(
            entries[0].how_to_apply.as_deref(),
            Some("Keep replies to 3 sentences max.")
        );
        assert_eq!(entries[1].summary, "Keep tests deterministic");
        assert_eq!(
            entries[1].why.as_deref(),
            Some("Nondeterminism obscures regressions.")
        );
        assert_eq!(entries[2].summary, "Use focused diffs");
        assert_eq!(entries[2].why.as_deref(), Some("Easier to review."));
        assert_eq!(
            entries[2].how_to_apply.as_deref(),
            Some("Limit edits to requested files.")
        );
        assert_eq!(
            render_auto_memory_body("ignored", &entries),
            "Use short responses when debugging\n\nWhy: The user prefers brevity in debug sessions.\n\nHow to apply: Keep replies to 3 sentences max.\n\nKeep tests deterministic\n\nWhy: Nondeterminism obscures regressions.\n\nUse focused diffs\n\nWhy: Easier to review.\n\nHow to apply: Limit edits to requested files."
        );
        assert_eq!(render_auto_memory_body("# User", &[]), "_No entries yet._");
    }

    #[test]
    fn matches_js_whitespace_heading_markers_and_field_precedence() {
        let entries = parse_auto_memory_entries(
            "\u{feff}# Heading\u{feff}\n\n-\u{00a0}  Summary\u{2003}with spaces\n\t  **WHY**: first\nWhy: replacement\n  How to apply: \tworks\n\n_No entries yet._",
        );
        assert_eq!(
            get_auto_memory_body_heading("\u{feff} intro\n\u{00a0}# Heading  \n"),
            "# Heading"
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].summary, "Summary with spaces");
        assert_eq!(entries[0].why.as_deref(), Some("replacement"));
        assert_eq!(entries[0].how_to_apply.as_deref(), Some("works"));
    }

    #[test]
    fn merges_using_js_truthy_and_nullish_precedence_and_builds_search_text() {
        let current = ManagedAutoMemoryEntry {
            summary: "old".into(),
            why: Some(String::new()),
            how_to_apply: None,
        };
        let incoming = ManagedAutoMemoryEntry {
            summary: String::new(),
            why: Some("new why".into()),
            how_to_apply: Some("Use it".into()),
        };
        let merged = merge_auto_memory_entry(&current, &incoming);
        assert_eq!(merged.summary, "old");
        assert_eq!(merged.why.as_deref(), Some(""));
        assert_eq!(merged.how_to_apply.as_deref(), Some("Use it"));
        assert_eq!(build_auto_memory_entry_search_text(&merged), "old use it");
        assert_eq!(
            render_auto_memory_body("", &[merged]),
            "old\n\nHow to apply: Use it"
        );
    }
}
