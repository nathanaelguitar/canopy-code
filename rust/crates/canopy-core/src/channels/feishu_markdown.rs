//! Feishu markdown helpers for interactive cards and bounded message chunks.
//!
//! Port of `packages/channels/feishu/src/markdown.ts`. String limits follow
//! JavaScript UTF-16 code-unit counts; splits stay on valid Rust UTF-8 scalar
//! boundaries when a supplementary character would otherwise be divided.

use serde_json::{Value, json};

const CHUNK_LIMIT: usize = 4_000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuildCardOptions {
    pub title: Option<String>,
    pub show_stop_button: bool,
    pub is_streaming: bool,
    pub status_label: Option<String>,
    pub collapsible: bool,
    pub collapsible_threshold: Option<usize>,
}

/// Build a Feishu interactive card JSON structure with markdown content.
///
/// Final non-streaming output is split around tables. Long terminal output may
/// use a collapsible panel, and streaming output stays in one markdown element.
pub fn build_card_content(markdown: &str, options: BuildCardOptions) -> Value {
    let mut elements = Vec::<Value>::new();
    let status_label = options
        .status_label
        .as_deref()
        .or_else(|| options.is_streaming.then_some("生成中..."));
    let content_markdown = if status_label.is_some_and(|label| !label.is_empty()) {
        format!("{markdown}\n\n---\n*{}*", status_label.unwrap_or_default())
    } else {
        markdown.to_owned()
    };
    let threshold = options
        .collapsible_threshold
        .filter(|threshold| *threshold != 0)
        .unwrap_or(500);

    if options.collapsible && !options.is_streaming && js_len(markdown) > threshold {
        let raw_split = js_index_of_char(markdown, '\n', 200)
            .filter(|index| *index > 0)
            .unwrap_or(200);
        let safe_split = js_last_index_of_char(markdown, ' ', raw_split);
        let mut split_at = safe_split.filter(|index| *index > 100).unwrap_or(raw_split);

        let preview_candidate = js_slice(markdown, 0, split_at);
        let fence_count = preview_candidate
            .split('\n')
            .filter(|line| count_fences(line) % 2 == 1)
            .count();
        if fence_count % 2 == 1 {
            let fence_start = js_index_of_str(markdown, "\n```", split_at);
            if fence_start.is_some_and(|index| index > 0 && index < raw_split + 500) {
                let fence_start = fence_start.unwrap_or_default();
                let fence_line_end = js_index_of_char(markdown, '\n', fence_start + 1);
                split_at = fence_line_end
                    .filter(|index| *index > 0)
                    .unwrap_or(fence_start + 4);
            }
            // If no nearby close fence exists, the source accepts this split.
        }

        let preview = js_slice(markdown, 0, split_at);
        let rest = js_slice_from(markdown, split_at);
        let preview_content = if status_label.is_some_and(|label| !label.is_empty()) {
            format!("{preview}\n\n---\n*{}*", status_label.unwrap_or_default())
        } else {
            preview
        };
        elements.push(json!({ "tag": "markdown", "content": preview_content }));
        elements.push(json!({
            "tag": "collapsible_panel",
            "expanded": false,
            "background_color": "default",
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": "查看更多"
                }
            },
            "elements": [{ "tag": "markdown", "content": rest }]
        }));
    } else if options.is_streaming {
        elements.push(json!({ "tag": "markdown", "content": content_markdown }));
    } else {
        for segment in split_by_tables(&content_markdown) {
            elements.push(json!({ "tag": "markdown", "content": segment }));
        }
    }

    if options.show_stop_button {
        elements.push(json!({
            "tag": "button",
            "text": { "tag": "plain_text", "content": "停止" },
            "type": "danger",
            "value": { "action": "stop" }
        }));
    }

    let mut card = json!({
        "schema": "2.0",
        "config": {
            "wide_screen_mode": true,
            "summary": { "content": js_slice(markdown, 0, 3_500) }
        },
        "body": { "elements": elements }
    });
    if let Some(title) = options.title.as_deref().filter(|title| !title.is_empty()) {
        card["header"] = json!({
            "title": {
                "tag": "plain_text",
                "content": if options.is_streaming {
                    format!("{title} ...")
                } else {
                    title.to_owned()
                }
            },
            "template": if options.is_streaming { "blue" } else { "green" }
        });
    }
    card
}

/// Extract a short title from the first markdown line.
pub fn extract_title(text: &str) -> String {
    let first_line = text.split('\n').next().unwrap_or_default();
    let cleaned = first_line.trim_start_matches(is_js_title_prefix_marker);
    let title = js_slice(cleaned, 0, 20);
    if title.is_empty() {
        "Qwen Code".to_owned()
    } else {
        title
    }
}

/// Split long text into chunks that fit within Feishu's message size limit.
/// Code fences are closed and reopened when a chunk boundary crosses a block.
pub fn split_chunks(text: &str) -> Vec<String> {
    if text.is_empty() || js_len(text) <= CHUNK_LIMIT {
        return vec![text.to_owned()];
    }

    let mut chunks = Vec::new();
    let mut buffer = String::new();
    let mut in_code = false;
    let mut fence_line = "```".to_owned();

    for line in text.split('\n') {
        let fence_count = count_fences(line);
        let will_be_in_code = if fence_count % 2 == 1 {
            !in_code
        } else {
            in_code
        };

        // Account for the state after adding this line, not the prior state.
        let reserve = if will_be_in_code {
            js_len(&fence_line) + 1
        } else {
            0
        };
        if js_len(&buffer) + js_len(line) + 1 + reserve > CHUNK_LIMIT && !buffer.is_empty() {
            if in_code {
                buffer.push_str("\n```");
            }
            chunks.push(std::mem::take(&mut buffer));
            if in_code {
                buffer = fence_line.clone();
            }
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(line);

        // Hard-split lines that exceed the available chunk budget. In code,
        // reserve three units for the closing fence and one for its newline.
        let budget = if will_be_in_code {
            CHUNK_LIMIT - "\n```".len()
        } else {
            CHUNK_LIMIT
        };
        while js_len(&buffer) > budget {
            let max_slice = if in_code {
                CHUNK_LIMIT - "\n```".len() - 1
            } else {
                CHUNK_LIMIT
            };
            let (mut piece, remaining) = split_utf16_prefix(&buffer, max_slice);
            buffer = remaining;
            if in_code {
                piece.push_str("\n```");
                buffer = format!("{fence_line}\n{buffer}");
            }
            chunks.push(piece);
        }

        if fence_count % 2 == 1 {
            if !in_code {
                fence_line = trim_js(line).to_owned();
            }
            in_code = !in_code;
        }
    }

    if !buffer.is_empty() {
        chunks.push(buffer);
    }
    chunks
}

fn split_by_tables(text: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    let mut in_table = false;
    let mut in_code = false;

    for line in text.split('\n') {
        if count_fences(line) % 2 == 1 {
            in_code = !in_code;
            current.push(line);
            continue;
        }
        if in_code {
            current.push(line);
            continue;
        }

        let trimmed = trim_js(line);
        let is_table_line = trimmed.starts_with('|') && trimmed.ends_with('|');
        if is_table_line && !in_table {
            if !current.is_empty() && current.iter().any(|line| !trim_js(line).is_empty()) {
                segments.push(current.join("\n"));
                current.clear();
            }
            in_table = true;
            current.push(line);
        } else if !is_table_line && in_table {
            in_table = false;
            segments.push(current.join("\n"));
            current.clear();
            current.push(line);
        } else {
            current.push(line);
        }
    }

    if !current.is_empty() {
        segments.push(current.join("\n"));
    }
    segments
        .into_iter()
        .filter(|segment| !trim_js(segment).is_empty())
        .collect()
}

fn count_fences(text: &str) -> usize {
    text.matches("```").count()
}

fn js_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// Slice by JavaScript UTF-16 offsets without constructing an invalid UTF-8
/// string. The start rounds forward and the end rounds backward if an offset
/// bisects a supplementary scalar.
fn js_slice(text: &str, start: usize, end: usize) -> String {
    let start_byte = byte_offset_after_utf16(text, start);
    let end_byte = byte_offset_before_utf16(text, end);
    if end_byte <= start_byte {
        String::new()
    } else {
        text[start_byte..end_byte].to_owned()
    }
}

fn js_slice_from(text: &str, start: usize) -> String {
    let start_byte = byte_offset_after_utf16(text, start);
    text[start_byte..].to_owned()
}

fn split_utf16_prefix(text: &str, max_units: usize) -> (String, String) {
    let end_byte = byte_offset_before_utf16(text, max_units);
    (text[..end_byte].to_owned(), text[end_byte..].to_owned())
}

fn byte_offset_before_utf16(text: &str, offset: usize) -> usize {
    let mut units = 0;
    for (byte, character) in text.char_indices() {
        let next_units = units + character.len_utf16();
        if next_units > offset {
            return byte;
        }
        units = next_units;
    }
    text.len()
}

fn byte_offset_after_utf16(text: &str, offset: usize) -> usize {
    if offset == 0 {
        return 0;
    }
    let mut units = 0;
    for (byte, character) in text.char_indices() {
        let next_units = units + character.len_utf16();
        if next_units > offset {
            return byte + character.len_utf8();
        }
        if next_units == offset {
            return byte + character.len_utf8();
        }
        units = next_units;
    }
    text.len()
}

fn js_index_of_char(text: &str, needle: char, from: usize) -> Option<usize> {
    let start_byte = byte_offset_after_utf16(text, from);
    text[start_byte..]
        .find(needle)
        .map(|relative| js_len(&text[..start_byte + relative]))
}

fn js_index_of_str(text: &str, needle: &str, from: usize) -> Option<usize> {
    let start_byte = byte_offset_after_utf16(text, from);
    text[start_byte..]
        .find(needle)
        .map(|relative| js_len(&text[..start_byte + relative]))
}

fn js_last_index_of_char(text: &str, needle: char, at_or_before: usize) -> Option<usize> {
    let mut units = 0;
    let mut found = None;
    for character in text.chars() {
        if units > at_or_before {
            break;
        }
        if character == needle {
            found = Some(units);
        }
        units += character.len_utf16();
    }
    found
}

fn is_js_title_prefix_marker(character: char) -> bool {
    character == '#'
        || character == '*'
        || character == '-'
        || character == '>'
        || is_js_whitespace(character)
}

fn is_js_whitespace(character: char) -> bool {
    character.is_whitespace() || character == '\u{feff}'
}

fn trim_js(text: &str) -> &str {
    text.trim_matches(is_js_whitespace)
}

#[cfg(test)]
mod tests {
    use super::{BuildCardOptions, build_card_content, extract_title, split_chunks};
    use serde_json::json;

    #[test]
    fn builds_default_card_and_summary() {
        let card = build_card_content("Hello world", BuildCardOptions::default());
        assert_eq!(card["schema"], "2.0");
        assert_eq!(card["body"]["elements"][0]["tag"], "markdown");
        assert_eq!(card["body"]["elements"][0]["content"], "Hello world");
        assert_eq!(card["config"]["summary"]["content"], "Hello world");
        assert!(card.get("header").is_none());
    }

    #[test]
    fn adds_default_or_custom_streaming_status_and_blue_header() {
        let default_status = build_card_content(
            "text",
            BuildCardOptions {
                title: Some("Title".to_owned()),
                is_streaming: true,
                ..BuildCardOptions::default()
            },
        );
        assert_eq!(
            default_status["body"]["elements"][0]["content"],
            "text\n\n---\n*生成中...*"
        );
        assert_eq!(default_status["header"]["title"]["content"], "Title ...");
        assert_eq!(default_status["header"]["template"], "blue");

        let custom_status = build_card_content(
            "text",
            BuildCardOptions {
                is_streaming: true,
                status_label: Some("运行中...".to_owned()),
                ..BuildCardOptions::default()
            },
        );
        assert!(
            custom_status["body"]["elements"][0]["content"]
                .as_str()
                .unwrap()
                .contains("运行中...")
        );
        assert!(
            !custom_status["body"]["elements"][0]["content"]
                .as_str()
                .unwrap()
                .contains("生成中...")
        );
    }

    #[test]
    fn appends_terminal_status_and_stop_button_without_streaming_controls() {
        let card = build_card_content(
            "text",
            BuildCardOptions {
                status_label: Some("已完成".to_owned()),
                show_stop_button: true,
                title: Some("Done".to_owned()),
                ..BuildCardOptions::default()
            },
        );
        let elements = card["body"]["elements"].as_array().unwrap();
        assert_eq!(elements[0]["content"], "text\n\n---\n*已完成*");
        assert_eq!(elements[1]["tag"], "button");
        assert_eq!(elements[1]["value"], json!({ "action": "stop" }));
        assert_eq!(card["header"]["template"], "green");
    }

    #[test]
    fn builds_collapsible_long_terminal_content_and_keeps_status_out_of_panel() {
        let long_text = "a ".repeat(320);
        let card = build_card_content(
            &long_text,
            BuildCardOptions {
                collapsible: true,
                collapsible_threshold: Some(500),
                status_label: Some("已完成".to_owned()),
                ..BuildCardOptions::default()
            },
        );
        let elements = card["body"]["elements"].as_array().unwrap();
        assert_eq!(
            elements[0]["content"],
            format!("{}a\n\n---\n*已完成*", "a ".repeat(99))
        );
        assert_eq!(elements[1]["tag"], "collapsible_panel");
        assert_eq!(
            elements[1]["elements"][0]["content"],
            format!(" {}", "a ".repeat(220))
        );
        assert!(
            !elements[1]["elements"][0]["content"]
                .as_str()
                .unwrap()
                .contains("已完成")
        );

        let short = build_card_content(
            "short",
            BuildCardOptions {
                collapsible: true,
                collapsible_threshold: Some(500),
                ..BuildCardOptions::default()
            },
        );
        assert_eq!(short["body"]["elements"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn splits_table_from_following_content_but_not_table_like_code() {
        let markdown = "Before table\n| A | B |\n| --- | --- |\n| 1 | 2 |\nAfter table";
        let card = build_card_content(markdown, BuildCardOptions::default());
        let elements = card["body"]["elements"].as_array().unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0]["content"], "Before table");
        assert_eq!(
            elements[1]["content"],
            "| A | B |\n| --- | --- |\n| 1 | 2 |"
        );
        assert_eq!(elements[2]["content"], "After table");

        let no_table =
            build_card_content("Hello\nWorld\nNo tables here", BuildCardOptions::default());
        assert_eq!(no_table["body"]["elements"].as_array().unwrap().len(), 1);

        let code_table = build_card_content(
            "```\n| A | B |\n| --- | --- |\n| 1 | 2 |\n```",
            BuildCardOptions::default(),
        );
        assert_eq!(code_table["body"]["elements"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn extract_title_strips_markers_truncates_and_defaults() {
        assert_eq!(extract_title("Hello World\nmore text"), "Hello World");
        assert_eq!(extract_title("## My Title\ncontent"), "My Title");
        assert_eq!(extract_title("* Item one"), "Item one");
        assert_eq!(extract_title("> Quote text"), "Quote text");
        assert_eq!(extract_title(""), "Qwen Code");
        assert_eq!(extract_title("###"), "Qwen Code");
        assert_eq!(extract_title(&"a".repeat(30)).encode_utf16().count(), 20);
    }

    #[test]
    fn returns_single_chunk_for_empty_and_short_text() {
        assert_eq!(split_chunks(""), [""]);
        assert_eq!(split_chunks("short text"), ["short text"]);
    }

    #[test]
    fn splits_long_lines_and_keeps_each_chunk_within_the_limit() {
        let text = format!("{}\n", "a".repeat(100)).repeat(50);
        let chunks = split_chunks(&text);
        assert!(chunks.len() > 1);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 4_000)
        );

        let chunks = split_chunks(&"a".repeat(5_000));
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].encode_utf16().count(), 4_000);
        assert_eq!(chunks[1].encode_utf16().count(), 1_000);
    }

    #[test]
    fn closes_reopens_fences_and_preserves_near_limit_code() {
        let long_code = format!("```\n{}\n```", "x\n".repeat(2_500));
        let chunks = split_chunks(&long_code);
        assert!(chunks.len() > 1);
        assert!(chunks[0].contains("```"));
        assert!(chunks[1].trim_start().starts_with("```"));
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 4_000)
        );

        let near_limit = format!("```\n{}\n```", "x".repeat(3_993));
        let chunks = split_chunks(&near_limit);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 4_000)
        );
        let recovered = chunks
            .join("\n")
            .split('\n')
            .filter(|line| !line.starts_with("```"))
            .collect::<String>();
        assert_eq!(recovered, "x".repeat(3_993));
    }

    #[test]
    fn reserves_closing_fence_space_when_an_open_fence_line_follows_text() {
        let text = format!("{}\n```js\n{}\n```", "a".repeat(3_991), "z".repeat(500));
        let chunks = split_chunks(&text);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 4_000)
        );
    }

    #[test]
    fn limits_use_javascript_utf16_units_and_do_not_split_utf8_scalars() {
        let text = format!("{}🦀", "a".repeat(3_999));
        let chunks = split_chunks(&text);
        assert_eq!(chunks[0], "a".repeat(3_999));
        assert_eq!(chunks[1], "🦀");
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.encode_utf16().count() <= 4_000)
        );

        let summary = build_card_content(
            &format!("{}🦀tail", "a".repeat(3_499)),
            BuildCardOptions::default(),
        );
        assert_eq!(
            summary["config"]["summary"]["content"].as_str().unwrap(),
            "a".repeat(3_499)
        );
    }
}
