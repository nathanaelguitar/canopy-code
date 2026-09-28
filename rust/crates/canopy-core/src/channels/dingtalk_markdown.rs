//! DingTalk Markdown message normalization.
//!
//! Port of `packages/channels/dingtalk/src/markdown.ts`. DingTalk messages are
//! split at 3,800 JavaScript string units. Open code fences are closed at a
//! chunk boundary and reopened at the start of the next chunk.
//!
//! JavaScript measures strings and slices in UTF-16 code units. This module
//! uses the same measurement for limits and titles, but keeps Rust strings
//! valid UTF-8: if a limit would bisect a supplementary Unicode character, it
//! rounds the split back to the preceding scalar boundary.

const CHUNK_LIMIT: usize = 3_800;

/// Splits text into DingTalk-sized chunks, closing and reopening code fences
/// when an active fenced block crosses a boundary.
pub fn split_chunks(text: &str) -> Vec<String> {
    if text.is_empty() || utf16_len(text) <= CHUNK_LIMIT {
        return vec![text.to_owned()];
    }

    let mut chunks = Vec::new();
    let mut buffer = String::new();
    let mut in_code = false;

    let lines: Vec<&str> = text.split('\n').collect();
    for (line_index, line) in lines.iter().enumerate() {
        let fence_count = count_fences(line);
        let toggles_code_fence = fence_count % 2 == 1;
        append_line(
            line,
            line_index > 0,
            in_code && toggles_code_fence,
            in_code != toggles_code_fence,
            in_code,
            &mut buffer,
            &mut chunks,
        );

        if toggles_code_fence {
            in_code = !in_code;
        }
    }

    if !buffer.is_empty() {
        chunks.push(buffer);
    }
    chunks
}

/// Extracts a short title from the first Markdown line for a webhook payload.
pub fn extract_title(text: &str) -> String {
    let first_line = text.split('\n').next().unwrap_or_default();
    let prefix_bytes = first_line
        .char_indices()
        .take_while(|(_, ch)| is_title_prefix(*ch))
        .map(|(byte_index, ch)| byte_index + ch.len_utf8())
        .last()
        .unwrap_or(0);
    let title = &first_line[prefix_bytes..];
    let title_end = byte_index_for_utf16(title, 20);
    let title = &title[..title_end];

    if title.is_empty() {
        "Reply".to_owned()
    } else {
        title.to_owned()
    }
}

/// DingTalk's Markdown renderer does not need further rewriting today; this
/// function provides the stable normalization entry point and chunking rules.
pub fn normalize_dingtalk_markdown(text: &str) -> Vec<String> {
    split_chunks(text)
}

fn append_line(
    line: &str,
    needs_line_break: bool,
    closes_code_fence: bool,
    leaves_code_fence_open: bool,
    in_code: bool,
    buffer: &mut String,
    chunks: &mut Vec<String>,
) {
    let mut remaining = line;
    let mut prefix_pending = needs_line_break;
    let mut line_opened_fence_in_buffer = false;

    while !remaining.is_empty() || prefix_pending {
        let prefix = if prefix_pending { "\n" } else { "" };
        let prefix_len = utf16_len(prefix);
        let remaining_len = utf16_len(remaining);
        let final_capacity =
            CHUNK_LIMIT as isize - utf16_len(buffer) as isize - prefix_len as isize;
        let fits_as_final_piece = remaining_len as isize <= final_capacity;
        let close_fence_overhead = if (in_code && !(closes_code_fence && fits_as_final_piece))
            || (!in_code && leaves_code_fence_open)
        {
            utf16_len("\n```")
        } else {
            0
        };
        let occupied = utf16_len(buffer) + prefix_len;
        let available = CHUNK_LIMIT.saturating_sub(close_fence_overhead + occupied);

        if available == 0 {
            flush(buffer, chunks, in_code || line_opened_fence_in_buffer);
            continue;
        }

        let mut piece_len = available.min(remaining_len);
        if piece_len < remaining_len {
            let first_fence_start = piece_len.saturating_sub(2);
            for fence_start in first_fence_start..piece_len {
                if utf16_slice(remaining, fence_start, 3) == Some("```")
                    && piece_len < fence_start + 3
                {
                    piece_len = fence_start;
                    break;
                }
            }
        }

        let piece_end = byte_index_for_utf16(remaining, piece_len);
        // A one-unit budget can land in the middle of a supplementary
        // character. Keep the Rust string valid and let the next iteration
        // flush this buffer to make enough room for the full scalar.
        if piece_end == 0 && !remaining.is_empty() {
            flush(buffer, chunks, in_code || line_opened_fence_in_buffer);
            continue;
        }

        let piece = &remaining[..piece_end];
        let appended_text = format!("{prefix}{piece}");
        buffer.push_str(&appended_text);
        remaining = &remaining[piece_end..];
        prefix_pending = false;
        line_opened_fence_in_buffer |=
            !in_code && leaves_code_fence_open && appended_text.contains("```");

        if !remaining.is_empty() {
            let keep_code_open = in_code || line_opened_fence_in_buffer;
            flush(buffer, chunks, keep_code_open);
            prefix_pending = keep_code_open;
        }
    }
}

fn flush(buffer: &mut String, chunks: &mut Vec<String>, keep_code_open: bool) {
    if keep_code_open {
        buffer.push_str("\n```");
    }
    chunks.push(std::mem::take(buffer));
    if keep_code_open {
        buffer.push_str("```");
    }
}

fn count_fences(line: &str) -> usize {
    let bytes = line.as_bytes();
    let mut count = 0;
    let mut index = 0;
    while index + 3 <= bytes.len() {
        if &bytes[index..index + 3] == b"```" {
            count += 1;
            index += 3;
        } else {
            index += 1;
        }
    }
    count
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// Returns a UTF-8 byte boundary at or before the requested UTF-16 offset.
fn byte_index_for_utf16(text: &str, units: usize) -> usize {
    let mut consumed = 0;
    for (byte_index, ch) in text.char_indices() {
        let char_units = ch.len_utf16();
        if consumed + char_units > units {
            return byte_index;
        }
        consumed += char_units;
        if consumed == units {
            return byte_index + ch.len_utf8();
        }
    }
    text.len()
}

/// Returns an exact UTF-16 slice only when both endpoints are Rust scalar
/// boundaries. The only caller looks for ASCII backtick runs.
fn utf16_slice(text: &str, start: usize, length: usize) -> Option<&str> {
    let start_byte = byte_index_for_utf16(text, start);
    let end_byte = byte_index_for_utf16(text, start + length);
    if utf16_len(&text[..start_byte]) != start || utf16_len(&text[..end_byte]) != start + length {
        return None;
    }
    text.get(start_byte..end_byte)
}

fn is_title_prefix(ch: char) -> bool {
    matches!(ch, '#' | '*' | '-' | '>')
        || matches!(
            ch,
            '\u{0009}'..='\u{000d}'
                | '\u{0020}'
                | '\u{00a0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200a}'
                | '\u{2028}'..='\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
        )
}

#[cfg(test)]
mod tests {
    use super::{CHUNK_LIMIT, extract_title, normalize_dingtalk_markdown, split_chunks, utf16_len};

    fn assert_chunk_limits(chunks: &[String]) {
        assert!(chunks.iter().all(|chunk| utf16_len(chunk) <= CHUNK_LIMIT));
    }

    #[test]
    fn returns_one_chunk_for_short_and_empty_text() {
        assert_eq!(split_chunks("short text"), ["short text"]);
        assert_eq!(split_chunks(""), [""]);
    }

    #[test]
    fn splits_multiple_lines_within_limit() {
        let line = format!("{}\n", "a".repeat(100));
        let text = line.repeat(50);
        let chunks = split_chunks(&text);
        assert!(chunks.len() > 1);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn splits_a_single_long_line_and_preserves_text() {
        let text = "a".repeat(5_000);
        let chunks = split_chunks(&text);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks.concat(), text);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn preserves_newlines_around_a_long_line() {
        let text = format!("before\n{}\nafter", "b".repeat(5_000));
        let chunks = split_chunks(&text);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn preserves_newline_before_a_long_line() {
        let text = format!("{}\n{}", "a".repeat(3_799), "b".repeat(5_000));
        let chunks = split_chunks(&text);
        assert_eq!(chunks.concat(), text);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn flushes_an_exact_limit_line_before_a_following_newline() {
        let text = format!("{}\n", "a".repeat(CHUNK_LIMIT));
        let chunks = split_chunks(&text);
        assert_eq!(chunks.concat(), text);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn inline_backticks_do_not_open_code_blocks() {
        let text = format!("before ``` inline ``` {}", "x".repeat(5_000));
        let chunks = split_chunks(&text);
        assert_eq!(chunks.concat(), text);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn closes_and_reopens_a_code_fence_across_chunks() {
        let long_code = format!("```\n{}", "x\n".repeat(2_000)) + "```";
        let chunks = split_chunks(&long_code);
        assert!(chunks.len() > 1);
        assert!(chunks[0].contains("```"));
        assert!(chunks[1].trim_start().starts_with("```"));
        assert!(!chunks[1].starts_with("```\n\n"));
        assert!(chunks[1].starts_with("```\nx"));
    }

    #[test]
    fn splits_a_long_code_line_and_preserves_fences() {
        let long_code = format!("```\n{}\n```", "x".repeat(5_000));
        let chunks = split_chunks(&long_code);
        assert!(chunks.len() > 1);
        assert!(chunks[0].ends_with("\n```"));
        assert!(chunks[1].starts_with("```\nx"));
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn reserves_room_for_closing_fence_overhead() {
        let long_code = format!("```\n{}\n```", "x".repeat(3_793));
        let chunks = split_chunks(&long_code);
        assert!(chunks.len() > 1);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn keeps_limit_when_a_long_code_line_ends_with_a_fence() {
        let long_code = format!("```\n{}{}", "x".repeat(5_000), "```");
        let chunks = split_chunks(&long_code);
        assert!(chunks.len() > 1);
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn reserves_room_after_a_long_opening_fence_line() {
        let long_code = format!("```{}\ny\n```", "x".repeat(3_797));
        let chunks = split_chunks(&long_code);
        assert!(chunks.len() > 1);
        assert!(chunks[0].ends_with("\n```"));
        assert!(chunks[1].starts_with("```\n"));
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn does_not_split_a_fence_delimiter_between_chunks() {
        let long_code = format!("{}\n```{}\ny\n```", "a".repeat(3_794), "x".repeat(100));
        let chunks = split_chunks(&long_code);
        assert_eq!(chunks.concat(), long_code);
        assert!(!chunks[0].ends_with("\n`"));
        assert!(chunks[1].starts_with("\n```"));
        assert_chunk_limits(&chunks);
    }

    #[test]
    fn extracts_first_line_and_strips_heading_bold_list_and_quote_markers() {
        assert_eq!(extract_title("Hello World\nmore text"), "Hello World");
        assert_eq!(extract_title("## My Title\ncontent"), "My Title");
        assert_eq!(extract_title("* Item one"), "Item one");
        assert_eq!(extract_title("> Quote text"), "Quote text");
        assert_eq!(extract_title("- bullet"), "bullet");
    }

    #[test]
    fn truncates_title_at_twenty_utf16_units() {
        assert_eq!(
            utf16_len(&extract_title(
                "This is a very long title that should be truncated"
            )),
            20
        );
        assert_eq!(
            extract_title(&format!("{}x", "😀".repeat(11))),
            "😀".repeat(10)
        );
    }

    #[test]
    fn uses_reply_when_title_is_empty() {
        assert_eq!(extract_title(""), "Reply");
        assert_eq!(extract_title("###"), "Reply");
    }

    #[test]
    fn normalization_preserves_tables_and_short_plain_text() {
        let input = ["| A | B |", "| --- | --- |", "| 1 | 2 |"].join("\n");
        assert_eq!(normalize_dingtalk_markdown(&input), [input]);
        assert_eq!(normalize_dingtalk_markdown("simple text"), ["simple text"]);
    }

    #[test]
    fn long_supplementary_text_uses_utf16_budget_without_splitting_scalars() {
        let text = "😀".repeat(2_000);
        let chunks = split_chunks(&text);
        assert_eq!(chunks.concat(), text);
        assert_chunk_limits(&chunks);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.chars().all(|ch| ch == '😀'))
        );
    }
}
