//! Bounded UTF-8-aware line and byte-cursor reads for text files.
//!
//! This mirrors `packages/core/src/utils/read-text-range.ts`. Path reads use a
//! captured file-size snapshot; handle reads borrow the caller's descriptor and
//! issue positional reads, leaving its file cursor untouched.

use std::fs::File;
use std::io;
use std::path::Path;

use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::{Encoding, UTF_8};
use thiserror::Error;

use crate::utils::cancellation::{CancellationReason, CancellationToken};
use crate::utils::encoding::is_utf8_compatible_encoding;

pub const DEFAULT_RANGE_READ_BYTES: usize = 25_000;
pub const TEXT_RANGE_FAST_PATH_MAX_SIZE: u64 = 10 * 1024 * 1024;
pub const RANGE_READ_CHUNK_BYTES: usize = 512 * 1024;
const ENCODING_SAMPLE_BYTES: usize = 8 * 1024;
const UTF8_BOM: &[u8] = &[0xef, 0xbb, 0xbf];

/// Positional read interface used to keep handle-bound logic testable and
/// independent of a descriptor's mutable seek cursor.
pub trait ReadAt {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize>;
}

impl ReadAt for File {
    #[cfg(unix)]
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        FileExt::read_at(self, buffer, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::fs::FileExt;
        FileExt::seek_read(self, buffer, offset)
    }

    #[cfg(not(any(unix, windows)))]
    fn read_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "positional file reads are unsupported on this platform",
        ))
    }
}

#[derive(Clone)]
pub struct ReadTextRangeRequest<'a> {
    pub path: &'a Path,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub max_output_bytes: usize,
    /// `None` means unbounded, matching the source helper's default infinity.
    pub max_scan_bytes: Option<u64>,
    pub cancellation: Option<&'a CancellationToken>,
}

#[derive(Clone, Copy)]
pub struct ReadTextRangeFromHandleRequest<'a> {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    /// File length captured by the caller before invoking this function.
    pub file_size: u64,
    pub max_output_bytes: usize,
    pub max_scan_bytes: u64,
    pub cancellation: Option<&'a CancellationToken>,
}

#[derive(Clone, Copy)]
pub struct ReadTextCursorWindowRequest<'a> {
    pub start_offset: u64,
    /// File length captured by the caller before invoking this function.
    pub file_size: u64,
    pub limit: Option<usize>,
    pub max_output_bytes: usize,
    pub max_snap_bytes: u64,
    pub cancellation: Option<&'a CancellationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadTextRangeResult {
    pub content: String,
    pub original_line_count: usize,
    /// Byte offset just past the last line passed; absent at EOF.
    pub next_byte_offset: Option<u64>,
    pub encoding: String,
    pub bom: bool,
    pub line_ending: LineEnding,
    pub original_line_count_exact: bool,
    pub truncated_by_bytes: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadTextCursorWindowResult {
    pub content: String,
    pub start_offset: u64,
    pub next_offset: Option<u64>,
    pub encoding: String,
    pub bom: bool,
    pub line_ending: LineEnding,
    pub truncated_by_bytes: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineEnding {
    CrLf,
    Lf,
}

impl std::fmt::Display for LineEnding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::CrLf => "crlf",
            Self::Lf => "lf",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidUtf8Reason {
    InvalidUtf8,
}

#[derive(Debug, Error)]
pub enum ReadTextRangeError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{message}")]
    Cancelled { message: String },
    #[error("{message}")]
    CursorNotAtLineBoundary {
        start_offset: u64,
        max_snap_bytes: u64,
        message: String,
    },
    #[error("{message}")]
    LargeNonUtf8Text {
        encoding: String,
        reason: Option<InvalidUtf8Reason>,
        message: String,
    },
    #[error("{message}")]
    TextScanBudgetExceeded {
        scanned_bytes: u64,
        max_scan_bytes: u64,
        message: String,
    },
}

impl ReadTextRangeError {
    fn cursor_boundary(start_offset: u64, max_snap_bytes: u64) -> Self {
        Self::CursorNotAtLineBoundary {
            start_offset,
            max_snap_bytes,
            message: format!(
                "Byte offset {start_offset} is not the start of a line, and no line break was found within {max_snap_bytes} bytes after it. Resume from a cursor this reader returned."
            ),
        }
    }

    fn non_utf8(encoding: impl Into<String>, reason: Option<InvalidUtf8Reason>) -> Self {
        let encoding = encoding.into();
        let message = if reason == Some(InvalidUtf8Reason::InvalidUtf8) {
            "Large text file contains invalid UTF-8 byte sequence beyond the initial encoding sample. Convert or extract a smaller UTF-8 slice and read that instead.".to_owned()
        } else {
            format!(
                "Large non-UTF-8 text files are not supported for streaming reads (detected {encoding}). Convert or extract a smaller UTF-8 slice and read that instead."
            )
        };
        Self::LargeNonUtf8Text {
            encoding,
            reason,
            message,
        }
    }

    fn scan_budget(scanned_bytes: u64, max_scan_bytes: u64) -> Self {
        Self::TextScanBudgetExceeded {
            scanned_bytes,
            max_scan_bytes,
            message: format!(
                "Locating the requested line window would read more than {max_scan_bytes} bytes (line offsets are resolved by scanning from the start of the file). Use a byte-offset read to reach this part of the file."
            ),
        }
    }
}

fn check_cancelled(token: Option<&CancellationToken>) -> Result<(), ReadTextRangeError> {
    let Some(token) = token.filter(|token| token.is_cancelled()) else {
        return Ok(());
    };
    let message = match token.reason() {
        Some(CancellationReason::Explicit(reason)) => reason.to_string(),
        Some(CancellationReason::Timeout) => "operation timed out".to_owned(),
        None => "operation aborted".to_owned(),
    };
    Err(ReadTextRangeError::Cancelled { message })
}

/// Read a line-numbered range from a path. Small files use a bounded whole-file
/// path; larger files stream from byte zero so line offsets have a scan budget.
pub fn read_text_range(
    request: ReadTextRangeRequest<'_>,
) -> Result<ReadTextRangeResult, ReadTextRangeError> {
    check_cancelled(request.cancellation)?;
    let file = File::open(request.path)?;
    let file_size = file.metadata()?.len();
    let max_scan_bytes = request.max_scan_bytes.unwrap_or(u64::MAX);

    if file_size < TEXT_RANGE_FAST_PATH_MAX_SIZE && file_size <= max_scan_bytes {
        let raw = read_snapshot(&file, file_size, request.cancellation)?;
        check_cancelled(request.cancellation)?;
        let (content, encoding, bom) = decode_complete_text(&raw);
        let range = slice_decoded_content(
            &content,
            request.offset,
            request.limit,
            request.max_output_bytes,
        );
        return Ok(ReadTextRangeResult {
            content: range.content,
            original_line_count: range.original_line_count,
            next_byte_offset: None,
            encoding,
            bom,
            line_ending: detect_line_ending_from_content(&content),
            original_line_count_exact: true,
            truncated_by_bytes: range.truncated,
        });
    }

    read_large_utf8_range(
        &file,
        request.offset,
        request.limit,
        request.max_output_bytes,
        max_scan_bytes,
        file_size,
        request.cancellation,
    )
}

/// Read a line-numbered range from a borrowed descriptor and a caller-captured
/// size. Reads use explicit byte offsets and never close or seek the handle.
pub fn read_text_range_from_handle<H: ReadAt + ?Sized>(
    file_handle: &H,
    request: ReadTextRangeFromHandleRequest<'_>,
) -> Result<ReadTextRangeResult, ReadTextRangeError> {
    check_cancelled(request.cancellation)?;
    read_large_utf8_range(
        file_handle,
        request.offset,
        request.limit,
        request.max_output_bytes,
        request.max_scan_bytes,
        request.file_size,
        request.cancellation,
    )
}

/// Read complete lines beginning from a byte cursor. A returned cursor always
/// points at a line start, and absolute offsets include any UTF-8 BOM bytes.
pub fn read_text_cursor_window_from_handle<H: ReadAt + ?Sized>(
    file_handle: &H,
    request: ReadTextCursorWindowRequest<'_>,
) -> Result<ReadTextCursorWindowResult, ReadTextRangeError> {
    check_cancelled(request.cancellation)?;
    let encoding = detect_file_encoding(file_handle, request.file_size)?;
    check_cancelled(request.cancellation)?;
    if !is_utf8_compatible_encoding(&encoding) {
        return Err(ReadTextRangeError::non_utf8(encoding, None));
    }

    let bom = has_utf8_bom(file_handle, request.file_size)?;
    let start_offset = snap_to_line_start(
        file_handle,
        request.start_offset,
        request.file_size,
        request.max_snap_bytes,
        request.cancellation,
        false,
    )?;
    if start_offset >= request.file_size {
        return Ok(ReadTextCursorWindowResult {
            content: String::new(),
            start_offset,
            next_offset: None,
            encoding: "utf-8".to_owned(),
            bom,
            line_ending: LineEnding::Lf,
            truncated_by_bytes: false,
        });
    }

    let mut decoder = Utf8StreamDecoder::default();
    let mut output = CursorWindowOutput {
        saw_crlf: preceded_by_crlf_terminator(file_handle, start_offset)?,
        ..CursorWindowOutput::default()
    };
    let mut pending = String::new();
    let mut first_chunk = true;
    let mut reached_eof = true;

    for_each_chunk(
        file_handle,
        start_offset,
        request.file_size,
        request.cancellation,
        |raw| {
            check_cancelled(request.cancellation)?;
            let mut text = decoder.decode(raw, false, "utf-8")?;
            if first_chunk {
                first_chunk = false;
                if start_offset == 0 && text.starts_with('\u{feff}') {
                    text.remove(0);
                    output.consumed_bytes = output.consumed_bytes.saturating_add(3);
                }
            }
            pending.push_str(&text);

            let newline = pending.find('\n');
            let pending_line_len = newline.unwrap_or(pending.len());
            let separator = usize::from(!output.lines.is_empty());
            if output
                .content_bytes
                .saturating_add(separator)
                .saturating_add(pending_line_len)
                > request.max_output_bytes
            {
                if output.lines.is_empty() {
                    output.emit_line(
                        &pending[..pending_line_len],
                        newline.is_some(),
                        request.limit,
                        request.max_output_bytes,
                    );
                } else {
                    output.stop = true;
                }
                reached_eof = false;
                return Ok(false);
            }

            let mut from = 0usize;
            let mut newline = newline;
            while let Some(index) = newline {
                output.emit_line(
                    &pending[from..index],
                    true,
                    request.limit,
                    request.max_output_bytes,
                );
                from = index + 1;
                if output.stop {
                    break;
                }
                newline = pending[from..].find('\n').map(|relative| from + relative);
            }
            if from > 0 {
                pending.drain(..from);
            }
            if output.stop {
                reached_eof = false;
                return Ok(false);
            }
            Ok(true)
        },
    )?;

    if reached_eof {
        let tail = decoder.decode(&[], true, "utf-8")?;
        pending.push_str(&tail);
        if !output.stop {
            output.emit_line(&pending, false, request.limit, request.max_output_bytes);
        }
    }

    if output.skip_rest_of_line {
        let resume_at = snap_to_line_start(
            file_handle,
            start_offset.saturating_add(output.consumed_bytes.max(1)),
            request.file_size,
            request.file_size,
            request.cancellation,
            true,
        )?;
        output.consumed_bytes = resume_at.saturating_sub(start_offset);
        if !output.saw_crlf {
            output.saw_crlf = preceded_by_crlf_terminator(file_handle, resume_at)?;
        }
    }

    let content = output.lines.join("\n");
    let next_offset = start_offset.saturating_add(output.consumed_bytes);
    Ok(ReadTextCursorWindowResult {
        content,
        start_offset,
        next_offset: (next_offset < request.file_size).then_some(next_offset),
        encoding: "utf-8".to_owned(),
        bom,
        line_ending: if output.saw_crlf {
            LineEnding::CrLf
        } else {
            LineEnding::Lf
        },
        truncated_by_bytes: output.truncated_by_bytes,
    })
}

struct SliceRange {
    content: String,
    original_line_count: usize,
    truncated: bool,
}

#[derive(Default)]
struct CursorWindowOutput {
    lines: Vec<String>,
    content_bytes: usize,
    consumed_bytes: u64,
    truncated_by_bytes: bool,
    stop: bool,
    skip_rest_of_line: bool,
    saw_crlf: bool,
}

impl CursorWindowOutput {
    fn emit_line(
        &mut self,
        line: &str,
        had_newline: bool,
        limit: Option<usize>,
        max_output_bytes: usize,
    ) {
        let separator = usize::from(!self.lines.is_empty());
        let line_bytes = line.len();
        if self
            .content_bytes
            .saturating_add(separator)
            .saturating_add(line_bytes)
            > max_output_bytes
        {
            if !self.lines.is_empty() {
                self.stop = true;
                return;
            }
            let cut = truncate_utf8(line, max_output_bytes);
            self.lines.push(cut.content.clone());
            if had_newline && line.ends_with('\r') {
                self.saw_crlf = true;
            }
            self.content_bytes = cut.content.len();
            self.consumed_bytes = self.consumed_bytes.saturating_add(cut.content.len() as u64);
            self.truncated_by_bytes = true;
            self.skip_rest_of_line = true;
            self.stop = true;
            return;
        }

        self.lines.push(line.to_owned());
        if had_newline && line.ends_with('\r') {
            self.saw_crlf = true;
        }
        self.content_bytes = self
            .content_bytes
            .saturating_add(separator)
            .saturating_add(line_bytes);
        self.consumed_bytes = self
            .consumed_bytes
            .saturating_add(line_bytes as u64)
            .saturating_add(u64::from(had_newline));
        if limit.is_some_and(|limit| self.lines.len() >= limit) {
            self.stop = true;
        }
    }
}

fn slice_decoded_content(
    content: &str,
    offset: Option<usize>,
    limit: Option<usize>,
    max_output_bytes: usize,
) -> SliceRange {
    let lines = content.split('\n').collect::<Vec<_>>();
    let line_count = lines.len();
    let start = offset.unwrap_or(0).min(line_count);
    let end = limit.map_or(line_count, |limit| {
        start.saturating_add(limit).min(line_count)
    });
    let selected = lines[start..end].join("\n");
    let truncated = truncate_utf8(&selected, max_output_bytes);
    SliceRange {
        content: truncated.content,
        original_line_count: line_count,
        truncated: truncated.truncated,
    }
}

fn append_selected_fragment(
    fragment: &str,
    max_output_bytes: usize,
    output: &mut String,
    output_bytes: &mut usize,
    truncated_by_bytes: &mut bool,
) {
    if fragment.is_empty() || *truncated_by_bytes {
        return;
    }
    let available = max_output_bytes.saturating_sub(*output_bytes);
    if available == 0 {
        *truncated_by_bytes = true;
        return;
    }
    let cut = truncate_utf8(fragment, available);
    output.push_str(&cut.content);
    *output_bytes = output_bytes.saturating_add(cut.content.len());
    if cut.truncated {
        *truncated_by_bytes = true;
    }
}

fn read_large_utf8_range<H: ReadAt + ?Sized>(
    file_handle: &H,
    offset: Option<usize>,
    limit: Option<usize>,
    max_output_bytes: usize,
    max_scan_bytes: u64,
    file_size: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<ReadTextRangeResult, ReadTextRangeError> {
    let encoding = detect_file_encoding(file_handle, file_size)?;
    check_cancelled(cancellation)?;
    if !is_utf8_compatible_encoding(&encoding) {
        return Err(ReadTextRangeError::non_utf8(encoding, None));
    }

    let offset = offset.unwrap_or(0);
    let end_line = limit.map_or(usize::MAX, |limit| offset.saturating_add(limit));
    let source_end = file_size.min(max_scan_bytes);
    if source_end == 0 && file_size > 0 {
        return Err(ReadTextRangeError::scan_budget(0, max_scan_bytes));
    }

    let mut current_line = 0usize;
    let mut output = String::new();
    let mut output_bytes = 0usize;
    let mut truncated_by_bytes = false;
    let mut bom = false;
    let mut first_chunk = true;
    let mut line_ending = LineEnding::Lf;
    let mut previous_chunk_ended_with_cr = false;
    let mut original_line_count_exact = true;
    let mut stopped_early = false;
    let mut scanned_bytes = 0u64;
    let mut consumed_bytes = 0u64;
    let mut decoder = Utf8StreamDecoder::default();

    let mut position = 0u64;
    let mut buffer = vec![0u8; RANGE_READ_CHUNK_BYTES];
    while position < source_end {
        check_cancelled(cancellation)?;
        let wanted = usize::try_from((source_end - position).min(RANGE_READ_CHUNK_BYTES as u64))
            .unwrap_or(RANGE_READ_CHUNK_BYTES);
        let bytes_read = file_handle.read_at(&mut buffer[..wanted], position)?;
        check_cancelled(cancellation)?;
        if bytes_read == 0 {
            break;
        }
        position = position.saturating_add(bytes_read as u64);
        scanned_bytes = scanned_bytes.saturating_add(bytes_read as u64);
        let mut chunk = decoder.decode(&buffer[..bytes_read], false, &encoding)?;
        if first_chunk {
            first_chunk = false;
            if chunk.starts_with('\u{feff}') {
                chunk.remove(0);
                bom = true;
                consumed_bytes = consumed_bytes.saturating_add(3);
            }
        }

        if is_selected_line(current_line, offset, end_line)
            && previous_chunk_ended_with_cr
            && chunk.starts_with('\n')
        {
            line_ending = LineEnding::CrLf;
        }
        previous_chunk_ended_with_cr = chunk.ends_with('\r');

        let mut from = 0usize;
        while let Some(relative) = chunk[from..].find('\n') {
            let newline = from + relative;
            if is_selected_line(current_line, offset, end_line) {
                let fragment = &chunk[from..newline];
                if fragment.ends_with('\r') {
                    line_ending = LineEnding::CrLf;
                }
                append_selected_fragment(
                    fragment,
                    max_output_bytes,
                    &mut output,
                    &mut output_bytes,
                    &mut truncated_by_bytes,
                );
                if current_line.saturating_add(1) < end_line {
                    append_selected_fragment(
                        "\n",
                        max_output_bytes,
                        &mut output,
                        &mut output_bytes,
                        &mut truncated_by_bytes,
                    );
                }
            }
            consumed_bytes = consumed_bytes
                .saturating_add((newline - from) as u64)
                .saturating_add(1);
            current_line = current_line.saturating_add(1);
            from = newline + 1;
            if current_line >= end_line || truncated_by_bytes {
                original_line_count_exact = false;
                stopped_early = true;
                break;
            }
        }

        if !stopped_early && from < chunk.len() {
            let tail = &chunk[from..];
            if is_selected_line(current_line, offset, end_line) {
                append_selected_fragment(
                    tail,
                    max_output_bytes,
                    &mut output,
                    &mut output_bytes,
                    &mut truncated_by_bytes,
                );
            }
            consumed_bytes = consumed_bytes.saturating_add(tail.len() as u64);
        }
        if current_line >= end_line || truncated_by_bytes {
            original_line_count_exact = false;
            stopped_early = true;
        }
        if stopped_early {
            break;
        }
    }

    let budget_exhausted =
        !stopped_early && file_size > max_scan_bytes && scanned_bytes >= max_scan_bytes;
    if budget_exhausted {
        return Err(ReadTextRangeError::scan_budget(
            scanned_bytes,
            max_scan_bytes,
        ));
    }

    if !stopped_early {
        let tail = decoder.decode(&[], true, &encoding)?;
        if !tail.is_empty() {
            if is_selected_line(current_line, offset, end_line) {
                append_selected_fragment(
                    &tail,
                    max_output_bytes,
                    &mut output,
                    &mut output_bytes,
                    &mut truncated_by_bytes,
                );
            }
            consumed_bytes = consumed_bytes.saturating_add(tail.len() as u64);
        }
        if truncated_by_bytes {
            original_line_count_exact = false;
            stopped_early = true;
        }
    }

    let next_byte_offset = (stopped_early && !truncated_by_bytes && consumed_bytes < file_size)
        .then_some(consumed_bytes);

    Ok(ReadTextRangeResult {
        content: output,
        original_line_count: current_line.saturating_add(1),
        next_byte_offset,
        encoding: "utf-8".to_owned(),
        bom,
        line_ending,
        original_line_count_exact,
        truncated_by_bytes,
    })
}

fn is_selected_line(line: usize, offset: usize, end_line: usize) -> bool {
    line >= offset && line < end_line
}

fn detect_line_ending_from_content(content: &str) -> LineEnding {
    if content.contains("\r\n") {
        LineEnding::CrLf
    } else {
        LineEnding::Lf
    }
}

fn truncate_utf8(content: &str, max_bytes: usize) -> TruncatedText {
    if content.len() <= max_bytes {
        return TruncatedText {
            content: content.to_owned(),
            truncated: false,
        };
    }
    if max_bytes == 0 {
        return TruncatedText {
            content: String::new(),
            truncated: true,
        };
    }
    let mut end = 0usize;
    for (index, character) in content.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    TruncatedText {
        content: content[..end].to_owned(),
        truncated: true,
    }
}

struct TruncatedText {
    content: String,
    truncated: bool,
}

fn read_snapshot<H: ReadAt + ?Sized>(
    file_handle: &H,
    file_size: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<u8>, ReadTextRangeError> {
    let capacity = usize::try_from(file_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file snapshot is too large"))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut buffer = vec![0u8; RANGE_READ_CHUNK_BYTES.min(capacity.max(1))];
    let mut position = 0u64;
    while position < file_size {
        check_cancelled(cancellation)?;
        let wanted = usize::try_from((file_size - position).min(buffer.len() as u64))
            .unwrap_or(buffer.len());
        let bytes_read = file_handle.read_at(&mut buffer[..wanted], position)?;
        check_cancelled(cancellation)?;
        if bytes_read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..bytes_read]);
        position = position.saturating_add(bytes_read as u64);
    }
    Ok(bytes)
}

fn for_each_chunk<H, F>(
    file_handle: &H,
    from: u64,
    to_exclusive: u64,
    cancellation: Option<&CancellationToken>,
    mut consume: F,
) -> Result<(), ReadTextRangeError>
where
    H: ReadAt + ?Sized,
    F: FnMut(&[u8]) -> Result<bool, ReadTextRangeError>,
{
    let mut buffer = vec![0u8; RANGE_READ_CHUNK_BYTES];
    let mut position = from;
    while position < to_exclusive {
        check_cancelled(cancellation)?;
        let wanted = usize::try_from((to_exclusive - position).min(RANGE_READ_CHUNK_BYTES as u64))
            .unwrap_or(RANGE_READ_CHUNK_BYTES);
        let bytes_read = file_handle.read_at(&mut buffer[..wanted], position)?;
        check_cancelled(cancellation)?;
        if bytes_read == 0 {
            return Ok(());
        }
        position = position.saturating_add(bytes_read as u64);
        if !consume(&buffer[..bytes_read])? {
            return Ok(());
        }
    }
    Ok(())
}

#[derive(Default)]
struct Utf8StreamDecoder {
    pending: Vec<u8>,
}

impl Utf8StreamDecoder {
    fn decode(
        &mut self,
        bytes: &[u8],
        final_chunk: bool,
        encoding: &str,
    ) -> Result<String, ReadTextRangeError> {
        if self.pending.is_empty() && bytes.is_empty() {
            return Ok(String::new());
        }
        let mut combined = std::mem::take(&mut self.pending);
        combined.extend_from_slice(bytes);
        match std::str::from_utf8(&combined) {
            Ok(text) => Ok(text.to_owned()),
            Err(error) if error.error_len().is_none() && !final_chunk => {
                let valid_up_to = error.valid_up_to();
                self.pending.extend_from_slice(&combined[valid_up_to..]);
                Ok(std::str::from_utf8(&combined[..valid_up_to])
                    .expect("valid_up_to is valid UTF-8")
                    .to_owned())
            }
            Err(_) => Err(ReadTextRangeError::non_utf8(
                encoding,
                Some(InvalidUtf8Reason::InvalidUtf8),
            )),
        }
    }
}

fn detect_file_encoding<H: ReadAt + ?Sized>(
    file_handle: &H,
    file_size: u64,
) -> Result<String, ReadTextRangeError> {
    if file_size == 0 {
        return Ok("utf-8".to_owned());
    }
    let sample_size = usize::try_from(file_size)
        .unwrap_or(usize::MAX)
        .min(ENCODING_SAMPLE_BYTES);
    let mut sample = vec![0u8; sample_size];
    let bytes_read = read_some(file_handle, &mut sample, 0)?;
    sample.truncate(bytes_read);
    if let Some((encoding, _)) = detect_bom(&sample) {
        return Ok(encoding.to_owned());
    }
    if valid_utf8_prefix(&sample) {
        return Ok("utf-8".to_owned());
    }
    Ok(guess_encoding(&sample))
}

fn decode_complete_text(raw: &[u8]) -> (String, String, bool) {
    if raw.is_empty() {
        return (String::new(), "utf-8".to_owned(), false);
    }
    if let Some((encoding, bom_length)) = detect_bom(raw) {
        let body = &raw[bom_length..];
        let content = match encoding {
            "utf-8" => String::from_utf8_lossy(body).into_owned(),
            "utf-16le" => decode_utf16(body, true),
            "utf-16be" => decode_utf16(body, false),
            "utf-32le" => decode_utf32(body, true),
            "utf-32be" => decode_utf32(body, false),
            _ => String::from_utf8_lossy(body).into_owned(),
        };
        return (content, encoding.to_owned(), true);
    }
    if let Ok(content) = std::str::from_utf8(raw) {
        return (content.to_owned(), "utf-8".to_owned(), false);
    }
    let encoding = guess_encoding(raw);
    if !is_utf8_compatible_encoding(&encoding) {
        let (content, _) = Encoding::for_label(encoding.as_bytes())
            .unwrap_or(UTF_8)
            .decode_without_bom_handling(raw);
        (content.into_owned(), encoding, false)
    } else {
        (
            String::from_utf8_lossy(raw).into_owned(),
            "utf-8".to_owned(),
            false,
        )
    }
}

fn detect_bom(bytes: &[u8]) -> Option<(&'static str, usize)> {
    if bytes.starts_with(&[0xff, 0xfe, 0x00, 0x00]) {
        Some(("utf-32le", 4))
    } else if bytes.starts_with(&[0x00, 0x00, 0xfe, 0xff]) {
        Some(("utf-32be", 4))
    } else if bytes.starts_with(UTF8_BOM) {
        Some(("utf-8", 3))
    } else if bytes.starts_with(&[0xff, 0xfe]) {
        Some(("utf-16le", 2))
    } else if bytes.starts_with(&[0xfe, 0xff]) {
        Some(("utf-16be", 2))
    } else {
        None
    }
}

fn valid_utf8_prefix(bytes: &[u8]) -> bool {
    match std::str::from_utf8(bytes) {
        Ok(_) => true,
        Err(error) => error.error_len().is_none(),
    }
}

fn guess_encoding(bytes: &[u8]) -> String {
    let mut detector = EncodingDetector::new(Iso2022JpDetection::Deny);
    detector.feed(bytes, true);
    detector
        .guess(None, Utf8Detection::Allow)
        .name()
        .to_ascii_lowercase()
}

fn decode_utf16(bytes: &[u8], little_endian: bool) -> String {
    let units = bytes.chunks_exact(2).map(|pair| {
        if little_endian {
            u16::from_le_bytes([pair[0], pair[1]])
        } else {
            u16::from_be_bytes([pair[0], pair[1]])
        }
    });
    std::char::decode_utf16(units)
        .map(|result| result.unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

fn decode_utf32(bytes: &[u8], little_endian: bool) -> String {
    bytes
        .chunks_exact(4)
        .map(|word| {
            let code_point = if little_endian {
                u32::from_le_bytes([word[0], word[1], word[2], word[3]])
            } else {
                u32::from_be_bytes([word[0], word[1], word[2], word[3]])
            };
            char::from_u32(code_point).unwrap_or(char::REPLACEMENT_CHARACTER)
        })
        .collect()
}

fn read_some<H: ReadAt + ?Sized>(
    file_handle: &H,
    buffer: &mut [u8],
    offset: u64,
) -> Result<usize, ReadTextRangeError> {
    let mut read = 0usize;
    while read < buffer.len() {
        match file_handle.read_at(&mut buffer[read..], offset.saturating_add(read as u64))? {
            0 => break,
            count => read += count,
        }
    }
    Ok(read)
}

fn has_utf8_bom<H: ReadAt + ?Sized>(
    file_handle: &H,
    file_size: u64,
) -> Result<bool, ReadTextRangeError> {
    if file_size < UTF8_BOM.len() as u64 {
        return Ok(false);
    }
    let mut probe = [0u8; 3];
    let bytes_read = file_handle.read_at(&mut probe, 0)?;
    Ok(bytes_read == 3 && probe == *UTF8_BOM)
}

fn preceded_by_crlf_terminator<H: ReadAt + ?Sized>(
    file_handle: &H,
    start_offset: u64,
) -> Result<bool, ReadTextRangeError> {
    if start_offset < 2 {
        return Ok(false);
    }
    let mut probe = [0u8; 2];
    let bytes_read = file_handle.read_at(&mut probe, start_offset - 2)?;
    Ok(bytes_read == 2 && probe == *b"\r\n")
}

fn snap_to_line_start<H: ReadAt + ?Sized>(
    file_handle: &H,
    start_offset: u64,
    file_size: u64,
    max_snap_bytes: u64,
    cancellation: Option<&CancellationToken>,
    allow_eof: bool,
) -> Result<u64, ReadTextRangeError> {
    if start_offset == 0 || start_offset >= file_size {
        return Ok(start_offset);
    }
    let mut previous = [0u8; 1];
    let previous_read = file_handle.read_at(&mut previous, start_offset - 1)?;
    if previous_read == 1 && previous[0] == b'\n' {
        return Ok(start_offset);
    }

    let snap_end = file_size.min(start_offset.saturating_add(max_snap_bytes));
    let mut position = start_offset;
    let mut scanned = 0u64;
    let mut buffer = vec![0u8; RANGE_READ_CHUNK_BYTES];
    while position < snap_end {
        check_cancelled(cancellation)?;
        let wanted = usize::try_from((snap_end - position).min(RANGE_READ_CHUNK_BYTES as u64))
            .unwrap_or(RANGE_READ_CHUNK_BYTES);
        let bytes_read = file_handle.read_at(&mut buffer[..wanted], position)?;
        check_cancelled(cancellation)?;
        if bytes_read == 0 {
            break;
        }
        if let Some(index) = buffer[..bytes_read].iter().position(|byte| *byte == b'\n') {
            return Ok(start_offset
                .saturating_add(scanned)
                .saturating_add(index as u64)
                .saturating_add(1));
        }
        scanned = scanned.saturating_add(bytes_read as u64);
        position = position.saturating_add(bytes_read as u64);
    }
    if allow_eof {
        Ok(file_size)
    } else {
        Err(ReadTextRangeError::cursor_boundary(
            start_offset,
            max_snap_bytes,
        ))
    }
}

/// Report whether the string contains CRLF. As in the TypeScript utility, any
/// CRLF pair takes precedence over LF-only metadata.
pub fn line_ending_from_content(content: &str) -> LineEnding {
    detect_line_ending_from_content(content)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::{Seek, SeekFrom};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::{
        LineEnding, RANGE_READ_CHUNK_BYTES, ReadAt, ReadTextCursorWindowRequest,
        ReadTextRangeError, ReadTextRangeFromHandleRequest, ReadTextRangeRequest,
        line_ending_from_content, read_text_cursor_window_from_handle, read_text_range,
        read_text_range_from_handle,
    };
    use crate::utils::cancellation::CancellationToken;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "canopy-read-text-range-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone)]
    struct MutableBytes(Arc<Mutex<Vec<u8>>>);

    impl MutableBytes {
        fn new(bytes: Vec<u8>) -> Self {
            Self(Arc::new(Mutex::new(bytes)))
        }
    }

    struct AppendOnStreamRead(Mutex<AppendState>);

    struct AppendState {
        bytes: Vec<u8>,
        suffix: Vec<u8>,
        appended: bool,
    }

    impl AppendOnStreamRead {
        fn new(bytes: Vec<u8>, suffix: &[u8]) -> Self {
            Self(Mutex::new(AppendState {
                bytes,
                suffix: suffix.to_vec(),
                appended: false,
            }))
        }
    }

    impl ReadAt for AppendOnStreamRead {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
            let mut state = self.0.lock().unwrap();
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            if offset >= state.bytes.len() {
                return Ok(0);
            }
            let count = buffer.len().min(state.bytes.len() - offset);
            buffer[..count].copy_from_slice(&state.bytes[offset..offset + count]);
            if buffer.len() == RANGE_READ_CHUNK_BYTES && !state.appended {
                let suffix = std::mem::take(&mut state.suffix);
                state.bytes.extend_from_slice(&suffix);
                state.appended = true;
            }
            Ok(count)
        }
    }

    struct CancelOnStreamRead {
        bytes: Vec<u8>,
        token: CancellationToken,
    }

    impl ReadAt for CancelOnStreamRead {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            if offset >= self.bytes.len() {
                return Ok(0);
            }
            let count = buffer.len().min(self.bytes.len() - offset);
            buffer[..count].copy_from_slice(&self.bytes[offset..offset + count]);
            if buffer.len() == RANGE_READ_CHUNK_BYTES {
                self.token.cancel();
            }
            Ok(count)
        }
    }

    impl ReadAt for MutableBytes {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
            let bytes = self.0.lock().unwrap();
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            if offset >= bytes.len() {
                return Ok(0);
            }
            let count = buffer.len().min(bytes.len() - offset);
            buffer[..count].copy_from_slice(&bytes[offset..offset + count]);
            Ok(count)
        }
    }

    fn lines(count: usize) -> String {
        (0..count)
            .map(|index| format!("line-{} {}", index + 1, "x".repeat(180)))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn fast_path_keeps_split_newline_semantics_and_detects_bom_line_endings() {
        let dir = TempDir::new();
        let empty = dir.0.join("empty");
        fs::write(&empty, b"").unwrap();
        let result = read_text_range(ReadTextRangeRequest {
            path: &empty,
            offset: Some(0),
            limit: Some(10),
            max_output_bytes: 100,
            max_scan_bytes: None,
            cancellation: None,
        })
        .unwrap();
        assert_eq!(result.content, "");
        assert_eq!(result.original_line_count, 1);

        let body = [b"\xef\xbb\xbf".as_slice(), b"first\r\nsecond\r\n"].concat();
        let path = dir.0.join("bom-crlf");
        fs::write(&path, body).unwrap();
        let result = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(0),
            limit: Some(10),
            max_output_bytes: 100,
            max_scan_bytes: None,
            cancellation: None,
        })
        .unwrap();
        assert_eq!(result.content, "first\r\nsecond\r\n");
        assert!(result.bom);
        assert_eq!(result.line_ending, LineEnding::CrLf);
        assert_eq!(result.original_line_count, 3);
        assert!(result.original_line_count_exact);

        let mut utf16 = vec![0xff, 0xfe];
        for unit in "utf16\r\n".encode_utf16() {
            utf16.extend_from_slice(&unit.to_le_bytes());
        }
        let path = dir.0.join("utf16le");
        fs::write(&path, utf16).unwrap();
        let result = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(0),
            limit: None,
            max_output_bytes: 100,
            max_scan_bytes: None,
            cancellation: None,
        })
        .unwrap();
        assert_eq!(result.content, "utf16\r\n");
        assert_eq!(result.encoding, "utf-16le");
        assert!(result.bom);
    }

    #[test]
    fn streams_deep_large_ranges_and_keeps_output_within_utf8_byte_budget() {
        let dir = TempDir::new();
        let body = lines(65_000);
        let path = dir.0.join("large.log");
        fs::write(&path, body).unwrap();
        let result = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(42_000),
            limit: Some(3),
            max_output_bytes: 256,
            max_scan_bytes: None,
            cancellation: None,
        })
        .unwrap();
        assert!(result.content.starts_with("line-42001 "));
        assert!(result.content.len() <= 256);
        assert!(result.truncated_by_bytes);
        assert!(!result.original_line_count_exact);
        assert_eq!(result.encoding, "utf-8");
    }

    #[test]
    fn scan_budget_rejects_deep_ranges_but_serves_a_shallow_window() {
        let dir = TempDir::new();
        let body = lines(5_000);
        let path = dir.0.join("budget.log");
        fs::write(&path, body).unwrap();
        let shallow = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(0),
            limit: Some(3),
            max_output_bytes: 1_024,
            max_scan_bytes: Some(100_000),
            cancellation: None,
        })
        .unwrap();
        assert!(shallow.content.starts_with("line-1 "));

        let error = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(4_000),
            limit: Some(20),
            max_output_bytes: 1_024,
            max_scan_bytes: Some(100_000),
            cancellation: None,
        })
        .unwrap_err();
        assert!(matches!(
            error,
            ReadTextRangeError::TextScanBudgetExceeded {
                scanned_bytes: 100_000,
                max_scan_bytes: 100_000,
                ..
            }
        ));
    }

    #[test]
    fn handle_reads_are_bounded_to_the_captured_size_snapshot() {
        let initial = format!("{}\n{}", "a".repeat(500_000), "b".repeat(50_000));
        let handle =
            AppendOnStreamRead::new(initial.as_bytes().to_vec(), b"\nAPPENDED-AFTER-OPEN\n");
        let snapshot_size = initial.len() as u64;
        let result = read_text_range_from_handle(
            &handle,
            ReadTextRangeFromHandleRequest {
                offset: Some(1),
                limit: Some(1),
                file_size: snapshot_size,
                max_output_bytes: 100_000,
                max_scan_bytes: 1_000_000,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(result.content, "b".repeat(50_000));
        assert!(!result.content.contains("APPENDED"));
        assert!(result.original_line_count_exact);
    }

    #[test]
    fn handle_reads_leave_the_descriptor_cursor_unchanged_and_preserve_bom_offsets() {
        let dir = TempDir::new();
        let path = dir.0.join("handle.log");
        fs::write(&path, b"\xef\xbb\xbfone\ntwo\nthree\n").unwrap();
        let mut positioned = File::open(path).unwrap();
        positioned.seek(SeekFrom::Start(2)).unwrap();
        let cursor_before = positioned.stream_position().unwrap();
        let file_size = positioned.metadata().unwrap().len();
        let result = read_text_range_from_handle(
            &positioned,
            ReadTextRangeFromHandleRequest {
                offset: Some(0),
                limit: Some(1),
                file_size,
                max_output_bytes: 100,
                max_scan_bytes: file_size,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(result.content, "one");
        assert!(result.bom);
        assert_eq!(result.next_byte_offset, Some(7));
        assert_eq!(positioned.stream_position().unwrap(), cursor_before);
    }

    #[test]
    fn byte_truncation_never_splits_utf8_and_marks_invalid_late_bytes() {
        let multibyte = MutableBytes::new("a🙂b".as_bytes().to_vec());
        let result = read_text_range_from_handle(
            &multibyte,
            ReadTextRangeFromHandleRequest {
                offset: Some(0),
                limit: Some(1),
                file_size: 6,
                max_output_bytes: 4,
                max_scan_bytes: 6,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(result.content, "a");
        assert!(result.truncated_by_bytes);
        assert!(!result.content.contains('\u{fffd}'));

        let mut late_invalid = vec![b'a'; 9 * 1024];
        late_invalid.extend_from_slice(&[0xc0, 0xaf]);
        late_invalid.extend(std::iter::repeat_n(b'b', RANGE_READ_CHUNK_BYTES));
        let error = read_text_range_from_handle(
            &MutableBytes::new(late_invalid.clone()),
            ReadTextRangeFromHandleRequest {
                offset: Some(0),
                limit: Some(500),
                file_size: late_invalid.len() as u64,
                max_output_bytes: 20_000,
                max_scan_bytes: late_invalid.len() as u64,
                cancellation: None,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ReadTextRangeError::LargeNonUtf8Text {
                reason: Some(super::InvalidUtf8Reason::InvalidUtf8),
                ..
            }
        ));
    }

    #[test]
    fn cursor_windows_snap_to_line_starts_and_refuse_unbounded_snaps() {
        let bytes = MutableBytes::new(b"alpha\nbeta\ngamma\n".to_vec());
        let page = read_text_cursor_window_from_handle(
            &bytes,
            ReadTextCursorWindowRequest {
                start_offset: 2,
                file_size: 17,
                limit: Some(1),
                max_output_bytes: 1_024,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(page.start_offset, 6);
        assert_eq!(page.content, "beta");
        assert_eq!(page.next_offset, Some(11));

        let error = read_text_cursor_window_from_handle(
            &MutableBytes::new(vec![b'x'; 5_000]),
            ReadTextCursorWindowRequest {
                start_offset: 10,
                file_size: 5_000,
                limit: None,
                max_output_bytes: 100,
                max_snap_bytes: 64,
                cancellation: None,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ReadTextRangeError::CursorNotAtLineBoundary {
                start_offset: 10,
                max_snap_bytes: 64,
                ..
            }
        ));
    }

    #[test]
    fn cursor_pages_keep_crlf_metadata_and_advance_past_truncated_lines() {
        let giant = format!("{}\r\nbb", "x".repeat(600 * 1024));
        let handle = MutableBytes::new(giant.as_bytes().to_vec());
        let first = read_text_cursor_window_from_handle(
            &handle,
            ReadTextCursorWindowRequest {
                start_offset: 0,
                file_size: giant.len() as u64,
                limit: None,
                max_output_bytes: 100,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(first.content, "x".repeat(100));
        assert!(first.truncated_by_bytes);
        assert_eq!(first.line_ending, LineEnding::CrLf);
        assert_eq!(first.next_offset, Some(600 * 1024 + 2));

        let second = read_text_cursor_window_from_handle(
            &handle,
            ReadTextCursorWindowRequest {
                start_offset: first.next_offset.unwrap(),
                file_size: giant.len() as u64,
                limit: None,
                max_output_bytes: 100,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(second.content, "bb");
        assert_eq!(second.line_ending, LineEnding::CrLf);
    }

    #[test]
    fn large_range_detects_crlf_across_a_read_chunk_boundary() {
        let dir = TempDir::new();
        let mut body = vec![b'a'; RANGE_READ_CHUNK_BYTES - 1];
        body.extend_from_slice(b"\r\nsecond\n");
        body.extend(std::iter::repeat_n(b'x', 11 * 1024 * 1024));
        let path = dir.0.join("split-crlf.log");
        fs::write(&path, body).unwrap();
        let result = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(0),
            limit: Some(2),
            max_output_bytes: RANGE_READ_CHUNK_BYTES + 100,
            max_scan_bytes: None,
            cancellation: None,
        })
        .unwrap();
        assert_eq!(result.line_ending, LineEnding::CrLf);
        assert!(result.content.ends_with("\r\nsecond"));
    }

    #[test]
    fn cursor_makes_progress_when_a_multibyte_character_does_not_fit() {
        let handle = MutableBytes::new("中\nnext\n".as_bytes().to_vec());
        let first = read_text_cursor_window_from_handle(
            &handle,
            ReadTextCursorWindowRequest {
                start_offset: 0,
                file_size: "中\nnext\n".len() as u64,
                limit: None,
                max_output_bytes: 1,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(first.content, "");
        assert!(first.truncated_by_bytes);
        assert_eq!(first.next_offset, Some("中\n".len() as u64));

        let second = read_text_cursor_window_from_handle(
            &handle,
            ReadTextCursorWindowRequest {
                start_offset: first.next_offset.unwrap(),
                file_size: "中\nnext\n".len() as u64,
                limit: None,
                max_output_bytes: 1_024,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(second.content, "next\n");
    }

    #[test]
    fn cursor_output_budget_keeps_whole_lines_and_bom_cursor_offsets() {
        let body = [b"\xef\xbb\xbf".as_slice(), b"one\r\ntwo\r\n"].concat();
        let handle = MutableBytes::new(body.clone());
        let first = read_text_cursor_window_from_handle(
            &handle,
            ReadTextCursorWindowRequest {
                start_offset: 0,
                file_size: body.len() as u64,
                limit: Some(1),
                max_output_bytes: 100,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(first.content, "one\r");
        assert!(first.bom);
        assert_eq!(first.line_ending, LineEnding::CrLf);
        assert_eq!(first.next_offset, Some(8));

        let limited = read_text_cursor_window_from_handle(
            &MutableBytes::new(b"aaa\nbbb\nccc\r\n".to_vec()),
            ReadTextCursorWindowRequest {
                start_offset: 0,
                file_size: 13,
                limit: None,
                max_output_bytes: 8,
                max_snap_bytes: 1_024,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(limited.content, "aaa\nbbb");
        assert_eq!(limited.line_ending, LineEnding::Lf);
        assert_eq!(limited.next_offset, Some(8));
    }

    #[test]
    fn rejects_large_legacy_encoding_and_observes_cancellation() {
        let dir = TempDir::new();
        let body = vec![0x81; 11 * 1024 * 1024];
        let path = dir.0.join("legacy.log");
        fs::write(&path, body).unwrap();
        let error = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(0),
            limit: Some(10),
            max_output_bytes: 10_000,
            max_scan_bytes: None,
            cancellation: None,
        })
        .unwrap_err();
        assert!(matches!(error, ReadTextRangeError::LargeNonUtf8Text { .. }));

        let token = CancellationToken::new();
        token.cancel();
        let error = read_text_range(ReadTextRangeRequest {
            path: &path,
            offset: Some(0),
            limit: Some(10),
            max_output_bytes: 10_000,
            max_scan_bytes: None,
            cancellation: Some(&token),
        })
        .unwrap_err();
        assert!(matches!(error, ReadTextRangeError::Cancelled { .. }));

        let token = CancellationToken::new();
        let growing_read = CancelOnStreamRead {
            bytes: vec![b'a'; 11 * 1024 * 1024],
            token: token.clone(),
        };
        let error = read_text_range_from_handle(
            &growing_read,
            ReadTextRangeFromHandleRequest {
                offset: Some(0),
                limit: Some(10),
                file_size: 11 * 1024 * 1024,
                max_output_bytes: 10_000,
                max_scan_bytes: 11 * 1024 * 1024,
                cancellation: Some(&token),
            },
        )
        .unwrap_err();
        assert!(matches!(error, ReadTextRangeError::Cancelled { .. }));
    }

    #[test]
    fn reports_the_next_line_byte_cursor_without_repeating_a_final_newline() {
        let body = b"first\nsecond\n";
        let result = read_text_range_from_handle(
            &MutableBytes::new(body.to_vec()),
            ReadTextRangeFromHandleRequest {
                offset: Some(0),
                limit: Some(2),
                file_size: body.len() as u64,
                max_output_bytes: 1_024,
                max_scan_bytes: body.len() as u64,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(result.content, "first\nsecond");
        assert_eq!(result.next_byte_offset, None);
    }

    #[test]
    fn reports_a_forward_byte_cursor_after_skipping_a_chunk_spanning_line() {
        let first_line = "a".repeat(RANGE_READ_CHUNK_BYTES + 10);
        let body = format!("{first_line}\nsecond\nthird");
        let result = read_text_range_from_handle(
            &MutableBytes::new(body.as_bytes().to_vec()),
            ReadTextRangeFromHandleRequest {
                offset: Some(1),
                limit: Some(1),
                file_size: body.len() as u64,
                max_output_bytes: 1_024,
                max_scan_bytes: body.len() as u64,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(result.content, "second");
        assert_eq!(
            result.next_byte_offset,
            Some(format!("{first_line}\nsecond\n").len() as u64)
        );
        assert!(!result.original_line_count_exact);
    }

    #[test]
    fn range_beyond_eof_returns_the_exact_line_count() {
        let body = b"a\nb";
        let result = read_text_range_from_handle(
            &MutableBytes::new(body.to_vec()),
            ReadTextRangeFromHandleRequest {
                offset: Some(99),
                limit: Some(10),
                file_size: body.len() as u64,
                max_output_bytes: 1_024,
                max_scan_bytes: body.len() as u64,
                cancellation: None,
            },
        )
        .unwrap();
        assert_eq!(result.content, "");
        assert_eq!(result.original_line_count, 2);
        assert!(result.original_line_count_exact);
        assert!(!result.truncated_by_bytes);
    }

    #[test]
    fn small_byte_truncation_preserves_scalar_boundaries() {
        let cut = super::truncate_utf8("a🙂b", 4);
        assert_eq!(cut.content, "a");
        assert!(cut.truncated);
        assert_eq!(line_ending_from_content("a\r\nb"), LineEnding::CrLf);
        assert_eq!(line_ending_from_content("a\nb"), LineEnding::Lf);
    }
}
