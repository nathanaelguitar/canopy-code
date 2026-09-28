//! Bounded ACP JSONL framing shared by the native channel subprocess hosts.

use tokio::io::{self, AsyncBufRead, AsyncBufReadExt};

pub(crate) const MAX_ACP_OUTPUT_LINE_BYTES: usize = 16 * 1024 * 1024;

pub(crate) enum BoundedLine {
    Complete(Vec<u8>),
    TooLarge,
}

/// Read one newline-delimited frame without retaining more than
/// [`MAX_ACP_OUTPUT_LINE_BYTES`]. The channel host terminates the child when
/// this reports an oversized frame, so the rest of that frame is not drained.
pub(crate) async fn read_bounded_line<R>(reader: &mut R) -> io::Result<Option<BoundedLine>>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::with_capacity(8 * 1024);

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(BoundedLine::Complete(bytes)))
            };
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if bytes.len().saturating_add(consumed) > MAX_ACP_OUTPUT_LINE_BYTES {
            return Ok(Some(BoundedLine::TooLarge));
        }
        bytes.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);

        if newline.is_some() {
            return Ok(Some(BoundedLine::Complete(bytes)));
        }
    }
}
