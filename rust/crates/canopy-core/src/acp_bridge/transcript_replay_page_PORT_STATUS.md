# Transcript replay page port status

The new `transcript_replay_page` module reads a `SessionTranscriptRecordPage`
and returns `BridgeSessionTranscriptPage.events` using the serve event shape
`{v: 1, type: "session_update", data: update}`. It signs the next cursor with
the reader's workspace cursor codec after writing updated replay state into a
forward cursor. Backward pages preserve their cursor replay state and start
with no pending tool calls, matching the TypeScript page converter.

Implemented projections include user and assistant text, thought chunks,
images and stored media references, assistant tool starts, correlated tool
results and errors, dangling tool-call failure events, cumulative usage,
`todo_write` plans, V2 and legacy Goal cards, history-gap notices, and branch
checkpoint metadata on the final visible assistant text chunk. Timestamps are
lifted to the event update and retained in metadata. Replay cursors accept the
current pending-call fields and the previous `recordId`/`timestamp` aliases.

## Bounds and failure behavior

The transcript reader bounds input pages to 500 records and 4 MiB by default.
Replay output is separately capped at 20,000 events, 8 MiB of serialized
events, and a 64 KiB encoded cursor. If an output cap is reached, the module
returns the events emitted so far as a partial page, sets `hasMore` to false,
and withholds the continuation cursor. The replay error currently uses the
same generic text as the TypeScript converter's conversion failure.

## Remaining parity and host integration

- The serve route still needs to call this module. Its TypeScript wrapper also
  flushes recording before backward reads and finalizes dangling calls only
  when no prompt is active before or after the read; the native route must
  supply the same `finalize_dangling` decision.
- Tool titles, kinds, and locations use deterministic fallback metadata
  (`toolName` plus optional argument description, kind `other`, no locations).
  The CLI's configured `ToolCallEmitter` metadata resolver is not available in
  this core module.
- Tool-result text does not run the CLI's ACP output projection, terminal text
  sanitizer, or projection diagnostics. Vision notice prefixes and file diffs
  are projected, but presentation can differ for those specialized displays.
- The TypeScript implementation reports malformed transcript-part diagnostics
  and localizes the history-gap notice. This core converter skips malformed
  parts without diagnostics and currently uses the English source string.
- For user hook display-text replacement, the native path preserves visible
  text and non-text parts but appends replacement text if the original message
  has no text part; the TypeScript helper also preserves its exact insertion
  position among interleaved parts.
- Event and cursor caps are native safety limits and do not exist in the
  TypeScript page converter itself. Capped pages are deliberately marked
  partial and do not advance the cursor.

No tests or Cargo commands were run for this port slice.
