# BlockStreamer Rust port status

Source: packages/channels/base/src/BlockStreamer.ts and
packages/channels/base/src/BlockStreamer.test.ts.

Implementation: block_streamer.rs.

## Ported behavior

- Buffers pushed text, force-splits at max_chars, then checks for the last
  eligible paragraph boundary at or after min_chars.
- Split priority is paragraph break, newline, space, then the size limit.
- Measures thresholds and split positions in UTF-16 code units, as JavaScript
  does, while retaining valid UTF-8 Rust strings.
- Idle emission resets after each push, emits only at or above min_chars,
  and is disabled by a zero duration.
- Serializes asynchronous sends, swallows send errors and panics, trims blocks,
  skips empty blocks, and counts emitted non-empty blocks.
- flush emits the remainder and awaits previously queued sends. stop cancels
  the idle task and drops only the buffered text; later pushes remain valid.
- Timer scheduling is behind BlockStreamerClock, with a Tokio implementation
  for production and a controllable clock in unit tests.

## API and behavioral differences

- Construct BlockStreamer inside a Tokio runtime. push and stop are
  synchronous; flush is async because it waits for the send queue.
- The Rust callback returns a future resolving to Result<(), E>. A returned
  error or panic is swallowed to match the TypeScript promise chain.
- max_chars == 0 is rejected to avoid a non-progressing split loop. The
  TypeScript implementation does not validate this option.
- JavaScript strings can contain lone UTF-16 surrogates after slicing through
  an emoji. Rust strings cannot. If the split limit lands inside a surrogate
  pair, this port keeps the full Unicode scalar together, so that one emitted
  block can exceed the limit by one UTF-16 unit.

## Verification

- Added 21 focused unit tests for boundaries, split preference, idle/reset/stop,
  flush, serialization, error swallowing, trimming, and UTF-16 split behavior.
- rustfmt --edition 2024 rust/crates/canopy-core/src/channels/block_streamer.rs
  passes.
- git diff --check -- rust/crates/canopy-core/src/channels/block_streamer.rs
  passes.
- Cargo compilation and execution of the Rust unit tests are pending root
  workspace validation; no Cargo command was run for this slice.

This source has not yet been exported from channels/mod.rs or wired into a
channel adapter.
