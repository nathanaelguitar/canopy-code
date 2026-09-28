# `PollingChannelBase.ts` Rust port

Implemented in `canopy-core::channels::channel_polling` as a reusable generic
poll loop. It loads only object-shaped JSON cursors, falls back to the supplied
initial cursor on missing/corrupt state, saves after successful polls, starts
only one loop per instance, interrupts timer waits on stop, and applies the
source's 2-second exponential error backoff capped at 30 seconds. Cursor names
retain the source's ASCII normalization, 200 UTF-16-unit cap, and SHA-256
suffix. Writes use the shared synced atomic writer.

Platform polling methods remain adapter traits. The Rust API requires an active
Tokio runtime, exposes explicit stop-and-wait for orderly shutdown, rounds
positive fractional intervals up to one millisecond, and uses a same-directory
atomic replacement instead of the TypeScript implementation's fixed `.tmp`
file. No concrete channel adapter has been connected to this runtime yet.

Focused tests cover interval validation, bounded unique cursor paths, cursor
restore and invalid-file fallback, durable save/reload, idempotent start,
successful polling, error handling, and interruptible shutdown. All five tests
pass in the `canopy-core` workspace target.
