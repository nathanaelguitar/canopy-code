# `SessionRouter.ts` Rust port

Implemented in `canopy-core::channels::session_router` with a bridge adapter
seam (`ChannelSessionBridge`). The module covers:

- `user`, `thread`, `chat_thread`, and `single` routing keys, including
  per-channel scope/approval overrides and the separate routing thread ID.
- One in-flight reservation per key, concurrent resolve coalescing, bridge
  replacement, group-target promotion, and source-channel attribution on new
  sessions.
- Route removal by target or session ID, sender-wide cleanup, eager death
  removal, lazy dormancy, and dead-ID tracking within active bridge load
  windows.
- Eager restore with all persisted keys reserved up front, lazy metadata-only
  restore, fallback creation after failed loads, generation/token invalidation,
  and best-effort disposal of late bindings.
- Persisted-map validation, malformed-file quarantine, invalid-entry dropping,
  route insertion order, private same-directory atomic replacement, and
  persistence of changed IDs.

Focused unit tests exercise scope and group behavior, coalesced creation,
invalidation/discard, retry after a session dies in a load window, eager restore
and failure cleanup, lazy load/replacement and dormant retention, death
handling, persisted target validation, route insertion order, and atomic-write
behavior. The isolated module harness passes 24 tests. Workspace Cargo checks
are tracked in the shared channel status note.

## Adapter and compatibility notes

- Rust callers supply `Arc<dyn ChannelSessionBridge>` and invoke
  `handle_session_died` from their bridge event loop. This module does not own
  bridge event subscriptions or daemon session lifecycle wiring.
- `SessionTarget` is the shared channel-loop type and carries flattened extra
  JSON fields; the TypeScript interface only names its standard fields.
- Bridge failures cross the seam as strings. Persistence remains best effort,
  matching the source's log-and-continue behavior. Writes use the core atomic
  writer with owner-only permissions.
- Public Rust APIs use typed scopes, option structs, the exported `RouterResult`, and tuple
  restore counts rather than mirroring TypeScript positional object shapes.
- Rust state operations use short-lived standard mutex guards; no guard spans
  a bridge await. The persistence lock is process-local, as in the source.
- The focused harness and workspace compile must still verify the module once
  it is exported from `channels/mod.rs`.
