# QQ Bot persisted routing state port status

## Implemented

`qqbot_persistence.rs` ports the routing-state helpers from
`packages/channels/qqbot/src/QQChannel.ts` (`serializeQQState`, `saveQQState`,
`flushQQState`, and `restoreQQState`). It defines the five ordered map fields,
serializes them in the source's field order as `[key, value]` arrays, and uses
`IndexMap` to keep JavaScript `Map` insertion order.

Restore behavior covers object-root validation, per-map field filters, UTF-16
key and message-ID length limits, OpenID syntax, safe integer sequence values,
reply timestamp checks, legacy string reply-ID normalization, missing/falsy
fields, and truthy non-array fields. Malformed JSON and non-iterable map entries
return a restore failure, matching the source's outer catch. Overflowing JSON
number literals are normalized so `1e999` retains JavaScript's `Infinity`
truthiness and is rejected by the same map filters.

Persistence uses a 500 ms replaceable debounce, writes to `<path>.tmp` with
owner-only mode on Unix, then renames atomically. Failed writes best-effort
remove the temp file. `flush()` cancels the pending debounce and writes even
after disposal; delayed saves check disposal before writing. File operations
are injectable through `QqStateFileOps`.

## Focused coverage

Ten Rust tests cover serialized field order and shape, accepted/rejected map
values and boundaries, UTF-16 limits, legacy reply-ID migration, timestamp
upper bound, overflow numbers, malformed roots/entries, debounce replacement,
temp-write/rename ordering, flush cancellation after disposal, disposed timer
behavior, failed-write cleanup, missing/corrupt files, and Unix mode `0o600`.

## Caller wiring still required

- Export `qqbot_persistence` from `rust/crates/canopy-core/src/channels/mod.rs`.
- Supply QQChannel's per-channel state path, ensuring the parent `channels`
  directory exists, and copy/share the five adapter maps with
  `QqbotRoutingState`.
- Call `save()` after source state mutations; mark the helper disposed as part
  of disconnect; call `flush()` for disconnect, invalid-session recovery, and
  the before-exit hook. `save()` must run on an active Tokio runtime.
- Tokio does not expose Node's timer `.unref()` behavior directly. The delayed
  save is a detached task and is dropped when its runtime shuts down.
- Rust strings cannot represent JavaScript lone UTF-16 surrogate code units;
  ordinary Unicode keys and IDs use the same UTF-16 length checks.

## Verification

- `rustfmt --edition 2024` completed on `qqbot_persistence.rs`.
- An isolated integration harness ran with
  `cargo test -p canopy-core --test qqbot_persistence_harness --offline` from
  `rust/`: 18 passed, 0 failed (10 QQ persistence tests plus 8 existing
  sanitizer tests pulled in by the standalone path harness). The harness file
  was removed after the run; no shared module export or manifest was changed.
