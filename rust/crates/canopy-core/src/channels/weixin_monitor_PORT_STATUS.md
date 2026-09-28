# Weixin monitor Rust port status

`weixin_monitor.rs` ports the long-poll loop and user-message extraction from
`packages/channels/weixin/src/monitor.ts`.

## Behavior covered

- Reads `<Weixin state directory>/cursor.txt`, trims the restored value, and
  writes the next cursor only after all message callbacks succeed.
- Uses the shared `weixin_api::get_updates`, `weixin_types`, and
  `weixin_accounts::get_state_dir` modules.
- Starts with a 40-second poll timeout and uses a positive server timeout plus
  5 seconds for subsequent polls.
- Pauses 30 seconds on `errcode == -14`; other failures retry after 2 seconds,
  then pause 30 seconds after each third consecutive error.
- Stops cleanly when its `CancellationToken` is cancelled, including when the
  API call fails during cancellation.
- Caches per-user context tokens and extracts USER text, image/file CDN
  references, referenced text, message IDs, and media-only fallback text.
- Provides injectable API, cursor, clock, sleep, and output boundaries for
  deterministic offline testing.

## Dependencies and integration

No Cargo manifest or lockfile changes are required. The module uses existing
`reqwest`, `serde_json`, and Tokio dependencies. Export it from
`channels/mod.rs` with:

```rust
pub mod weixin_monitor;
```

Until that export is added, the normal `canopy-core` test target does not
compile or discover this module's unit tests. The file was compiled and tested
through an isolated offline harness against the existing wire models and
compatible API/account seams.

## Verification

`rustfmt --edition 2024` completed. The isolated offline harness passed 13
tests: 9 monitor tests covering cursor read/write ordering, callback failure,
message extraction, dynamic timeout, session-expiry pause, retry delays, and
cancellation; plus 4 existing Weixin wire-type tests. No TypeScript
`monitor.test.ts` is present in the Weixin package, so these focused Rust tests
are based on the source loop behavior and the requested edge cases.
