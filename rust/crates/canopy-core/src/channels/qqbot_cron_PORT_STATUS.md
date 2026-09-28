# QQBot cron text buffering port status

Ported the non-prompt text chunk accumulation and cron-flow depth gate from
`handleCronTextChunk` / `runCronFlow` in
`packages/channels/qqbot/src/QQChannel.ts` to `qqbot_cron.rs`.

## Implemented

- Captures cron-flow depth at chunk receipt, defers handling by one Tokio
  scheduler turn, and checks readiness and active prompt-stream ownership
  before buffering.
- Coalesces chunks per session, uses the configured threshold with the 4096
  default, counts UTF-16 code units like JavaScript, and schedules the 2-second
  idle flush or immediate flush at the threshold. An empty chunk still creates
  an entry and timer, then follows the source's zero-character no-target log
  and cleanup path without sending.
- Resolves the route at each send attempt. Missing targets drop the buffered
  message; retries verify the session buffer is still current before lookup.
- Retries transient and unclassified send failures after 5 seconds and then 10
  seconds. `RETRY_EXHAUSTED`, `ACTIVE_MSG_DISABLED`, and `FALLBACK_FAILED` are
  terminal; any failure on the third attempt drops the entry.
- A new chunk cancels a scheduled retry, prepends its pending text, then applies
  the normal coalescing delay. Disconnect aborts scheduled timers, clears the
  buffers, and resets cron-flow depth.
- Readiness, stream-state, route lookup, sending, logging, and sleeping are
  injected. The module has no persistence or bridge/router dependency.

## Integration notes and limits

The native CLI QQ host constructs this buffer only when
`cron-msg-experimental` is enabled. It connects ACP agent text chunks to
`handle_text_chunk(...)`, maps QQ delivery errors to `CronSendErrorCode`, and
implements route lookup, readiness, active-stream checks, and persisted QQ
message delivery. It resets readiness on gateway close and calls `disconnect()`
when the host exits. The TypeScript `runCronFlow()` method currently has no call
site in the repository, so there is no source scheduled-prompt trigger to wire
to `with_cron_flow(...)`; ordinary prompt chunks remain gated out. QQ also
inherits ChannelBase's disabled proactive-send policy, so shared channel loops
do not run on QQ.

Tokio timers require a running runtime and are aborted by `disconnect()`. An
in-flight send future is not cancelled, matching the source's inability to
cancel an already-started `sendMessage` promise. Node's `unref()` behavior has no
direct Tokio equivalent; tasks end with their timer, explicit disconnect, or
runtime shutdown.

## Verification

`rustfmt --edition 2024 rust/crates/canopy-core/src/channels/qqbot_cron.rs`
completed.

`cargo test --manifest-path /tmp/qqbot-cron-harness/Cargo.toml --offline`
passed: 19 passed, 0 failed (11 cron tests plus the included `sanitize` tests).
The temporary harness compiles these exact source files and uses manual sleeper
and send/route mocks; it performs no external network calls and advances the
2-second, 5-second, and 10-second timers deterministically.

The full workspace suite passed 1,412 tests after integration; the channel
subset passed 449 tests. Adapter construction and event forwarding remain to
be wired by the parent integration.
