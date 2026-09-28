# Channel loop scheduler port

`channel_loop_scheduler.rs` ports `packages/channels/base/src/ChannelLoopScheduler.ts`.
It polls the persistent `ChannelLoopStore`, asks an injected cron resolver for
the next fire time, coalesces overlapping ticks, reserves no more than five
jobs at once, and sends due work to channel runner adapters. The adapter API
receives the timeout budget and an async `should_continue` callback that checks
the scheduler generation and current enabled state.

The port includes startup reconciliation of stale `runningSince` fields,
recurrence anchoring on the later of `lastFiredAt` and `lastFinishedAt`,
rechecks before dispatch, skipped-run handling, failure counts and auto-disable,
bridge-recovery error handling, one-shot completion, success/failure persistence
recovery, and the 500-unit result / 1,000-unit error limits.

Behavior differences and limits:

- Callers inject the cron parser/resolver. This module does not bundle cron
  syntax or timezone policy.
- `start()` requires an active Tokio runtime. The interval skips missed timer
  ticks after a slow reconciliation or tick; concurrent/manual ticks coalesce.
- The timeout is forwarded as `timeout_ms`, matching the TypeScript adapter
  contract. The channel adapter is responsible for enforcing that budget.
- Rust cannot forcibly cancel an in-progress adapter future on `stop()`. Stop
  invalidates the generation and clears scheduler reservations. As in the
  TypeScript source, error handling checks the generation, while success checks
  whether its persisted `runningSince` marker still matches. A replacement run
  that finishes first clears that marker and fences the older result; if no
  replacement takes ownership, the old success can still commit. A run that
  throws a skip error still records its finish time after stop.
- Preview and error caps count UTF-16 code units like JavaScript `slice`, but
  Rust truncation stops at a valid UTF-8 scalar boundary if a supplementary
  character straddles the cap.
- The concrete store is shared through `Arc`; its per-instance update mutex
  does not coordinate separate store instances or processes.

Focused tests cover tick coalescing, bounded success/failure text, the five-run
cap, delayed dispatch, skip and recovery accounting, stale startup state,
disabled-job continuation checks, stop-generation fencing, and Unicode-safe
truncation. They are included in the core crate test module. Workspace Cargo
verification is pending root integration because the shared workspace Cargo
commands are coordinated by the parent task.
