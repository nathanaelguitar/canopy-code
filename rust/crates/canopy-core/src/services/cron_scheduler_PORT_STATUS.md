# Session cron scheduler port status

Source: `packages/core/src/services/cronScheduler.ts`.

`cron_scheduler.rs` ports the bounded session-only job/wakeup state, five-field
cron validation, deterministic recurring and one-shot jitter, the one-second
timer, manual ticks, one-shot removal, recurring expiry with a final fire,
single-pending-wakeup replacement, delay clamping, the 24-hour wakeup-chain
budget, and start/stop/disable/destroy behavior. The timer uses a standard
thread and invokes callbacks outside its state lock. `tools/cron.rs` provides
provider schemas and synchronous create/list/delete/loop-wakeup handlers.

The native slice is not a full port of TypeScript `CronScheduler`:

- `durable: true` is rejected. The scheduler does not load `scheduled_tasks.json`,
  acquire/release `cron_tasks_lock`, watch updates, persist fires/removals, or
  recover missed/catch-up/final work. The independent task-file and lock
  primitives are not lifecycle wiring.
- The native CLI declares and dispatches the four tools and holds an
  in-memory scheduler for a `WorkspaceTools` lifetime, but it does not yet
  start the scheduler timer or enqueue fired prompts into its live agent loop.
  The core scheduler's callback API is ready for that host wiring; tool
  dispatch alone does not make timer callbacks resume the model. Until that
  connection exists, CLI `cron_create` and `loop_wakeup` return an explicit
  unsupported-runtime error rather than leaving jobs that cannot fire.
- Runtime-only TypeScript hooks are omitted: `forceFireJob`, test-only fast
  timers, `setSkipDurableFire`, `getExitSummary`, and `hasPendingWork`.
- `stop` clears pending wakeups and the 24-hour chain origin while retaining
  cron jobs. `destroy` additionally clears jobs. `disable` is permanent for the
  scheduler instance.
- Rust prompt storage is capped at 64 KiB per cron job and 10,000 characters
  per loop wakeup, bounding total in-memory prompt storage alongside the
  50-job/one-wakeup limits. The TypeScript scheduler itself does not enforce
  the cron prompt byte cap.
- Cron timestamps use the process-local Chrono timezone; cron-parser DST
  stepping retains the documented Rust elapsed-minute behavior rather than
  JavaScript local-calendar setter behavior.

No tests were added or run for this slice.
