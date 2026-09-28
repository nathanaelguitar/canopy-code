# Durable cron scheduler owner lock

Source: `packages/core/src/services/cronTasksLock.ts`.

`cron_tasks_lock.rs` ports the per-project owner lock file, exclusive-create
acquisition, same-process/session/lock-ID idempotence, dead or malformed lock
takeover by atomic rename, moved-lock liveness recheck and hard-link restore,
two acquisition attempts, and best-effort ownership-checked release. Unix
process checks use signal 0; Windows uses a direct `tasklist` invocation.

This selects the one session allowed to fire shared durable tasks. It is not
wired to a Rust cron scheduler yet. Other operating systems without a process
probe return an explicit error. No tests were added or run for this slice.
