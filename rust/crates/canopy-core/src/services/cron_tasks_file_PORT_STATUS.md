# Durable cron task file store

Source: `packages/core/src/services/cronTasksFile.ts`.

`cron_tasks_file.rs` implements the per-project `scheduled_tasks.json` path
under the user's Canopy runtime directory, missing-file reads, corruption-failing
validation, unknown-field preservation, task/run validation, the 20-entry run
history cap, base-36 task IDs, atomic no-follow replacement, and serialized
read/modify/write updates. Updates use an in-process per-file mutex and a
cross-process lock file with the source's 15 ms retry, two-second stale
threshold, and three-second timeout. `remove_ids` keeps the source's lock-free
miss check and avoids creating a directory when no ID matches.

The session-only Rust runtime now lives in `cron_scheduler.rs`; it deliberately
does not load from or write to this store. Task management HTTP/UI routes,
archive/unarchive policy, cross-session catch-up dispatch, and durable channel
delivery are not connected to the native CLI scheduler. The write API does
not expose the TypeScript `assertCanCommit` callback; a host that can invalidate
a pending write must guard its call to `write`/`update`. The common atomic
writer provides synced temp-file replacement and replaces a final symlink
rather than following it.

`cargo check --workspace --locked` passes with this module exported. No tests
were added or run for this slice.
