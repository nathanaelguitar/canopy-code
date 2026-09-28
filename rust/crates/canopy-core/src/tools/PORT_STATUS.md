# Tool port status

`list_directory.rs` ports the direct-entry listing tool in
`packages/core/src/tools/ls.ts`. It has a separate contract from the recursive
`getFolderStructure` helper, now ported in `utils/folder_structure.rs` and
exported from `utils/mod.rs`: direct listing sorts directories first, filters
ignored entries, and caps direct entries at 100, while the tree uses a 20-item
combined recursive budget and renders ignored folders with `...`. No native
caller uses the tree helper yet.

Native `glob` and `grep` results carry their returned file paths as host-only
tool metadata. The agent runtime preserves these paths through output
finalization and passes them to the post-tool hook for conditional-rule lookup;
they are not counted as model-visible output.

`image_view.rs` implements the native `zoom_image` crop/decode/resize path;
its bounded memory behavior, CLI/ACP wiring, and remaining Sharp parity gaps
are tracked in `image_view_PORT_STATUS.md`.

`cron.rs` defines native create/list/delete/loop-wakeup schemas and dispatch
against the session-only runtime in `services/cron_scheduler.rs`. The native
CLI registers and dispatches these tools while cron is enabled. Durable task
creation is explicitly rejected until native file ownership and reload are
wired. The CLI has not started the scheduler timer or connected its fire
callback to the live model loop; see
`services/cron_scheduler_PORT_STATUS.md`.
