# Hierarchical memory discovery port status

`discovery.rs` ports the instruction-file portion of
`packages/core/src/utils/memoryDiscovery.ts`:

- It uses the configured context filename list, preserves include-directory,
  global, upward-project, local-slot, and extension-file ordering, and keeps
  directory work in batches of 10 and root-file reads in batches of 20.
- It supports explicit-only discovery, trusted upward scans, the fixed
  `.canopy/CANOPY.local.md` slot, scope classification (`user`, `project`,
  `local`, `extension`), load reasons, UTF-8 replacement, and CWD-relative
  display paths.
- Project root detection reuses `utils::project_root::find_project_root`, which
  accepts `.git` directories and regular worktree marker files while ignoring
  symlink markers.
- The server wrapper loads global and trusted project baseline rules after
  instruction content, returns `rule_count` and conditional rules, and skips
  all rule discovery in explicit-only mode. Rule exclusions default to empty;
  invalid exclude patterns propagate as `Err` when the source would compile
  them.
- Tree imports preserve inline replacement, per-chain cycle deduplication,
  source order, path containment checks, the five-level depth cap, and
  post-order include notifications. Flat imports preserve unique-file output,
  reverse traversal, shallower-route re-expansion, and one notification per
  emitted imported file.
- Host notifications use `MemoryFileLoadedNotification` and an owned async
  callback. Callback errors are best-effort and go to the optional
  `on_notification_failure` seam; the module does not depend on hook dispatch.

The Rust request takes home and global Canopy paths explicitly, with a
process-based constructor for the default environment. The source's
`FileDiscoveryService` parameter is not used by `memoryDiscovery.ts` and is not
required here.

The native `canopy run` and ACP paths load instruction files and baseline
rules at startup and append them to the user system instruction; both use the
current workspace trust setting. ACP and CLI attach returned conditional rules
to their workspace executor. After successful filesystem-tool responses, the
post-tool hook matches paths from tool arguments and `glob`/`grep` results, then
adds escaped reminder context after output truncation. The CLI does not yet
pass extension context files or rule exclusions, or deliver
instruction-loaded hooks.

Per-prompt auto-memory recall in native `run` and ACP now supplies the core
resolver with an OpenAI-compatible JSON selector. It uses a `fastModel` only
when the active adapter can resolve it on the same OpenAI endpoint and
credential route; otherwise the session model is used. Selector errors retain
the deterministic fallback, and ACP cancellation reaches the side query.
Callers supply up to 16 distinct, most-recent function-call names from prepared
API history. Recall telemetry is not yet recorded. Cross-auth and model-specific
endpoint, credential, and custom-header routing remain outside this adapter.

Remaining parser gap: import code-region detection handles fenced, indented,
and inline backtick code, but does not use the full `marked` lexer. The module
is exported as `memory::discovery` and passes `cargo check -p canopy-cli --locked`.
