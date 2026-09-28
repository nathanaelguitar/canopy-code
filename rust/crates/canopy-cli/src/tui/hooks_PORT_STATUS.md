# Native interactive hooks browser port status

`tui/hooks.rs` implements the read-only native TUI equivalent of
`packages/cli/src/ui/commands/hooksCommand.ts` and
`packages/cli/src/ui/components/hooks/**`. It renders the full hook event list,
per-event counts and summaries, matcher groups for matcher-aware events,
configured handler rows, and a handler detail view. Up/Down or `j`/`k` move the
selection, Enter descends, Escape backs out, and `q` closes or backs out.
PageUp/PageDown scroll long views. The screen does not mutate hook settings or
runtime state.

The model is built from `HookRegistry` and `SessionHooksManager` snapshots.
`hook_host::load_registry_for_hooks_ui` loads configured sources for the
browser even when the runtime host is absent because all hooks are disabled or
only non-runtime events are configured; it does not initialize or execute any
hook runners.
Registry sources are labeled as Local Settings, User Settings, System
Settings, Extensions, or Session (temporary). Extension names and paths are
shown when those fields are retained on the hook config; the registry currently
does not preserve extension display metadata as a first-class field. Session
function hooks are rendered from their descriptive fields without exposing
their callback.

When the caller passes `disable_all_hooks`, the screen shows the disabled
message and configured-hook count and accepts only close keys. Event details
include a summary of supported inputs and exit-code behavior. Invalid or
unknown hook config kinds remain visible with their available name and source.

## Integration and remaining parity gaps

`ChatTerminal::show_hooks_dialog` accepts `HooksDialogModel::from_snapshots`;
the hook host exposes a read-only session snapshot, while the prompt setup
loads a display registry from configured user, project, and active extension
sources. Slash completion, `/help`, and the interactive prompt dispatcher now
route `/hooks` to this view. When no fullscreen terminal is available, the
command prints a concise text summary.

The TypeScript screen reads raw user/workspace settings and active extension
manifests directly. The native view reads the validated active hook registry,
so invalid definitions omitted by hook loading are not displayed; source paths
or extension labels not retained by the registry cannot be recovered here.
Most explanatory text is ported in English, but it is not connected to the
TypeScript translation catalog. The native view also adds runtime-disabled
state and sequential-execution labels when available.

No tests were added or run. Rust formatting and diff checks are the requested
verification scope for this slice.
