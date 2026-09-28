# Native hook system façade port status

`system.rs` connects host-created `HookEventPayload`s to the configured
`HookRegistry`, `HookEventDispatcher`, `SessionHooksManager`, and
`NativeDispatchExecutor`. Event dispatch snapshots enabled/source-priority
registry entries and clones session state under a short read lock, then drops
the lock before awaiting hook execution. The host supplies the messages
snapshot and cancellation token per event.

## Integration

Add `pub mod system;` to `rust/crates/canopy-core/src/hooks/mod.rs`. Construct
`HookSystem` with an initialized registry, session manager, and shared native
runner adapter. Use `set_hook_enabled` and `reload_configured_hooks` for basic
registry updates. Use `with_session_manager_mut` for short synchronous
session-hook registration/removal operations.

## Host responsibilities and gaps

- The façade accepts ready-to-use registry entries. Settings reads, source
  selection, trusted-folder policy, extension activation, schema validation,
  and error feedback remain host work before registry initialization/reload.
- The host builds `HookEventPayload` inputs and matcher contexts through
  `event_inputs.rs`, obtains message snapshots, configures command/HTTP/prompt
  runners, resolves JSON function callbacks, and supplies cancellation.
- Application event wiring, status messages, common-output side effects,
  debug logs, and telemetry are not implemented here.
- Registry and session state share one `RwLock` for consistent snapshots.
  The façade releases it before awaiting dispatch. Session mutation closures
  run synchronously under the write lock and must not block or re-enter the
  façade.

The module was formatted; no tests or compilation were run.
