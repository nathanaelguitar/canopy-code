# Monitor registry Rust port status

`monitor_registry.rs` ports the in-memory monitor task registry from
`packages/core/src/services/monitorRegistry.ts`.

Implemented:

- Running, completed, failed, and cancelled entry states with the shared task
  envelope and monitor-specific fields.
- The 16-running-monitor ceiling and 128 retained-terminal-entry limit.
- Registration and settlement callbacks, shared entry handles, owner-routed
  notification callbacks, owner lifecycle wake callbacks, and reset behavior.
- Event counting, UTF-16-unit line truncation, display-control sanitization,
  XML escaping, terminal notification shaping, and idempotent terminal
  notification delivery.
- Per-entry idle timeout workers that are re-armed on events and stopped when
  an entry settles or the registry resets.
- Natural completion/failure, visible and silent cancellation, owner-scoped
  cancellation, and abort-all behavior.
- Reserved output path generation with the source filename-component rule.

Integration boundaries and parity limits:

- Process spawning, stdout/stderr buffering, throttling, and the Monitor tool's
  spawn/exit wiring remain outside this service slice. Callers register a
  `CancellationToken` and invoke `emit_event`, `complete`, or `fail`.
- Rust callers pass `todo_work_chain_id` in the registration; this module has
  no ambient equivalent of the TypeScript async-local prompt context.
- Idle timers use one small standard-library worker thread per registered
  monitor because registry entry points are synchronous. The TypeScript
  event-loop timer and synchronous `AbortSignal` listener ordering are not
  reproduced exactly; cancellation tokens wake Rust async listeners instead.
- Event truncation counts UTF-16 code units. If the cut splits a surrogate
  pair, Rust emits the replacement character because Rust strings cannot
  contain the source JavaScript's unpaired surrogate.
