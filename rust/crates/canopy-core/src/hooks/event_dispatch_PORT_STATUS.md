# Hook event dispatch port status

`event_dispatch.rs` ports the private `executeHooks` orchestration path from
`packages/core/src/hooks/hookEventHandler.ts`. It asks the existing planner to
select and deduplicate host-supplied registry entries, appends matcher-selected
session hooks, switches to sequential execution when the registry plan or any
session hook requests it, applies successful sequential output to later input,
and sends results to the existing aggregator. Dispatch-level errors produce
the source fail-closed block output for `TodoCreated` and `TodoCompleted`.

## Integration

The module is exported from `hooks/mod.rs` and passes
`cargo check -p canopy-core --lib --locked`. The host should pass enabled
registry entries in source-priority order as `HookPlannerEntry` values and
implement `HookDispatchExecutor` to route JSON configs to command/HTTP/prompt
runners and typed function configs to the function runner. Supply the messages
snapshot and cancellation token from the host.

## Host work and parity notes

- Event-specific `fire...Event` payload builders, session/config access,
  telemetry, debug logging, and post-aggregation display side effects remain
  host work. The executor must convert runner-level failures into ordinary
  `HookExecutionResult` values; its `Err` path represents an orchestration
  failure and triggers the Todo fail-closed response.
- Registry entries are assumed already enabled, filtered for event, and
  source-priority sorted, matching `HookRegistry.getHooksForEvent`. Planning
  still performs matcher filtering and duplicate removal.
- Sequential prompt-expansion context is escaped and capped at 10,000 UTF-16
  units. Rust avoids splitting a non-BMP scalar at the boundary, whereas
  JavaScript slices UTF-16 code units. Sequential tool-input merging handles
  JSON objects; JavaScript object spread can also expose indexed properties
  when malformed non-object values are supplied.
- A pre-cancelled parallel dispatch synthesizes the source cancelled result
  for each hook. Sequential dispatch stops before the next hook when its token
  is cancelled. Cancellation during execution is delegated to the injected
  runner adapter.
- The session manager and planner retain their documented Rust regex and
  alias-table differences from JavaScript. Event context and input are passed
  through as caller-owned JSON rather than built in this module.

No tests were added or run for this slice.
