# Hook helper parity status

This module ports two pure helpers:

- `build_context_usage` mirrors `buildContextUsage` from
  `packages/core/src/hooks/context-usage.ts`. It returns no value when the
  context limit or input-token count is absent, non-finite, zero, or negative;
  otherwise it returns the ratio and original measurements in the existing
  `ContextUsageData` shape.
- `detect_todo_changes` mirrors `detectTodoChanges` from
  `packages/core/src/hooks/types.ts`, using the shared Rust todo item/status
  types. It reports newly seen IDs and transitions from a non-completed state
  to completed, preserving new-list order and JavaScript `Map` duplicate-ID
  behavior.

These are standalone helpers and do not wire themselves into the live CLI hook
lifecycle. The TypeScript helper accepts JavaScript numbers; the Rust API uses
`f64` to preserve finite-value checks and fractional measurements.
