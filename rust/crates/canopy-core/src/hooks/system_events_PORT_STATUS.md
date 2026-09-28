# Hook system event methods port status

`system_events.rs` adds all 22 public `fire...Event` methods from
`packages/core/src/hooks/hookSystem.ts` on the native `HookSystem`. Each method
uses the corresponding `HookEventInputBuilder` method and dispatches through
`HookSystem::execute_event`.

The five methods that return full `AggregatedHookResult` in TypeScript keep
that result shape in Rust: Stop, MessageDisplay, StopFailure, TodoCreated, and
TodoCompleted. Other methods return `Option<SpecificHookOutput>`, preserving
the event-specific output kind and projected JSON, including `None` when no
hook produced final output.

## Event coverage

All 22 wrappers are present: UserPromptSubmit, InstructionsLoaded,
UserPromptExpansion, Stop, MessageDisplay, SessionStart, SessionEnd,
SessionDelete, PreToolUse, PostToolUse, PostToolUseFailure, PostToolBatch,
PreCompact, Notification, SubagentStart, SubagentStop, StopFailure,
PostCompact, PermissionRequest, PermissionDenied, TodoCreated, and
TodoCompleted.

## Host wiring and parity gaps

- The host supplies `HookBaseInput` for each call. Stop and SubagentStop also
  require a fresh `HookEventSnapshots` value from the host.
- `HookEventExecutionOptions` carries the message snapshot and cancellation
  token per call; the TypeScript shared `MessagesProvider` must be adapted by
  the host into these event-local values.
- JSON objects and arrays use `serde_json::Map` and `Value` at the Rust API
  boundary. Permission modes, event reasons, triggers, and todo phases are
  strings because this crate does not define all of the corresponding
  TypeScript domain enums.
- TypeScript convenience outputs are runtime classes with helper methods. Rust
  returns `SpecificHookOutput { kind, value }`; callers use the kind and JSON
  fields directly. The event aggregator applies its existing field projection
  and PostToolUse defaults before these wrappers return.
- This module adds event methods but does not wire application lifecycle call
  sites or automatically capture host snapshots/messages.

## Export needed

Add this line to `hooks/mod.rs`:

```rust
pub mod system_events;
```
