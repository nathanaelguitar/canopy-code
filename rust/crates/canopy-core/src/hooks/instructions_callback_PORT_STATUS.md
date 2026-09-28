# InstructionsLoaded callback port status

`instructions_callback.rs` ports `createInstructionsLoadedCallback` from
`packages/core/src/hooks/instructionsLoadedCallback.ts`.

`InstructionsLoadedNotification` mirrors the five TypeScript fields. The
factory accepts a resolver that returns the current optional `HookSystem` for
each notification. The callback checks enabled configured registry entries
and all session hooks for `InstructionsLoaded`, returns immediately when no
hook system or no matching hooks exist, and otherwise calls
`fire_instructions_loaded_event`. It intentionally discards the optional hook
output and returns unit.

The Rust callback receives `HookBaseInput` and
`HookEventExecutionOptions` (messages and cancellation) explicitly from the
host. Memory type and load reason remain strings because their TypeScript
unions are not currently defined as shared Rust types. Registry/session
presence checks use snapshots; application memory-discovery wiring remains a
host responsibility.

## Export needed

Add this line to `hooks/mod.rs`:

```rust
pub mod instructions_callback;
```
