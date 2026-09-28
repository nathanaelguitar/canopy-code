# Native hook dispatcher adapter port status

`native_dispatch_executor.rs` implements `HookDispatchExecutor` for the native
command, HTTP, function, and prompt runners. It accepts already-configured
runner instances. Command configs with `async: true` are registered with a
shared Tokio `AsyncHookRegistry`, return an immediate `{ continue: true }`
result, and run the command in a detached task that completes or fails the
registry entry through `AsyncCommandHookRunner`. Session function hooks use
their existing typed callbacks.
Registry JSON function hooks use the injected `JsonFunctionHookResolver`; an
unresolved callback follows the function runner's normal
`Invalid callback: expected a function` failure path.

## Integration

The module is exported from `hooks/mod.rs` and passes both
`cargo fmt --all -- --check` and `cargo check --workspace --locked`. Construct
the adapter with the host's configured command, HTTP, function, and prompt
runners plus a shared `Arc<tokio::sync::Mutex<AsyncHookRegistry>>`. The host
owns runner configuration: command environment/shell context, HTTP URL
allowlist and private-network policy,
prompt model executor, JSON function callback lookup, and the async registry's
timeout polling, process termination, and pending-output delivery.

## Remaining differences

- The shared aggregator result contains only `success`, `output`, `error`,
  and `duration`. Command stdout/stderr/exit code, runner outcome enums, raw
  hook configs, and async status metadata are not carried through this adapter;
  the async registry separately retains its stdout/stderr/output queues.
- Async command admission rejection returns the source non-blocking continue
  output and the concurrency error. The registry owns timeout bookkeeping,
  but this adapter does not retain child-process handles for a host timeout
  check to terminate; command execution also retains its own configured
  timeout and cancellation behavior.
- Registry JSON function callback resolution is a synchronous host seam.
  Missing callbacks are projected through `FunctionHookRunner` as a normal
  non-blocking function-hook error.
- Invalid command/HTTP JSON configs and unknown hook types become ordinary
  failed runner results. Their Rust error wording is not identical to the
  TypeScript outer `HookRunner` exception wrapper.
- `AsyncCommandHookRunner` observes detached-task panic/cancellation and marks
  the registry entry failed. The host still owns timeout polling, child
  termination, and pending-output delivery.

No tests were added or run for this slice.
