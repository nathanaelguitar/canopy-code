# Async command hook runner parity

`async_command_runner.rs` ports the admission and detached-execution path from
`HookRunner.executeAsyncHook` and `executeCommandHookInBackground` in
`packages/core/src/hooks/hookRunner.ts`. It composes `CommandHookRunner` with
the existing `AsyncHookRegistry` and returns immediately after successful
registration with `{ continue: true }`.

## Covered behavior

- Checks registry capacity before constructing the registration, then calls
  `register` under a separate lock acquisition so its second capacity check
  catches the admission race.
- Returns the source rejection error and immediate continue output when either
  capacity check rejects the hook.
- Registers hook ID/name, event, session ID, current start time, and the
  configured timeout (using the 60-second default when the value is falsey).
- Runs the command through `CommandHookRunner` in a detached Tokio task. On
  success it appends stdout/stderr and completes the registry entry with the
  parsed output; on failure it records the error. A supervisor records task
  panics or cancellation as registry failures.
- Returns a cancelled result before registration when the supplied cancellation
  token is already cancelled.

## Integration

The module is exported from `hooks/mod.rs` and passes the integrated
`cargo check -p canopy-core --lib --locked`. The host must provide a shared
`Arc<tokio::sync::Mutex<AsyncHookRegistry>>` and call the wrapper for async
command configurations.

## Remaining differences

- The Rust wrapper has no logger, so the source's admission, start, completion,
  and failure diagnostics are not emitted.
- The registry timeout value is recorded for the existing host timeout poller;
  this wrapper does not start that timer or terminate timed-out child
  processes. `AsyncHookRegistry::check_timeouts` and process ownership remain
  host responsibilities.
- Hook IDs use the Rust registry's seven-character hexadecimal random suffix
  rather than the source's base-36 suffix.
- Tokio task panic/cancellation messages and process error strings differ from
  JavaScript `Error` formatting. Both paths mark the registered hook failed.
- Rust timeout values are converted to signed integer milliseconds for the
  registry. Non-finite values fall back to the default; large finite values
  saturate at the integer bounds.
