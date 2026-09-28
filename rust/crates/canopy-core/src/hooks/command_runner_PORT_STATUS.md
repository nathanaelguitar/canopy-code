# Command hook runner port status

`command_runner.rs` ports the synchronous command invocation path from
`packages/core/src/hooks/hookRunner.ts`. It includes shell selection and
project-directory expansion, sanitized environment layering, hook-input JSON
on stdin, bounded stdout/stderr capture, timeout and cancellation handling,
and the source's structured/plain-text output interpretation.

## Integration

The module is exported from `rust/crates/canopy-core/src/hooks/mod.rs`.
`cargo fmt --all -- --check` and `cargo check --workspace --locked` pass with
it included. The host supplies shell-context values from the source runtime's
`getShellContextEnvVars()` through `CommandHookRunner::shell_context_environment`.

## Remaining differences

- The async-command wrapper is in the adjacent `async_command_runner.rs`
  module. The host still needs to export that module and construct it with a
  shared `Arc<tokio::sync::Mutex<AsyncHookRegistry>>`; see its parity notes.
- JavaScript's AsyncLocalStorage shell context is not available in Rust. The
  caller injects those environment values; process environment and hook-config
  environment still follow the source precedence.
- Output retention is capped at 1 MiB of bytes per stream and decoded with
  UTF-8 replacement. Node's implementation compares JavaScript string length
  while slicing incoming buffers, so non-ASCII boundary behavior can differ.
- Unix termination sends SIGTERM and escalates to SIGKILL after two seconds if
  the child remains alive. The source's escalation guard checks
  `child.killed`, which can mean a signal was sent rather than that the child
  exited. Non-Unix Rust termination uses Tokio's immediate hard kill.
- Windows Git Bash discovery checks PATH but does not search the extra common
  installation directories used by `shell-utils.ts`. Hosts can inject the
  resolved global shell configuration to preserve that behavior.
- Shell argument escaping follows the common POSIX single-quote form and
  matches PowerShell/cmd quoting for the project-directory substitution, but
  it is not a full port of the `shell-quote` package's edge-case rules.
- Rust cancellation uses `CancellationToken`; it does not reproduce
  AbortSignal listener ordering. Spawn and wait errors are returned as strings,
  so platform-specific error formatting differs from Node's `Error` objects.
