# Native hooks command port status

`hooks_command.rs` implements the top-level `canopy hooks` command and its
`canopy hook` alias. With no arguments it exits successfully without printing
to stdout or stderr, matching the TypeScript command's user-visible behavior.
The TypeScript handler also emits a session-bound debug message before calling
`process.exit(0)`; the native standalone dispatcher has no equivalent
session-bound debug logger, so that diagnostic is not persisted. As in the
TypeScript command, help and version are disabled for this command and
unexpected trailing arguments are rejected.

The Rust CLI already builds and runs configured hooks as part of prompt and
tool execution through `CliPromptHookHost` in `hook_host.rs`. The top-level
command does not configure or inspect them. The interactive Rust prompt loop
also provides a read-only `/hooks` browser through `tui/hooks.rs`.

## Dispatcher integration

The native dispatcher handles both spellings through
`hooks_command::handles(command)`, passes trailing arguments to `run`, and
lists `canopy hooks` plus its `hook` alias in root help.

No tests were added or run, per task instructions.
