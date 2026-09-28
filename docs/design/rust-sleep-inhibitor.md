# Rust sleep inhibition slice

The Rust agent runtime will match Canopy's default-on `preventSystemSleep`
behavior while it is processing a prompt. The runtime owns one process-wide
inhibitor with reference-counted RAII handles, so overlapping sessions share a
single OS process and every return path releases its own reference.

The platform commands match `packages/core/src/services/sleepInhibitor.ts`:

- macOS runs `caffeinate -is`.
- Linux runs `systemd-inhibit` in block mode, but skips headless SSH sessions.
- Windows uses PowerShell and `SetThreadExecutionState`.
- Unsupported platforms and spawn failures remain best-effort no-ops.

The native CLI and ACP read the existing `general.preventSystemSleep` setting
across their loaded settings scopes and default it to `true`. The runtime also
exposes a builder override for other hosts. The process is released when the
last active runtime prompt finishes or is cancelled.
