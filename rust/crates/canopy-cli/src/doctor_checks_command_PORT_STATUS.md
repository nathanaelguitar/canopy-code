# General `/doctor` checks port status

The general interactive `/doctor` command is wired into the native `canopy run`
prompt loop for both fullscreen and line-oriented sessions. It writes one
formatted result to the existing doctor transcript row or stdout. `/doctor memory`
continues through `memory_diagnostics_command` unchanged, and
`/doctor rollback` keeps its existing update rollback behavior.

The command receives only state available after an interactive runtime starts:
the selected provider and resolved model, a credential-present boolean and
optional environment variable name, successful settings/client initialization,
the active provider tool declaration count, and configured MCP server names
with per-session connection statuses from that session's MCP manager. Credential
values are never passed to the diagnostics. MCP entries without a connection
snapshot warn as unavailable; they are not treated as connected or disabled.

Auth validation checks the active native provider kind and credential presence.
For Anthropic, it also requires a nonempty `baseUrl` on the selected model
provider entry, or `ANTHROPIC_BASE_URL` when no matching entry exists, matching
the interactive TypeScript validator. This is configuration validation only;
it does not contact the provider or prove that a credential will be accepted.
ChatGPT OAuth and Vertex AI credential validation are unavailable because those
auth flows are not selected by the current native interactive runtime. The
retired Canopy OAuth method reports its configured deprecation failure.

Node.js and npm explicitly report `not_applicable`; the runtime row reports the
native Rust executable. The platform row includes OS, architecture, and release
when the release probe succeeds. It uses `uname -r` on Unix and `cmd.exe` with
`/C ver` on Windows, with a five-second timeout and 1 KiB stdout cap; probe
failure is reported as a warning. Ripgrep is not a runtime dependency because
`grep_search` is implemented in Rust. Git uses the same timeout and output cap;
no network requests are made.

Remaining parity gaps: there is no shared `ToolRegistry`, so tool count is the
active declaration snapshot. Disabled or skipped MCP servers cannot currently
be distinguished from other servers without a live session connection and are
reported as unavailable. The separate `/doctor cpu-profile` subcommand is not
implemented.
