# MCP approve/reject CLI port status

`mcp_approval_command.rs` implements the explicit `canopy mcp approve` and
`canopy mcp reject` commands. The top-level Rust CLI dispatcher and help now
route and list both operations.

## Behavior

- A server name records one decision. `--all` records the decision for every
  approval-requiring server visible in the current merged configuration,
  including servers that already have an approval or rejection. This matches
  the TypeScript command's target selection. With no name and no `--all`, the
  command prints `Specify a server name or pass --all.` and makes no change.
- Only configs with `scope: project` or `scope: workspace` are eligible.
  User/default, system, and extension servers are excluded. The command does
  not connect to, spawn, or otherwise execute any MCP server.
- Rust settings loading filters workspace settings for an untrusted workspace.
  Project `.mcp.json` remains available for an explicit decision, matching the
  TypeScript command's settings-plus-project merge. Approval itself is the
  explicit user action; there is no automatic prompt or implicit approval.
- Decisions are bound to the current canonical server-config hash and resolved
  workspace path. A config edit changes the hash and returns the server to
  pending. Project `.mcp.json` and workspace settings use the shared MCP
  source merger; `mcp.allowed` and `mcp.excluded` do not affect this command.
- The existing approval file is read with a 1 MiB cap. Missing files start as
  an empty approval map. Corrupt, unreadable, oversized, or structurally
  invalid data fails closed without overwriting it. Updated JSON is also capped
  at 1 MiB and written atomically with mode `0600`.
- `--all` updates the selected records in memory and performs one atomic write,
  so a write failure does not report partially successful status changes.
  Success messages are printed only after the write succeeds. Names in output
  have terminal control sequences stripped.

## Remaining differences and limits

- The TypeScript command writes once per server and reports save errors to
  stderr while its handler continues. Rust performs one atomic batch write and
  returns a CLI error on failure; this avoids claiming a decision was saved
  when persistence failed.
- Rust refuses to mutate a malformed or over-limit approvals file. The
  TypeScript loader warns and exposes an empty in-memory map, which can lead a
  later save to replace malformed contents.
- The shared Rust project MCP loader currently reads `.mcp.json` without an
  explicit byte cap. Approval-file reads and writes are bounded, but an
  oversized project config is still a configuration-loader limit to address.
