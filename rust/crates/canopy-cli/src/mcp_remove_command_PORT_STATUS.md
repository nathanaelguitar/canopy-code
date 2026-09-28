# Native MCP remove command port status

`mcp_remove_command.rs` exposes `pub fn run(args: &[String]) -> Result<(), String>` for `canopy mcp remove <name> [--scope user|project]`. The scope defaults to `user`; `-s` is accepted as an alias for `--scope`.

## Behavior

- Loads settings through the core config loader. For project scope, it first skips project settings, checks the settings paths and workspace trust, and only then reads the project settings file. This avoids loading or migrating an untrusted project's settings before the trust check.
- Removes only the selected key from `mcpServers`. It uses the core JSONC subtree replacement helper and atomic file writer because `update_setting_value` deep-merges objects and cannot delete one member. Other server entries, root keys, and retained JSONC formatting/comments are preserved.
- If the server is absent, prints the TypeScript-compatible not-found message and does not touch OAuth tokens.
- After a successful settings write, best-effort deletes the server's credentials through `ConfiguredTokenStorage`. Token cleanup failures do not fail the removal.
- Does not start or connect to an MCP server.

## Integration

- The top-level dispatcher and help now expose `canopy mcp remove`.
- No Cargo dependencies or manifest changes were required.
- No tests or Cargo commands were run for this port, per task instructions.
