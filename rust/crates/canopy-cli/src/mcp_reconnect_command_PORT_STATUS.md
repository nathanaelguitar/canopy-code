# MCP reconnect CLI port status

`mcp_reconnect_command.rs` provides the standalone `reconnect` handler and
`handles` predicate. The top-level CLI dispatcher and help now expose it.

## Behavior and safety

- A named server reconnects only when explicitly named. `--all` (`-a`) walks
  the configured server map sequentially. Supplying both a name and `--all`,
  or neither, is an error. An unknown named server is reported without
  starting a transport. In `--all`, each server reports its own result and a
  failed entry does not prevent later entries from being attempted.
- Configuration is assembled from merged settings, project `.mcp.json`, and
  active local extension MCP sources. Workspace/system settings override
  project entries; active extensions fill only missing names. Extension
  activation is filtered through workspace trust, safe mode, and bare mode.
- No transport starts during config loading. Before connecting, the command
  requires a trusted workspace and checks project/workspace scopes against the
  1 MiB bounded approval snapshot and current config hash. Missing, stale,
  rejected, unreadable, or malformed approval data blocks that server without
  connecting. Non-gated user/system and active extension servers use the
  normal native workspace policy.
- Each selected server gets an isolated `McpCliWorkspace` and session. The
  existing native session path initializes MCP and performs tool discovery
  (including the runtime's prompt/resource discovery pass); discovery errors
  become per-server failures. The session is stopped and the workspace pool
  is drained on success, discovery failure, or timeout.
- A configured `mcp.serverCommand` contributes the synthetic server name
  `mcp`, matching the Rust host's TypeScript `Config` behavior. It overrides a
  same-named configured server, uses the current workspace as `cwd`, and is
  included by named reconnect and `--all`. It is admitted only for trusted
  workspaces outside safe and bare modes. The command is split into argv
  without a shell, with quoting and environment-variable expansion; shell
  operators, comments, globs, malformed quoting, and oversized input are
  rejected. Existing five-second connection/discovery and bounded shutdown
  limits apply to its process.
- Connection plus discovery is bounded to 5 seconds per server. Cleanup is
  bounded to 3 seconds. `--all` considers at most 64 servers; excess entries
  are skipped with a diagnostic. The CLI process exits after this one-shot
  operation, so a successful connection is not retained by an interactive
  session.

## Remaining parity differences

- The TypeScript command constructs a `Config`/`ToolRegistry` and asks it to
  rediscover tools for a server. Rust uses one short-lived native MCP workspace
  per server. The protocol operation is similar, but no connection is retained
  in the calling CLI or another active session.
- Rust blocks all MCP connections when the workspace is untrusted, following
  `McpCliWorkspace` policy. This is stricter than the TypeScript reconnect
  wrapper, which explicitly passes workspace trust to extension activation
  while relying on settings and pending-approval filtering for server config.
- Automatic browser OAuth is disabled. Existing credentials can be resolved
  through the native workspace, but a server requiring a new interactive OAuth
  grant fails and must be authorized through the existing interactive flow.
- The TypeScript reconnect wrapper's initial name enumeration does not list a
  `serverCommand`-only server, even though the underlying `Config` tool registry
  injects it as `mcp`. Rust includes the dynamic server in the explicit named
  and `--all` inventories.
- Native extension inventory and reconnect enumeration are bounded and local.
  The reconnect command caps `--all` at 64 servers, so large configurations
  can be partially processed even though TypeScript iterates the full map.
