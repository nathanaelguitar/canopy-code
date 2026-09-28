# MCP list CLI port status

`canopy mcp list` is wired as a read-only inventory with sequential live probes
for servers that are eligible to connect. It displays each server's configured
HTTP URL, SSE URL, or stdio command and arguments, followed by `Connected`,
`Disconnected`, `Pending approval`, or `Rejected`. Config-derived display text
has terminal control sequences stripped. An empty inventory prints
`No MCP servers configured.`

## Sources and precedence

The list is assembled from the same merged settings snapshot used by the CLI,
the workspace `.mcp.json`, and active local extension MCP sources. The merge
order is:

1. User/default settings (`scope` unset).
2. Project `.mcp.json` servers.
3. Workspace and system settings.
4. Active extension servers, only for names that have no configured server.

After those sources are merged, a configured `mcp.serverCommand` adds the
synthetic `mcp` stdio server and overrides any same-named entry. It is enabled
only for trusted workspaces outside safe and bare modes. The command uses the
workspace as its cwd and is split into bounded argv without starting a shell;
environment variables and shell quoting are supported, while operators,
comments, globs, malformed quoting, and oversized values are rejected.

Project JSONC is parsed by the shared Rust MCP settings loader. Project
servers are tagged with `scope: project`; workspace/system settings retain
their scope. This command accepts no MCP config argument. It does not apply
`mcp.allowed` or `mcp.excluded`, matching the TypeScript list command.

Active extension sources come from the native local extension inventory, with
workspace trust, safe mode, and bare mode gates applied. The inventory has
bounded entry/config limits; if those limits are reached, diagnostics go to
stderr and remaining extension sources may be omitted.

## Approval and probe behavior

Project- and workspace-scoped servers are checked against `mcpApprovals.json`
(or `CANOPY_CODE_MCP_APPROVALS_PATH`). The approval record must match the
resolved workspace path, server name, and current config hash, and have
`approved` status. Missing, rejected, stale, malformed, unreadable, or
over-limit approval data results in a yellow status and no connection attempt.
The approvals file read is capped at 1 MiB. Extension, user/default, and
system servers are not approval-gated.

Each eligible server gets a fresh native MCP client. The probe connects,
completes protocol initialization, sends `ping`, then closes the transport.
An outer 5-second timeout bounds startup, OAuth credential resolution, and
the ping. The native request timeout is also set to 5 seconds. Teardown is
bounded to 1 second. Probes run sequentially. Failures show `Disconnected`; a
timeout shows `Disconnected (timed out after 5000ms)`.

## Remaining parity differences

- The TypeScript command uses the MCP SDK's `Client` and `createTransport`;
  Rust uses `McpClientRuntime` and the native transport factory. Provider and
  transport edge behavior can therefore differ even though both paths perform
  initialization and ping.
- Rust's active extension inventory is local and bounded. It can omit sources
  beyond its inventory limits or extensions the TypeScript manager resolves
  through runtime-only state.
- For malformed stdio configs, Rust displays only string arguments; JavaScript
  `join` may coerce other JSON values to strings.
- The TypeScript `mcp list` wrapper enumerates configured servers directly and
  does not inject `mcp.serverCommand`. Rust includes the synthetic server,
  following the shared TypeScript `Config` transport semantics used by normal
  tool discovery.
- An MCP server's configured native timeout can fail before the command's
  outer 5-second limit. The displayed timeout duration remains the outer limit
  (`5000ms`), while the TypeScript status reflects the timeout error text it
  receives from its SDK wrapper.
