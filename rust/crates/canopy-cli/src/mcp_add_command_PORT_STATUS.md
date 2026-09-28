# MCP add command port status

`mcp_add_command::run(args: &[String]) -> Result<(), String>` implements the
standalone `canopy mcp add` command. The dispatcher and top-level help now
route and list it; the module handles its own detailed help output.

## Implemented behavior

- Accepts `<name> <commandOrUrl> [args...]`, scope (`user` or `project`),
  explicit `stdio`/`sse`/`http` transport, and HTTP(S)-to-HTTP / otherwise-
  stdio auto-detection. SSE is selected explicitly.
- Preserves unknown options and trailing positionals as stdio command
  arguments, including arguments after `--`.
- Maps repeated environment and header options, timeout, trust, description,
  include/exclude tool filters, and all OAuth options exposed by the TypeScript
  command. OAuth is rejected for stdio. The OAuth object uses the camel-case
  settings shape accepted by `McpOAuthProviderConfig`.
- Checks project scope against the home-directory alias and workspace trust
  before directly reading the project settings file. It skips workspace
  settings and environment loading during this preflight.
- Replaces only the named entry in `mcpServers`, retains unrelated JSONC,
  rejects malformed/non-object server maps, bounds settings reads to 16 MiB,
  rejects symlink/non-regular settings files, and commits with the shared
  atomic no-follow writer.
- Reports added/updated state without printing environment, header, or OAuth
  secret values. It does not connect to, probe, or spawn the configured server.

## Reused APIs and remaining differences

The module reuses `load_settings` / `LoadSettingsOptions` for scope paths and
trust preflight, `parse_jsonc_object` and `update_jsonc_content` for targeted
JSONC mutation, `atomic_write_file` with `SymlinkPolicy::NoFollow` for the
commit, and `strip_terminal_control_sequences` for displayed values and errors.
The persisted OAuth field names match the core `McpOAuthProviderConfig` serde
shape; serialization is assembled as JSON so absent TypeScript `undefined`
fields stay omitted.

The TypeScript command exposes no flags for the core OAuth fields
`audiences`, `tokenParamName`, or `registrationUrl`, so this command cannot set
them. The Rust argument parser accepts one value per repeated
`--include-tools`, `--exclude-tools`, and `--oauth-scopes` option; scopes are
comma-split as in TypeScript, while tool filters are persisted as the supplied
strings. Settings files over 16 MiB and symlink settings paths are rejected
for safe bounded mutation.
