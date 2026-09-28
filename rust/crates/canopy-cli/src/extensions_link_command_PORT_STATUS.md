# Native extension link port status

`extensions_link_command.rs` implements `canopy extensions link <path>` for a
local native Canopy extension directory or Agent Plugin directory. It checks
workspace trust, resolves the source to a canonical live path, validates the
manifest through the bounded installed-extension inventory, and displays a
terminal-safe consent summary for the extension's MCP servers, commands,
skills, subagents, and context files. Consent requires an explicit `y`/`yes`
when stdin is non-interactive; an empty response defaults to yes only on a
terminal.

Before consent, native Canopy manifests are checked for valid setting env-var
names. After consent, declared settings are prompted in TypeScript order:
sensitive values first, then plain values. Plain inputs are read visibly;
sensitive input uses hidden terminal input and is never echoed by the command.
Links use user-scope storage. Plain values are written to a staged `.env` with
the shared atomic no-follow writer. Sensitive values are saved to the macOS
keychain when available (unless file storage is forced), or the existing
encrypted file backend otherwise. The staged selector is atomically written
with mode 0600; secret snapshots and individual keys are removed best-effort if
settings staging or link commit fails. Agent Plugin manifests currently declare
no Canopy settings in the Rust manifest adapter, so they do not prompt for
settings.

The command writes a `type: link` install metadata sidecar and settings into a
staging slot, then commits the linked slot and default user activation through
the shared extension transaction journal. Runtime inventory follows the
metadata source path, so edits to the source directory remain live. Failed
staging is removed.

## Remaining parity gaps

- Links currently accept native `canopy-extension.json` and supported Agent
  Plugin manifests. Gemini, Claude, and Qoder source conversion is not applied
  because converting those formats would stop the link from reflecting source
  edits in place.
- At session startup, the runtime reads declared settings from the linked
  extension's user `.env` and secret selector/backend, then reads project
  `.env` and project-scoped secrets. Project values override user values.
  Explicit server `env` entries override stored values, and inherited process
  variables take precedence as in the TypeScript runtime. Values are added only
  to the matching extension's stdio MCP child configuration; remote and SDK
  transports do not receive child-process environment settings. If a settings
  file or secret bundle cannot be loaded, session discovery skips that server;
  `mcp list` reports it as disconnected. The list probe uses the same
  resolution and precedence before spawning an extension stdio server.
- Settings declarations are read from native `canopy-extension.json` files
  with a 1 MiB manifest limit. Agent Plugin settings are not supported because
  the current Agent Plugin adapter exposes no settings declarations.
- Consent reports resource names from the native inventory; it does not render
  the full MCP command/URL details shown by the TypeScript consent screen.
- Runtime extension hosts do not refresh automatically after a separate CLI
  link operation; reload or restart the host to see the new link.

No tests or Cargo commands were run for the runtime-injection slice. `rustfmt`
and whitespace checks passed.
