# Native `@ext:` and MCP reference status

`at_resource_references.rs` provides a host-neutral resolver for the non-file
prompt references handled by the TypeScript CLI `atCommandProcessor.ts` and
`mcp-server-mention.ts`.

## Resolver behavior

- `@ext:<name>` matches only the active local extensions supplied by the host,
  case-insensitively by extension name or config name. It returns an attributed
  context part with description, capability names, and bounded context-file
  content. Aggregate injected extension context is capped at 200,000 UTF-16
  units, each file at 50,000 units, and each extension at 64 context files.
  Extension roots and context files are canonicalized; symlinked context files
  outside their extension directory are skipped.
- `@mcp:<server>` matches only configured server names supplied by the host,
  case-insensitively. It returns the advisory server context with counts from
  the current prompt and resource registries.
- `@<server>:<uri>` uses longest configured-server prefix matching, then
  requires the exact `(server, URI)` pair in the current resource registry and
  reads through an already-connected session manager. It uses the shared MCP
  content formatter and its text/blob limits.
- Unrecognized or unavailable references return diagnostics and no parts, so a
  host can leave the original token as literal prompt text. The service does
  not discover servers, connect, install extensions, or fetch remote extension
  files.

## Host wiring still required

`canopy run` calls the resolver for one-shot prompts, clean `--resume` prompts,
and interactive line/TUI turns after opening the session MCP manager. It
supplies configured server names and prompt/resource registries rebuilt from
that session's successful discovery report. Resource reads therefore use the
same already-connected manager as model tool calls. Resolved parts append after
the retained query and existing session/file reference parts. Resolver
diagnostics use the current prompt-reference debug feedback path. Before the
resolver runs, mixed-reference parsing already handles `@session:` and filters
configured MCP names/colon forms from filesystem reads, so these references do
not fall through to `@path` file lookup.

The CLI also loads active extension descriptors through
`canopy_core::extension_inventory::load_active_local_extension_references`.
That bounded, read-only loader scans the QWEN_HOME-aware user extension
directory, validates supported Canopy and Agent Plugins manifests, follows
linked installs with valid metadata, and applies the activation snapshot,
legacy projection, trust, safe/bare-mode, and `--extensions` override gates.
ACP loads the same inventory with its workspace trust and process mode; ACP has
no explicit extension-name override. Both hosts pass only active descriptors
to this resolver.

`project_active_local_extension_reference` is the host-neutral eligibility
projection used by `ExtensionInventory` for validated candidates. It accepts
the activation result resolved for the current workspace, CLI name overrides,
safe/bare-mode flags, trust state, and the stored user/project scope. It applies
the same safe-mode, bare-mode, name-override, and activation precedence as the
TypeScript manager; project-scoped candidates are withheld in untrusted
workspaces. Global user-scope extensions can still be projected in an
untrusted workspace. Scope preferences remain metadata and do not substitute
for the extension activation decision.

`extension_inventory` reads the activation snapshot and legacy enablement file
without mutating either, and never treats marketplace records or extension
preference scope alone as proof that an extension is installed or active. Its
current discovery boundary is the global user extension directory; workspace
extension directories and ad hoc project-local manifests are not scanned.
Supported inputs and read/count limits are documented in
`../extension_inventory_PORT_STATUS.md`. The resolver's local extension
descriptors provide mention context. Separately, the inventory returns
bounded MCP server configs for active extensions; native CLI and ACP pass those
configs through their normal MCP settings assembly. The inventory and
reference resolver do not execute extension hooks or commands.

ACP `@ext:` and MCP reference wiring is implemented for text prompts; success
and error cards, non-text reference parsing, and TypeScript's full mixed
reference ordering remain incomplete. When wiring another host, resolve
`@ext:` first, handle `@session:` before MCP matching, pass configured server
names plus discovered registries and an already-connected manager, append
parts after the query, surface diagnostics, and preserve the original token
when `canonical_reference` is absent. A recognized MCP resource retains its
canonical token even when lookup/read fails, matching the TypeScript path.

No tests were added or run for this service.
