# Native extension inventory status

`extension_inventory` supplies read-only descriptors for `@ext:<name>` prompt
references and bounded MCP server configs for native `canopy run` and ACP. It
reads the QWEN_HOME-aware user extension directory and existing extension
enablement and activation files. It never installs, fetches, repairs,
quarantines, or writes extension state or plugin data. Marketplace records and
extension preferences do not establish that an extension is installed or
enabled.

## Supported inputs

- An installed user extension directory containing `canopy-extension.json`.
- An installed user extension directory containing an Agent Plugins v1 root
  `plugin.json` with the supported schema.
- A linked install only when the normal install slot contains valid
  `.canopy-extension-install.json` metadata with `type: "link"` and a source
  path. The target must be a directory with a supported manifest. Relative
  sources resolve from the process working directory, matching the loader's
  path behavior. Direct symlink entries in the extensions directory are
  ignored.
- Current `extension-store/state.json` policies, the legacy
  `extension-enablement.json` projection when it is newer or the snapshot is
  absent, project-scope trust gating, explicit `--extensions`/`-e` name
  overrides, and safe/bare mode gates.

Canopy extension descriptors include bounded context file paths, skill names,
MCP server names, and shallow agent names. Active Canopy MCP configs are
available to `canopy run` and ACP; settings and project config keep precedence,
extension configs fill only missing server names, and ACP session-provided
servers override extension entries. Agent Plugins descriptors include bounded
skill names and normalized servers from a root `mcp.json`. Agent Plugins MCP
normalization does not create plugin data directories. Loading a config does
not itself connect to or execute its server; each host's usual MCP lifecycle
decides which configured servers are admitted and started.

`ExtensionInventory.active_skill_extensions` retains parsed `SkillExtension`
configs for extensions that pass the same activation, trust, explicit-name,
safe-mode, and bare-mode projection. It can be assigned directly to
`SkillManagerConfig.active_extensions` by a host. `LocalExtensionReference.skills`
remains the existing list of skill names, in the same scan order. Both the
retained configs and name list come from one bounded scan of contained
`SKILL.md` files.

## Limits and unsupported cases

- Discovery is limited to the global user extensions directory. Workspace
  extension directories and ad hoc project-local manifests are not scanned.
- Marketplace/source registries, installed marketplace lists, and preferences
  are never treated as installation evidence. No conversion or remote lookup is
  performed.
- Direct symlink entries, link installs without valid install metadata, and
  manifests that resolve outside their owning extension directory are skipped.
- Unsupported Agent Plugins schema versions are skipped. Only the root
  `plugin.json` v1 manifest and root `mcp.json` are inspected; nested plugin
  layouts and Agent Plugin agent/command activation are not projected. Agent
  Plugins stdio servers whose working directory relies on a not-yet-created
  `${PLUGIN_DATA}` directory fail runtime path validation; native inventory
  does not create that directory. Existing data directories and HTTP servers
  can be used without this limitation.
- Canopy locale maps for `displayName` and `description` are not localized in
  this native mention descriptor; plain string values are used when present.
- Project-scoped candidates are withheld in untrusted workspaces when their
  recorded scope is known. As in the TypeScript preference store, absent or
  malformed scope entries have no project-scope value.
- The loader caps discovery at 256 extension-root entries and 128 manifest
  candidates; component directories at 128 entries; context paths and
  capability labels at 64; MCP server names at 256 bytes, 64 servers per
  extension and 128 active extension servers overall, each server config at
  64 KiB, each extension's MCP configs at 256 KiB, and active MCP configs at
  1 MiB total; preference, legacy, and activation policy maps at 512, including
  per-policy path rules; manifests, MCP files, legacy
  enablement, and activation state at 1 MiB; install metadata at 64 KiB;
  preferences at 256 KiB; skill manifests at 64 KiB; and total inventory
  reads at 8 MiB. Over-limit activation or legacy state fails closed. Over-limit
  preference scopes withhold extensions in untrusted workspaces; excess
  candidates/components are skipped with a diagnostic.
- If activation state and its legacy projection have equal filesystem
  timestamps but disagree by the stored projection hash, inventory is withheld
  because the TypeScript store treats that state as corrupt and this read-only
  loader cannot repair it.
