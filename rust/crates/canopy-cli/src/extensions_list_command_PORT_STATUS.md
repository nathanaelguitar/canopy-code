# Native extensions list CLI port status

`canopy extensions list` performs a bounded, read-only scan of installed user
extension slots. It lists supported Canopy and Agent Plugins v1 manifests even
when disabled, reports separate user-home and current-workspace activation, and
shows the installed path plus available source, origin, ref, and release-tag
metadata. Extension source URLs have credentials redacted before display.
Manifest, command, context, skill, agent, and MCP labels are rendered when the
native inventory has them. Corrupt or unreadable activation data leaves the
extension visible with `unknown` activation and a stderr diagnostic; it is not
misreported as disabled.

The list path shares candidate parsing, extension identity calculation, and
activation snapshot/legacy projection resolution with the native active
extension inventory. It does not load extensions into the runtime, create
Agent Plugins data directories, or mutate extension state.

Parity limits:

- Discovery remains limited to the global user extension directory. Workspace
  extension slots are not included, matching the TypeScript command's loaded
  manager cache.
- Native discovery retains its explicit entry, byte, and label caps. Nested
  command discovery is limited to depth 32, 4,096 visited entries, and 128
  displayed commands.
- Locale-map values for Canopy display names/descriptions are not resolved by
  the native inventory; plain string values are shown when present.
- The native CLI has no extension-manager cache, so paths that depend on
  runtime-only variable expansion or dynamically created extension data may
  not appear exactly as they do in an interactive TypeScript session.
