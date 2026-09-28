# Claude plugin package conversion status

`claude_package_converter.rs` ports the local filesystem conversion paths in
`packages/core/src/extension/claude-converter.ts`. It reuses the Rust Claude
manifest/agent projection and the shared extension hook-variable substitution.

## Covered

- Reads standalone `.claude-plugin/plugin.json` and marketplace
  `.claude-plugin/marketplace.json` only when the manifests resolve within the
  plugin/marketplace root. Standalone config bodies must be JSON objects;
  marketplace plugin configs retain the source converter's more permissive
  merge behavior.
- Converts standalone plugins and marketplace entries whose `source` is a
  local relative path. Marketplace fields override plugin fields when truthy,
  with the same default version when the plugin manifest is absent.
- Enforces lexical and canonical path confinement for marketplace sources,
  MCP/hooks file references, selected resources, package copies, and symlink
  targets. Broken/escaping links and special files are skipped while copying;
  recursive directory-link cycles are stopped. The output is created in a
  unique mode-0700 temporary directory and removed best-effort after fatal
  conversion errors. `.git` metadata is removed from the converted package.
- Collects configured commands, skills, and agents with the source folder
  flattening/subfolder layout, skips hidden entries and escaping resource
  symlinks, and retains copied folders when the manifest does not override
  them. Agent Markdown frontmatter is parsed and rewritten with mapped tool
  names, approval mode, MCP/hooks/skills fields, and body prompt.
- Loads optional MCP and hooks JSON path fields when confined. Hook files may
  use either a direct object or a `{ "hooks": ... }` wrapper, and command hook
  variables are substituted with the plugin root. Malformed optional files
  and missing resources are returned in `warnings`; hosts can forward those
  warnings to their logger.
- Writes pretty `canopy-extension.json` and returns config, temporary package
  path, external-content marker, and warning strings.

## Remaining gaps

- Marketplace URL/GitHub/git-subdirectory acquisition is not implemented in
  this core module. It returns `RemoteSourceRequiresHost`; the host must perform
  the existing network-policy-aware release/clone operation, then call
  `build_canopy_extension_from_plugin_with_external_content` with the acquired
  plugin root, merged config, warnings, and `external_content: true` when
  appropriate.
- The extension installer, marketplace install/update lifecycle, and UI have
  not been wired to call this Rust converter or own the returned temporary
  directory lifecycle.
- Invalid out-of-schema resource values are skipped where JavaScript may throw
  during `path` coercion. Rust's YAML parser/stringifier and JSON/filesystem
  diagnostic wording may differ from the Node dependencies. Agent parse/write
  failures are warnings, like the source converter, but the warning text may
  differ.
- Source conversion does not guard internal symlink cycles; Rust stops them
  safely. This can change output beneath cyclic links. No package-conversion
  runtime parity check has been run.
