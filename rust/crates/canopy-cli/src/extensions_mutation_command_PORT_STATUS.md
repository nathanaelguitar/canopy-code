# Native extension CLI port status

The Rust CLI routes `canopy extensions install`, `list`, `enable`, `disable`,
and `uninstall` to native commands. Update and source-registry commands remain
in the TypeScript CLI.

## Install

`canopy extensions install <source>` handles local directories and supported
archives, Git and GitHub repositories, GitHub release assets, HTTPS archive
URLs, and scoped npm packages. It accepts `--ref`, `--auto-update`,
`--pre-release`, `--registry`, `--consent`, and `--scope user|project|workspace`.
The installer reads `.npmrc` registry and `_authToken` entries and `NPM_TOKEN`,
and uses `GITHUB_TOKEN` for GitHub downloads and clones.

The install lifecycle checks workspace trust, obtains the source, extracts
archives with the shared safe extractors, converts Agent Plugins, Gemini,
Claude, and Qoder packages, and previews the extension's MCP servers,
commands, skills, agents, and context files before consent. It prompts for
settings, hides sensitive input, stores secrets through the existing encrypted
file or macOS Keychain backend, writes install metadata, validates the staged
extension through the bounded inventory loader, and commits the artifact and
initial activation with the transaction journal. Failed staging removes the
staged files and any newly stored secret references. Downloaded archives are
streamed to disk with a 100 MiB cap; extraction uses the core path and link
safety checks.

User scope is the default. Project/workspace scope enables the extension for
the current workspace. An explicitly supplied scope is also saved as the
extension's preference.

## Activation and removal

Enable and disable require an installed extension and accept `--scope user` or
`--scope workspace` (default: user). User scope writes the home-directory path
rule used by the TypeScript manager; workspace scope writes the canonical
current-workspace override.

Activation changes use the v2 `extension-store/state.json` snapshot and update
the compatibility `extensions/extension-enablement.json` projection. The
writer preserves map order for the legacy projection hash, uses the shared
`extension-store/lock.lock` directory lock with a heartbeat, keeps the previous
snapshot, and atomically writes private mode-0600 JSON files. When a projection
is newer than the snapshot it imports those overrides using the same rule
matching and ordering; a same-timestamp hash mismatch or invalid state fails
closed. Before each mutation, the Rust store recovers interrupted artifact
transactions under the same lock.

Uninstall removes a matching extension from the bounded installed user
inventory. It commits the directory move and activation snapshot through the
store's transaction journal, then clears that extension's preferences on a
best-effort basis. Corrupt journals are quarantined; an interrupted
pre-commit transaction is rolled back, while a committed transaction retries
cleanup on a later store operation. A committed snapshot can be restored from
a `state_committed` journal or the previous snapshot when the current state
file is corrupt.

## Remaining parity gaps

- Update checks and `canopy extensions update` are not ported. Installed source
  metadata is persisted for future update behavior.
- npm authentication supports `NPM_TOKEN` and registry-scoped `_authToken`
  entries. Full npmrc interpolation and other npm credential formats are not
  implemented.
- The native installer needs an interactive terminal to collect extension
  setting values. It does not expose a programmatic settings callback like the
  TypeScript manager API.
- Git acquisition uses the system `git` executable. Provider-specific Git
  transports and credential helpers can behave differently from the
  TypeScript `simple-git` path.
- Rust store reads cap state at 1 MiB and transaction journals at 4 MiB; the
  TypeScript store currently has no matching size caps.
- A running extension host does not refresh automatically after a separate
  CLI install, enable, disable, or uninstall command. It must reload or
  restart to observe the change.
