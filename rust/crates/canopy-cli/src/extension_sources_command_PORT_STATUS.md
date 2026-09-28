# Extension Marketplace Sources CLI Port Status

The native CLI exposes `canopy extensions sources add|remove|list|update`.
It stores marketplace records at the shared `marketplaces.json` path and uses
`canopy-core`'s bounded marketplace fetch and source registry APIs. GitHub
marketplace requests read `GITHUB_TOKEN`; source URLs are redacted in list
output and service errors.

`update` reloads the registered marketplace and advances `lastUpdatedAt`,
while retaining the existing source name, URL, type, and `addedAt` value to
match the TypeScript CLI. The native fetch uses the core default network policy,
which does not impose a stricter HTTPS-only restriction and therefore keeps
the TypeScript CLI's accepted HTTP sources.

This slice does not add extension install, uninstall, enable, disable, or
marketplace discovery commands. It also does not add interactive consent or
extension-manager cache invalidation; each command is a standalone process.
