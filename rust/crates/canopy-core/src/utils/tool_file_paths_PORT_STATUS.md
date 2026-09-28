# Tool file path extraction port status

`tool_file_paths.rs` ports `FS_PATH_TOOL_NAMES`,
`isFilesystemPathTool`, and `extractToolFilePaths` from
`packages/core/src/core/coreToolScheduler.ts`.

The extractor canonicalizes legacy aliases before applying the closed
filesystem-tool allowlist, preserves path order and duplicates, joins glob and
grep selectors to their optional roots without normalizing path segments, and
accepts LSP plain paths and local `file://` URIs while dropping other URI
schemes. The module is exported from `utils` and used by the native CLI's
conditional-rule hook.

The native `WorkspaceTools` post-tool hook uses the allowlist and extractor,
then merges tool-reported result paths for glob and grep before conditional
rule matching. Non-allowlisted path-like input debug logging is omitted because
this Rust utility layer has no matching scheduler debug logger. File URI
conversion uses the platform behavior of `reqwest::Url::to_file_path`; unusual
UNC URI behavior may differ from Node's `fileURLToPath` on some targets.
