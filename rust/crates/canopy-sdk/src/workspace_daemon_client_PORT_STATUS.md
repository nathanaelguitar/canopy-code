# Workspace daemon client projection port status

Source: `packages/sdk-typescript/src/daemon/DaemonClient.ts`,
`WorkspaceDaemonClient` (the class beginning near line 5329).

## Ported

- Added and exported `WorkspaceDaemonClientProjection` and
  `WorkspaceRequestOptions` from `workspace_daemon_client.rs`.
- Ported the workspace facade's public route catalog: channel management and
  pairing, MCP and voice, Git and GitHub, workspace status/configuration,
  agents and sessions, session groups and organization, file operations,
  settings/trust/permissions, tool and skill controls, reload/init, and
  extension activation.
- Workspace IDs and cwd selectors are URI-component encoded once and route
  labels retain the TypeScript workspace-qualified form. Session export and
  voice/file upload use the existing bounded byte transport. The global live
  methods delegate to the root `DaemonClient`, as in TypeScript.
- JSON request and response bodies use `serde_json::Value`; shared daemon
  transport helpers retain authentication, timeout, cancellation, and HTTP
  error handling.

## Remaining compatibility limits

- Rust returns open `Value` payloads instead of TypeScript's endpoint-specific
  compile-time interfaces.
- `upload_workspace_file_with_progress` reports 0 and subsequent `{ loaded,
total }` values as 64 KiB body chunks are consumed by reqwest. These values
  describe bytes yielded to the HTTP transport, not bytes confirmed by the
  daemon. The callback runs on the async task driving the request. Extension
  archive progress is available on the root `DaemonClient` through
  `install_extension_archive_with_progress`; the workspace projection does
  not expose that route. Other binary request methods do not expose progress
  callbacks.
  Existing Rust byte-route limits still cap uploads and successful binary
  responses at 32 MiB, whereas the TypeScript wrappers do not impose that same
  limit.
- `rustfmt --edition 2024 --check` and `cargo check -p canopy-sdk --locked
--offline` pass with the module exported. No tests were added or run.
