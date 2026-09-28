# `record_artifact` Rust port status

Source files: `packages/core/src/tools/record-artifact.ts`,
`packages/core/src/services/session-artifact-persistence.ts`, and the
`ChatRecordingService.recordToolResult` path.

`record_artifact.rs` now defines the native function schema, typed parameters,
argument validation, storage inference, and metadata-only tool result. It
requires exactly one of `workspacePath`, `managedId`, or `url`; validates the
locator, supported kind/storage, bounded display fields, and primitive metadata;
and returns one artifact in the first-class `ToolExecutionOutput.artifacts`
collection. It does not inspect or modify the referenced resource.

`AgentRuntime` carries that collection as `toolCallResult.artifacts`, separate
from the model-visible function response. `SessionRecorder::record_tool_result`
normalizes artifact objects through the existing `SessionArtifactStore` and
appends v2 `session_artifact_event` records to the UUID-chained transcript.
Every 50 durable changes it appends a `session_artifact_snapshot`. On resume,
`SessionStore` uses the existing active-lineage artifact selector, then the
existing snapshot rebuilder, to restore the store and continue the sequence.

The existing ACP `SessionArtifactStore` JSONL adapter and transcript reader
remain the source of truth for ACP's separate artifact API and reader
projection. This recorder integration does not create a second persistence
format.

Remaining integration gaps:

- The ACP server now declares `record_artifact` and dispatches it through
  `RecordArtifactTool::execute`, subject to permissions and core-tool
  enable/exclude settings. The interactive CLI host still needs its function
  declaration and `WorkspaceTools` dispatch registered.
- The recorder writes and restores artifact events, but the `SessionRecorder`
  store is not connected to ACP's live artifact-list store or a CLI artifact
  panel/event stream.
- `SessionArtifactStore` currently has no sticky-ephemeral mutation path or
  managed-content copy/restore validation in this integration.
- Invalid or unsupported artifact objects from a host tool are skipped for
  artifact events while the tool result itself remains recorded.

`cargo fmt --manifest-path rust/Cargo.toml --package canopy-core` and
`cargo check --manifest-path rust/Cargo.toml -p canopy-core --locked --offline`
passed. The CLI crate check passed before ACP host wiring, with three warnings
in `mcp_host.rs`; the full CLI check is pending after ACP host wiring. No tests
were added or run.
