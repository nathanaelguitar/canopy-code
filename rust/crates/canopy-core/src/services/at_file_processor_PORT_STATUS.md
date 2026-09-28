# Native `@file` processor status

`at_file_processor.rs` ports the filesystem portion of the TypeScript prompt
processors in `packages/cli/src/services/prompt-processors/atFileProcessor.ts`
and `packages/cli/src/ui/hooks/atCommandProcessor.ts`.

## Core API

- `AtFileProcessor::new(workspace_root)` creates a workspace-confined reader.
  Hosts can use `new_with_custom_ignore_files` to pass configured Canopy ignore
  files.
- `process_braced_injections(text, command_name, modalities)` expands
  `@{path}` in place for custom-command prompts. It returns Gemini-shaped
  `parts`, `files_read`, and host-visible `diagnostics`. Unclosed braces return
  an error; failed reads keep their original placeholder; ignored files are
  omitted.
- `resolve_file_mentions(mentions, modalities)` reads filesystem-only paths
  supplied by a host's mixed-`@` parser. It returns the file-context parts to
  append after the retained user query, per-path read status, paths, and
  diagnostics. The host must filter `@session:`, `@ext:`, and MCP references
  before calling it. `new_with_additional_allowed_roots` lets a host safely
  include an explicitly trusted root such as Canopy's global temp directory.

Both methods use the existing bounded `ReadFileTool`, enforce canonical
workspace containment after resolving symlinks, and respect Git and Canopy
ignore rules. Direct file reads retain the native reader's text, PDF, notebook,
and media behavior. Braced directory references recursively include file
content; ordinary `@path` directory references use the bounded folder-tree
projection, matching their distinct TypeScript behaviors.

## Native CLI wiring and remaining gaps

- One processor call adds at most 16 MiB of serialized model parts. A braced
  prompt may contain at most 64 injections. Directory content injection scans
  at most 10,000 entries and includes at most 128 files. Truncation is reported
  as a warning diagnostic.
- Ordinary `@path` processing is wired into native interactive turns,
  one-shot prompts, and clean `--resume` prompts. The host parses escaped
  paths, handles session references first, excludes configured MCP references
  and extension references for their separate resolver, keeps the `@path`
  tokens in the query, appends modality-aware file parts, and surfaces per-file
  success/error cards plus ignore and path diagnostics in TUI and line mode.
  Workspace paths and the canonical global temp directory are allowed; both
  the requested and canonical paths are checked for containment and ignore
  rules. The original prompt remains the display text.
- File content uses the native read-file projection for files and a bounded
  folder-tree projection for directories. A 16 MiB aggregate cap protects the
  appended content. Fullscreen TUI extension prompt commands now call
  `process_braced_injections` after argument expansion and submit its ordered
  text/media parts inline. Injection paths are confined to the workspace root;
  ignore, read-failure, and truncation diagnostics are surfaced in the TUI.
  The API still is not wired into ACP or non-interactive custom-command
  dispatch.
- The TypeScript config can search multiple workspace directories. Native
  `canopy run` currently resolves against one workspace root plus the global
  temp directory; extra configured workspace roots remain unported. Mixed
  reference content is grouped by reference kind rather than interleaved in
  query order.
- Image vision-bridge orchestration remains host-owned. Pass the selected
  model's `InputModalities`; the service does not start a bridge itself.
- Extension, MCP resource, and MCP server resolution remain in their separate
  native reference resolver. The service itself does not resolve those
  reference kinds.

No tests were added or run for this service.
