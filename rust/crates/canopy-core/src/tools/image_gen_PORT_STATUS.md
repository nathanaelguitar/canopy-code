# Image generation tool port status

Source: `packages/core/src/tools/image-gen.ts`.

`image_gen.rs` adds a native tool API that accepts a resolved image model route
and the host's effective API key. It mirrors the prompt's trimmed, non-empty
10,000 UTF-16-unit limit and the optional `width*height` size and total-pixel
checks. The tool calls `services::image_generation`, forwards the shared
cancellation token, and persists each verified PNG to
`.canopy/generated-images/<sanitized-session>/<uuid>.png` inside the canonical
workspace. It rechecks workspace containment around directory creation and
write, uses atomic replacement with mode `0600` and a no-follow destination,
and records the absolute result path.

The returned model content includes the saved path and, for image-capable
sessions, inline PNG data only when the image is at most 4 MiB. The service
caps larger downloaded PNGs at 10 MiB, so path-only responses remain bounded.
`ToolExecutionOutput.display` carries the generated-image artifact metadata
(workspace-relative path, MIME type, byte size, model, request ID, and requested
size) because the Rust tool result has no dedicated artifact field.

Remaining integration gaps:

- Native `canopy run` adapts configured `modelProviders` entries into the
  provider-neutral resolver and registers the tool only when `imageModel`
  uniquely selects an `imageOnly` entry with an explicit API-key environment
  name and registry base URL. The route resolver receives no hardcoded
  provider defaults. The Rust host currently has no bare/safe mode switches to
  pass to the resolver.
- ACP and interactive `canopy run` register the declaration only after the
  same route resolver succeeds. Interactive runs ask for approval by default,
  following `permissions` rules. ACP has no client permission request flow, so
  the image tool requires a matching `permissions.allow` rule there.
- ACP forwards its per-prompt `CancellationToken` through a host executor
  wrapper; cancelling the ACP prompt cancels the in-flight image request and
  download. The interactive executor interface still supplies a local token
  because it has no per-call runtime cancellation parameter.
- Atomic file replacement does not interrupt an in-progress synchronous disk
  write; cancellation is checked immediately before that bounded write.
- The Rust `ToolExecutionOutput` has no first-class artifact collection, so
  metadata is carried in its display object and file path list.

No tests were added or run. This tool adds no dependencies or lockfile changes.
