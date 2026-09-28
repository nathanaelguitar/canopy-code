# Computer-use Rust port status

This module ports the host-independent contracts from
`packages/core/src/tools/computer-use/`:

- pinned `cua-driver-rs` v0.5.2 platform/architecture asset mapping, mirror
  URL order, install paths, versioned approval key, and screenshot-dimension
  override precedence;
- all 35 tool descriptions and parameter schemas, captured from
  `schemas.ts` in `schemas-v0.5.2.json`;
- MCP permission-error string classification and precedence;
- high-risk tool/page-action detection and schema-directed parameter coercion,
  including JavaScript-compatible trimming and number-to-string formatting;
- install-state JSON shape, tolerant parsing, serialization, and exact-version
  approval matching;
- approval-file path resolution, tolerant file reads, recursive parent
  creation, direct JSON replacement, and exact package-spec approval checks;
- the install and macOS permission bootstrap state machine behind injected
  host/client interfaces, including ordered permission prompts, timeout,
  cancellation, daemon cleanup, and progress/error text;
- MCP text/image/audio result projection, structured-content forwarding with
  duplicate `tree_markdown` removed, and text-only display formatting.
- checksum validation, ordered mirror fallback, streamed hashing, staged driver
  installation, and best-effort macOS quarantine/LaunchServices handling;
- lazy MCP client lifecycle, screenshot configuration, idle shutdown, and
  reconnect behavior using Canopy's shared MCP runtime.
- filtered tool declarations and an `AgentToolExecutor` adapter with bounded
  arguments, schema-directed validation, and injected call authorization and
  bootstrap interfaces. Missing either interface rejects the call before the
  driver is invoked.
- native `canopy run` integration: `main.rs` resolves the CUA settings and
  image-dimension environment override, adds enabled declarations, and composes
  the adapter into both executor paths. `canopy-cli/src/computer_use.rs`
  provides terminal per-action approval, host bootstrap/download, macOS
  permission onboarding, and progress reporting. First-use approval explains
  the signed and notarized download and the macOS permission steps;
- native ACP integration: `acp_server.rs` resolves the same CUA settings,
  exposes enabled declarations, and composes the adapter into the session
  executor. `acp_server/computer_use.rs` requests `session/request_permission`
  for every action, offering one-call allow or reject. It checks the same
  deny/core-tool/exclusion rules; allow rules do not bypass ACP consent. The
  first accepted action also authorizes the verified driver install.
- ACP prompt cancellation now reaches CUA tool calls through the nested
  `AgentToolExecutor` compositions and `McpRequestOptions.cancellation`. A
  cancelled MCP call does not enter the driver's reconnect retry loop. Before
  transport dispatch begins, cancellation is reported as safe (no desktop
  action sent). After a mutating request begins dispatching, cancellation
  emits an ACP user-visible warning and returns an uncertain-completion result
  when the runtime still accepts the tool result: inspect the desktop and
  application state before retrying.
- structured CUA results serialize without first deep-cloning the structured
  `elements` tree, and successful calls no longer build an unused duplicate
  error string. Both changes reduce transient memory without changing the
  model-visible result.

The schema catalog must be regenerated when the TypeScript schemas or driver
pin change. Keep it synchronized with `packages/core/src/tools/computer-use/`
and verify that the catalog's tool names and schema keys remain identical.

Remaining gaps and differences:

- Rust `canopy run` exposes enabled CUA tools in its request declarations; it
  does not mirror TypeScript's deferred ToolSearch registry or full
  confirmation-mode UI. The Rust host uses a terminal y/N prompt when policy
  requires approval and fails closed when no terminal is available. ACP asks
  its client for a one-call choice on every action.
- The CLI bootstrap host's `is_cancelled` currently returns `false`. ACP races
  the bootstrap future against the prompt token and drops local work when
  cancelled; its outbound permission request cannot dismiss a dialog already
  shown by the ACP client.
- CUA MCP cancellation stops Canopy's local wait and passes the token to the
  transport, but it cannot guarantee that `cua-driver` stopped an action once
  its MCP request began dispatching. The MCP exchange has no confirmed
  action-retraction handshake; an interrupted mutating call therefore remains
  possibly applied.
  The uncertainty warning is best-effort because ACP may already be closing the
  prompt when it is emitted.
- The adapter validates the pinned schema features used by the current CUA
  catalog; it is not a general JSON Schema validator. Archive extraction uses
  native tar and PowerShell commands rather than Node's `tar` package, so some
  archive edge cases and error text may differ.

The CUA driver itself is the separately maintained Rust implementation under
`packages/cua-driver/rust`; this port changes Canopy's host integration. It does
not establish that the Node/V8-based product path's memory use or crash is
fixed.

## Memory review

The inspected code shows that CUA can add meaningful transient memory during
visual actions, but it does not prove that CUA caused the reported Canopy
crashes:

- Canopy rejects CUA arguments over 1 MiB and bounds their JSON depth and node
  count. Its shared stdio MCP transport separately rejects a JSON-RPC line
  above 64 MiB while reading it, before the retained buffer can grow beyond
  that cap (`agent_tool_adapter.rs` and `tools/mcp/native_transports.rs`).
- The stdio reader parses each line into owned JSON and then moves the
  `result` value out of the JSON-RPC envelope instead of deep-cloning it. It
  also releases a line buffer whose capacity grew above 1 MiB before waiting
  for the next message. A large wire line still briefly coexists with its
  parsed JSON value during parsing, but its capacity is not retained for the
  lifetime of the driver connection.
- `get_window_state` normally downsizes screenshots to a 1568-pixel longest
  edge. Its config permits `0` to disable resizing. The driver now consumes the
  capture PNG buffer: unchanged-size images reuse that allocation, and scaled
  captures release the compressed source after decoding. Encoding still
  briefly needs the output PNG and its base64 string at the same time.
- `get_desktop_state` intentionally returns the entire primary display at
  native resolution, without downscaling. Unless `screenshot_out_file` is
  supplied, both screenshot tools return inline base64 image content, which
  remains in the model-facing tool result.
- The agent runtime accepts a 50 MiB single tool result and caps accumulated
  history at 12 MiB. Rust externalizes history images at the configured image
  count threshold or 6 MiB of encoded image data, and starts an additional
  pass within 2 MiB of the history cap. A prompt or tool result that would
  exceed the cap is compacted before rejection; latest-turn images are kept in
  the bounded 8 MiB/session cache and referenced for request-time restoration.
  This byte-pressure trigger is Rust-specific; TypeScript currently triggers
  image externalization by count. A single image that cannot fit the bounded
  cache, or an oversized text-heavy history that remains over 12 MiB after
  image removal, still cannot proceed. The result is also copied into model
  history, session recording, and the live tool event, creating overlapping
  image allocations during that path.
- The Rust result projection previously deep-cloned all structured fields
  except `tree_markdown` before serializing, and cloned successful display text
  into an unused error fallback. It now serializes structured fields by
  reference and only builds that fallback for actual errors. The encoded
  screenshot, display text, and final structured JSON string still need memory
  for the model-facing result, matching the source output.

These paths make CUA screenshot handling a plausible source of short-lived
memory spikes, especially for full-desktop captures or when resizing is
disabled. There is no evidence here that the CUA subprocess has excessive
steady-state RSS or that it caused the crash; confirming that requires crash
logs and process-level memory samples for both Canopy and `cua-driver`. No
CUA-specific response-byte limit was added because it would change the existing
inline-image and full-resolution desktop behavior.
