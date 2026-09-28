# Rust SDK process-query port status

`canopy-sdk` implements a focused process-backed version of the TypeScript SDK
`query()` / `Query` / `ProcessTransport` path.

## Covered

- `query(prompt, options)` starts one turn; `Query::start(options)` starts an
  interactive query. Both complete the `initialize` control handshake before
  returning. `Query::send_text` and `Query::send_message` provide subsequent
  input, `Query::stream_input` accepts a Rust `Stream` of user messages, and
  `Query` itself implements `Stream<Item = Result<Value, QueryError>>`;
  `next_message` is also available for direct polling. `next_typed_message` and
  `typed_stream` expose typed SDK message variants while retaining each raw
  JSON payload and passing unknown message types through as open strings.
- The process uses `--input-format stream-json`, `--output-format stream-json`,
  and `--channel=SDK`, with the common process options mapped to their CLI
  flags. `QueryOptions::validate` ports the relevant option constraints,
  reserved-extra-argument checks, fork requirements, and UUID session checks.
- JSON-lines input is serialized with a trailing newline. Blank, malformed,
  and non-object or missing-string-`type` output lines are skipped, matching
  the TypeScript parser. CRLF and a final line without newline are accepted.
- The Python SDK confirms the same `stream-json` CLI flags, `SDK` channel,
  JSON-lines user envelope, and initialize/control-message shapes. The Java
  process SDK inspected here is an ACP JSON-RPC transport, so it has a separate
  wire protocol and is not used as an SDK stream-json reference.
- Output messages and SDK messages are queued through bounded channels. Each
  input or output JSON line is bounded to 16 MiB by default (configurable up to
  32 MiB); an over-limit output line terminates the stream with
  `QueryError::MessageTooLarge`.
- The initialize handshake (including initial effort), common public control
  methods, `can_use_tool` callbacks with permission suggestions, timeout and
  fail-closed validation, process exit errors, close/drop termination, and
  single-turn stdin closure after a result are implemented. Permission callbacks
  run concurrently with stream routing so `control_cancel_request` can abort a
  pending callback instead of waiting behind it. Optional `QueryOptions::cancellation`
  and `Query::cancel` provide query-wide cancellation for stream, permission,
  control, and process work. The optional `stderr` callback receives UTF-8 lossy
  chunks capped at 16 KiB of source bytes; its queue holds at most eight chunks.
  `debug` without a callback forwards drained bytes to the parent stderr.
- `QueryOptions::agents` accepts typed `SubagentConfig` entries and passes them
  in the initialize control payload. Required strings are validated as
  non-empty, and optional source fields are serialized only when supplied.
- `QueryOptions::mcp_servers` accepts external CLI config values and
  SDK-hosted tool server definitions. Context-aware tool, resource, and prompt
  handlers can also receive the MCP request ID, raw `_meta`, and a per-request
  cancellation token. External configs are forwarded in `mcpServers`; SDK
  server names are advertised in `sdkMcpServers` only after their Rust
  definitions validate. The per-query MCP dispatcher serves
  `initialize`, `ping`, `tools/list`, `tools/call`, `resources/list`,
  `resources/read`, `resources/templates/list`, `prompts/list`, and
  `prompts/get` messages. Resources and prompts use Rust async handlers and
  retain raw JSON metadata, argument descriptions, and callback results.
  Resource templates are read through URI-variable matching; optional
  template list handlers contribute concrete entries to `resources/list`,
  with template metadata merged beneath each returned resource as in the
  TypeScript MCP SDK. Requests obey `timeout.mcp_request_ms` (60 seconds by
  default); notifications are dispatched without waiting for a response.

## Exported Rust API

- `query(prompt, options) -> Result<Query, QueryError>`
- `Query::{start, query, session_id, cancel, next_message, next_typed_message,
typed_stream, send_message, send_text, stream_input, end_input, interrupt,
continue_last_turn, set_permission_mode, set_model, get_context_usage,
get_available_models, get_usage_info, supported_commands, mcp_server_status,
set_effort, set_effort_status, initial_effort_status, close, is_closed}`
- `SdkMessage` provides `message_type`, `session_id`, `uuid`,
  `parent_tool_use_id`, and the complete `raw` JSON value, plus `as_raw` and
  `into_raw` accessors. Its `kind()` method returns typed user, assistant,
  system, success/error result, and partial assistant stream-event variants;
  future message and nested event types remain available as unknown variants.
- Typed receive symbols include `SdkMessageKind`, `SdkUserMessage`,
  `SdkAssistantMessage`, `SdkSystemMessage`, `SdkResultMessageSuccess`,
  `SdkResultMessageError`, `SdkPartialAssistantMessage`, `SdkStreamEvent`, and
  `SdkContentBlockDelta`. Nested user and assistant API messages are modeled by
  `SdkApiUserMessage` and `SdkApiAssistantMessage`; content bodies use
  `SdkContent`, `SdkContentBlock`, and `SdkAnnotation`. Usage metadata is
  modeled by `SdkUsage`, `SdkExtendedUsage`, `SdkServerToolUse`,
  `SdkCacheCreationUsage`, and `SdkModelUsage`.
- Known nested content blocks, usage counters, result fields, and stream event
  tags have typed views. Each modeled nested object keeps a borrowed `raw`
  value, the outer `SdkMessage` retains its complete JSON, and unknown content
  block variants use an `Unknown` case. Raw nested `message`, `usage`, and
  `modelUsage` fields remain available for lossless access and forward
  compatibility.
- `QueryOptions`, `QueryTimeoutOptions`, `QueryError`, `PermissionMode`,
  `AuthType`, `SystemPrompt`, `EffortTier`, `EffortOverride`, and `EffortStatus`.
- `SubagentConfig`, `SubagentLevel`, and `SubagentRunConfig` model the SDK's
  initialize-time agent definitions; extension fields can be retained in
  `SubagentConfig::extra_fields`.
- `StderrHandler` is the caller callback type for captured CLI stderr.
- `QueryCancellation` is a cloneable caller-owned cancellation handle; provide
  it through `QueryOptions::cancellation` or use `Query::cancel` after startup.
- `CanUseToolHandler`, `CanUseToolRequest`, `CanUseToolDecision`,
  `CanUseToolCancellation`, `PermissionBehavior`, `PermissionSuggestion`, and
  `PermissionSuggestionType`.
- `McpMessageHandler` and `McpMessageRequest` provide a Rust callback boundary
  for CLI `mcp_message` control requests. Requests await the callback's JSON-RPC
  response and fail with `MCP request timeout` after `timeout.mcp_request_ms`,
  defaulting to 60 seconds. Notifications are dispatched without waiting for a
  response, matching the TypeScript control transport. This callback remains
  available as a fallback for server names not in `mcp_servers`.
- `McpServerConfig`, `SdkMcpServerConfig`, `McpToolDefinition`,
  `McpToolDefinitionWithContext`, `McpResourceDefinition`,
  `McpResourceTemplateDefinition`, `McpPromptDefinition`, and their handler
  types define SDK-hosted and external MCP servers. Use
  `McpToolDefinition::new`, `SdkMcpServerConfig::new` (or `default_version`),
  then optionally add static resources with `with_resources`, dynamic
  resources with `with_resource_templates`, and prompts with `with_prompts`.
  Template definitions use `new` or `new_with_context` and can add a concrete
  resource list callback with `with_list_handler`. Context-aware tool
  definitions are added with `with_context_tools`; static resource and prompt
  definitions also have `new_with_context` constructors. Insert
  `McpServerConfig::sdk(...)` into `QueryOptions::mcp_servers`. Use
  `McpServerConfig::external(json)` for a CLI-managed server.

## Remaining parity gaps

- When `executable` is omitted, Rust searches for `dist/cli/cli.js` beside the
  crate, in an installed `node_modules/@qwen-code/sdk` package, and in the
  checked-out `packages/sdk-typescript` package. It also checks the repository
  root `dist/cli.js` bundle and repeats package lookups from the host
  executable's directory and current directory. If no local bundle exists, it
  falls back to `qwen` from `PATH`; the TypeScript SDK instead reports that its
  bundled CLI is missing. Rust launches a found JavaScript bundle through
  `node` from `PATH`, since a compiled Rust library cannot inherit the Node or
  Bun `process.execPath` of a TypeScript caller. Explicit `executable` values
  retain the existing command/path, JavaScript, and TypeScript handling; unlike
  TypeScript, the Rust SDK cannot probe whether `tsx` is installed before using
  it. Electron `FORK_MODE`, custom spawn metadata, and SDK logger configuration
  are not ported.
- Input is exposed through a Rust `Stream` on `Query::stream_input` rather than
  accepting `AsyncIterable<SDKUserMessage>` directly in `query()`. Result
  variants only recognize the current success and error subtypes; future or
  inconsistent result discriminants remain an open `ResultUnknown` variant
  with the original JSON still available.
- The callback receives `tool_name`, optional `tool_use_id`, object `input`,
  typed permission suggestions, optional `blocked_path`, and a
  `CanUseToolCancellation` token. The token is set on `control_cancel_request`,
  query close/drop, process exit, and callback timeout. Its result is validated
  before serialization; missing callbacks, callback errors or panics, invalid
  allow/deny shapes, and timeouts deny the request. `timeout.can_use_tool_ms`
  defaults to 60 seconds.
- Query-wide cancellation is idempotent. A handle already cancelled before
  startup returns `QueryError::Aborted` without spawning the CLI. Once started,
  cancellation causes pending `stream_input`, control requests, and message
  receive to return `QueryError::Aborted`; the output stream yields that error
  once and then ends. Active permission callbacks receive cancellation through
  their existing per-request token, and pending MCP callback futures are
  dropped. The child receives SIGTERM on Unix and is
  escalated to SIGKILL after five seconds; non-Unix targets use Tokio's child
  kill operation. The callback future is dropped on cancellation, and callbacks
  that launch detached work must use the supplied token to stop that work.
- Stderr is captured through a separate pipe when `debug` or `stderr` is set,
  so it cannot be parsed as stdout protocol data. Callback delivery uses a
  bounded queue and one blocking callback invocation at a time. On process
  exit the supervisor joins the stderr drain workers for up to one second, then
  aborts their async tasks if needed. A synchronous callback already running
  cannot be forcibly stopped; callers should return promptly. With `debug`
  and no callback, captured bytes are forwarded to the parent's stderr.
- Hook callbacks remain unavailable in the TypeScript SDK: it sends
  `hooks: null`, exposes no hook registration option or callback, and the CLI
  advertises `can_handle_hook_callback: false`. Rust matches this by sending
  `hooks: null` and returning `Unknown control request subtype: hook_callback`
  if a CLI unexpectedly sends that request, matching the TypeScript Query
  fallback for unrecognized control requests.
- SDK-hosted Rust MCP definitions are query-scoped and live in the
  control-message router; closing/cancelling the query or losing the CLI
  process drops pending handler futures. Unlike the TypeScript SDK, Rust does
  not create an MCP SDK `Server`/transport instance or call a server `close()`
  hook. Tool schemas are passed through as JSON Schema;
  the TypeScript Zod argument validation is not ported, so handlers must
  validate arguments and return a valid `CallToolResult` JSON object. The Rust
  context-aware handler variants receive `McpRequestContext` with the JSON-RPC
  request ID, raw `_meta`, and `McpRequestCancellation`; the token is set on a
  peer `notifications/cancelled`, request timeout, or dropped transport work.
  Existing plain handlers keep their original signatures and do not receive
  that context. The TypeScript `RequestHandlerExtra` includes `sendRequest` and
  `sendNotification`, but the SDK's `SdkControlServerTransport` routes every
  server `send()` only to `Query.handleMcpServerResponse`, which resolves a
  matching pending response ID. Unmatched server-originated requests and all
  server-originated notifications are logged and dropped; the CLI control
  path only implements CLI-to-SDK `mcp_message` delivery. There is therefore no
  working TypeScript server-to-client behavior to port, and Rust does not add a
  one-sided sender API. HTTP/auth fields and session ID are also absent from
  the current in-memory transport. Resource-template registration,
  listing, reads, and optional concrete-resource listing are implemented.
  Resource-template completion callbacks and resource/prompt subscriptions
  are not implemented. URI template matching
  follows the installed TypeScript SDK matcher: simple, `+`, `#`, `.`, `/`,
  `?`, and `&` expressions are recognized; `?`/`&` expressions capture each
  named parameter and require nonempty values; exploded comma captures become
  string arrays. For other multi-variable expressions the SDK matcher binds
  only the first name (and simple expressions reject comma-separated values),
  which Rust preserves. Semicolon operators and prefix modifiers are not
  interpreted as RFC 6570 features. Rust bounds templates and URIs to 1 MB and
  template expressions to 256; the TypeScript implementation allows up to
  10,000 expressions. Rust also does not normalize request URIs through
  JavaScript's `URL` or implement template completion. Resource-template
  listing always reports the registered canonical `name` and `uriTemplate`;
  metadata cannot replace those fields. Resource and prompt callback values are raw JSON rather
  than TypeScript SDK callback objects, and Rust does not run
  the TypeScript SDK's Zod validation. External MCP
  configs are forwarded as JSON objects without the TypeScript SDK's Zod
  normalization. The legacy `McpMessageHandler` remains available for fallback
  server lookup and custom JSON-RPC behavior. Other CLI-to-SDK control requests
  receive an unsupported-request error. `streamClose` waiting is implemented
  for `Query::stream_input` and is interrupted by query cancellation.
- Agent validation follows the TypeScript option check for non-empty required
  strings; like that check, whitespace-only strings pass. The protocol type
  declares `level` required, but the actual option schema does not validate it,
  so Rust allows it to be omitted and supports the declared `session` value
  when supplied. Unmodeled TypeScript fields must be placed explicitly in
  `extra_fields` to be forwarded.
- Process spawn and close behavior is adapted to Tokio. On Unix it requests
  SIGTERM and escalates after five seconds; non-Unix targets currently use the
  Tokio child kill operation. Unlike the TypeScript parser, the Rust transport
  applies an explicit line-size bound.

## Daemon REST/SSE transport foundation

- `daemon_sse::parse_sse_stream` parses daemon event frames from a byte stream,
  retains raw JSON for forward compatibility, caps an incomplete frame, and
  can terminate after an idle-read timeout. Event frames are emitted
  incrementally rather than accumulated in an unbounded decoded-event queue.
- `daemon_rest::RestSseTransport` opens the session events route with bearer
  authentication, optional client identity, paired `Last-Event-ID` and epoch
  headers, `maxQueued`/`connectReason`/`previousStreamId` query values, and a
  connect-only timeout. It validates SSE content type, returns validated
  stream-id and epoch response metadata, captures bounded HTTP error bodies,
  and exposes a lazy event stream using the shared SSE parser. Caller
  cancellation, stream drop, and idempotent `dispose()` release active
  requests/bodies.
- These modules provide the REST/SSE subscription foundation. `daemon_client`
  now adds the process-independent daemon route catalog, and
  `workspace_daemon_client` adds the workspace-prefixed facade. They retain
  open JSON payloads and share these transports; the browser `fetch()` /
  `RequestInit` / `Response` abstraction and injectable `restFetch` remain
  unported. Reconnection is provided separately by `daemon_auto_reconnect`
  below.

## ACP daemon event normalization

- `daemon_event_denormalizer::denormalize_acp_notification(method, params)`
  maps current and legacy `session/update` payloads, `_qwen/notify`, and direct
  workspace event methods into the shared `daemon_sse::DaemonEvent` type. It
  assigns process-wide monotonic synthetic IDs, preserves metadata and
  originator fields, and retains the normalized event envelope in `raw`.
  `filter_events_by_session(events, session_id)` drops events carrying a
  different non-empty `data.sessionId` while passing workspace-scoped events.
  These synthetic IDs are local ordering tokens and do not support REST/SSE
  replay. Both ACP transport foundations consume this module as described
  below.

## ACP-over-HTTP transport foundation

- `daemon_acp_http::AcpHttpTransport` performs the ACP initialize handshake,
  captures the connection ID and capabilities snapshot, sends JSON-RPC requests
  and notifications, and correlates bounded pending requests across a
  connection SSE pump and per-session reply pumps. Reply stream failures sweep
  only the pending requests owned by that stream scope. Dropping a request
  future, cancelling it, or disposing the transport releases its pending slot
  and active stream work.
- `subscribe_events` opens the resumable session `/acp` stream with paired
  `Last-Event-ID` and epoch headers, validates the accepted stream's content
  type and epoch metadata, and uses `daemon_sse::parse_sse_frames` to retain the
  authoritative bus cursor. It projects permission requests and denormalizes
  ACP notifications into `DaemonEvent`. Each subscriber has a bounded queue
  (64 events by default, configurable up to 4096), and dropping the stream
  cancels its reader.
- `map_route` ports the current `acpRouteTable` URL/method mappings, including
  session, workspace, file, and bulk-session routes with query coercion.
  `dispatch_route` maps those calls to an HTTP-shaped result for Rust callers.
  This is a transport foundation; it is not the full TypeScript `DaemonClient`
  API.

## Remaining ACP HTTP parity gaps

- Rust does not implement `DaemonTransport.fetch(url, RequestInit)`, synthesize
  a web `Response`, or expose `restFetch`. Callers use `dispatch_route`,
  `send_request`, and `send_notification`; per-call `RequestInit` header merging
  and body parsing remain the responsibility of a future SDK adapter.
- Rust rejects a second simultaneous `subscribe_events` reader for the same
  session with `SessionAlreadySubscribed`. The TypeScript wrapper tracks a
  reader count, but the daemon's session stream is single-reader; the Rust API
  makes that constraint explicit rather than allowing the newer GET to detach
  an existing consumer.
- The accepted epoch is returned in `AcpStreamMetadata`; there is no callback
  equivalent to TypeScript's `onEpoch`. The ACP transport ignores REST-only
  queue and stream-lineage options because the ACP session stream uses the
  daemon's event-bus ring. The remaining `DaemonClient` APIs are not ported.

## ACP-over-WebSocket transport foundation

- `daemon_acp_ws::AcpWsTransport` lazily opens one authenticated `ws:` or
  `wss:` connection, performs the 30-second ACP initialize handshake, and
  multiplexes numeric JSON-RPC requests and notifications over a single
  bounded writer queue. Pending requests are capped at 1,024; disconnect and
  disposal reject them all. Aborting a `session/prompt` request sends
  `session/cancel` when the connection remains open.
- `dispatch_route` reuses `daemon_acp_http::map_route`, projects JSON-RPC
  errors through the same HTTP status mapping, and forwards optional
  `X-Qwen-Client-Id` metadata as `_meta.clientId`. Capabilities first queries
  the derived HTTP `/capabilities` endpoint and falls back to the initialize
  result with `v: 1` and an empty `features` array.
- `subscribe_events` fans denormalized ACP notifications to session-filtered
  streams while passing workspace events through. Each subscriber has a
  drop-oldest queue capped at 256 events. Dropping a stream unregisters it;
  cancelling closes it, and WebSocket close/disposal yields a terminal error.
  The transport does not support replay.

## Remaining ACP WebSocket parity gaps

- Rust exposes `dispatch_route` and `subscribe_events`, not the TypeScript
  `DaemonTransport.fetch(url, RequestInit)` and web `Response` adapter. The
  capabilities fallback derives `/capabilities` from the WebSocket origin
  rather than accepting TypeScript's injected `restFetch` and caller URL.
- Only `X-Qwen-Client-Id` is mapped into JSON-RPC metadata. Other
  `RequestInit` headers, URL-adapter behavior, browser WebSocket constraints,
  `DaemonClient` methods, transport negotiation, and reconnect callbacks are
  outside this module. Reconnect is lazy on the next request after a close;
  existing event streams end and cannot resume.
- TypeScript's REST-only subscribe fields (`epoch`, cursors, `maxQueued`,
  client ID, and stream lineage) have no WebSocket replay semantics and are not
  exposed by the Rust subscription API. JSON-RPC frames are capped at 8 MiB;
  this is an explicit Rust bound.

## Automatic transport reconnect foundation

- `daemon_auto_reconnect::AutoReconnectTransport` wraps an
  `AutoReconnectBackend`, with a concrete `RestSseBackend` that combines the
  existing REST/SSE subscriber with `reqwest` fetch calls, plus
  `AcpHttpBackend` and `AcpWsBackend` adapters over the native ACP transports.
  On a `Closed` error, raw REST fetch, URL-route dispatch, and event
  subscriptions retry once. Recovery is serialized across concurrent
  failures, disposes the old backend, tries the configured preferred-type
  factory, and falls back to REST/SSE if the factory is absent or fails. No
  backoff is used. Session reattachment remains the caller's responsibility.
- `AutoReconnectTransport::dispatch_route` accepts a method, path, query,
  JSON body, and optional client ID, then delegates to the ACP route table or
  the REST fallback. ACP WebSocket forwards the client ID as `_meta.clientId`;
  ACP HTTP ignores it. The REST fallback returns the upstream status and a
  parsed JSON or text body, capped at 1 MiB.
- ACP HTTP route and subscription cancellation forwards into
  `AcpCancellation`; ACP WebSocket forwards into `AcpWsCancellation`. Accepted
  stream callbacks retain the wrapper order: stream-accepted with no stream ID
  for ACP, then epoch when the ACP HTTP server supplied one. HTTP maps resume
  cursor, epoch, and connect timeout to its session SSE stream. WebSocket has
  no replay and ignores REST-only stream query options. WebSocket connection,
  initialization-timeout, and stream-close errors become retryable `Closed`
  errors; ACP HTTP only retries its explicit `Closed` error.
- Event cursor, client, diagnostics, timeout, and cancellation settings are
  forwarded unchanged to replacement streams. After each accepted stream,
  callbacks run in TypeScript order: accepted-stream callback, then epoch
  callback if an epoch was supplied. Normal stream end, cancellation, and
  non-closed errors do not trigger a reconnect. Cancellation during an active
  event stream ends it cleanly; a pre-cancelled connect returns `Cancelled`.
  `type` and `connected` reflect the current backend; `supports_replay` keeps
  the initial backend's value, matching the TypeScript snapshot.
- `dispose()` is idempotent and disposes the current backend. If a factory
  finishes after disposal, the newly-created backend is disposed before it can
  be installed.

## Remaining automatic reconnect parity gaps

- The native Rust `AutoReconnectBackend` is a Rust trait rather than the
  TypeScript `DaemonTransport` interface. ACP adapters expose the route API
  through `dispatch_route`; they return an operation error from the raw
  `fetch` API because the Rust ACP modules synthesize `AcpHttpResponse` rather
  than a `reqwest::Response`. Callers using ACP should use
  `AutoReconnectTransport::dispatch_route` for URL-shaped SDK routes.
- Rust accepts a typed fetch request with an absolute URL, method, headers,
  byte body, and optional timeout, and returns `reqwest::Response`. It does
  not expose the web `RequestInit`/`Response` API or a TypeScript-style
  injectable `restFetch`; callers can provide a `reqwest::Client`.
- The REST route adapter accepts a structured path/query/body rather than a
  full URL plus `RequestInit`; custom request headers and route-specific
  timeout options are not part of `AutoReconnectRouteRequest`. ACP HTTP
  subscription failures that surface as protocol/SSE errors remain
  non-retryable, matching the wrapper's rule to retry only a transport-closed
  error. ACP WebSocket has no replay, so its replacement stream starts live
  and may miss events during reconnection.
- Rust's async `subscribe_events` connects before returning its stream;
  TypeScript's async generator defers work until iteration starts. Recovery
  also guards disposal during an in-flight factory and disposes a late
  replacement, closing the race present in the TypeScript implementation.
  The current `RestSseTransport` reports disposal as a `Closed` stream item,
  so an already-active Rust stream returns that error; TypeScript's REST parser
  treats its disposal abort as clean stream completion.

## DaemonClient route facade

- `daemon_client::DaemonClient` now wraps the reconnecting REST/SSE and ACP
  transports. It provides generic JSON, raw-status, no-content, and bounded
  byte requests; REST-only selection; workspace ID/CWD scoping; health and
  capability discovery; event subscriptions; blocking and nonblocking
  session prompts; cancel/close; and the auth device-flow polling facade.
  JSON and error bodies are bounded at 1 MiB. Byte uploads and successful byte
  responses are bounded at 32 MiB, while byte error responses use the 1 MiB
  error-body cap. The default request timeout is 30 seconds, prompt queue cap
  is five per session, and the `QWEN_SERVER_TOKEN` fallback is supported.
  Session creation supports capability gates for requested IDs/source metadata
  and validates the returned ID against the lowercased requested UUID. The
  primary TypeScript `DaemonClient` route catalog is covered with endpoint
  methods for status/usage, workspace Git and MCP, extensions, files, memory
  and agents, sessions, permissions, auth, live voice, and channel control.
  Session request bodies are projected to the TypeScript fields, and route
  wrappers preserve their REST-only selections, client IDs, query values,
  timeouts, and special success statuses.
- Primary and workspace-scoped `upload_workspace_file_with_progress` methods
  stream the request body in 64 KiB chunks and report `{ loaded, total }`
  through `UploadProgress`. They preserve the per-call timeout, cancellation,
  client ID, upload-size limit, and response validation. Progress tracks bytes
  yielded as the HTTP client consumes the body; it does not confirm server
  receipt or persistence.
- `DaemonClient::install_extension_archive_with_progress` uses the same
  bounded streaming body for archive installation and retains the TypeScript
  SDK's fixed 120-second timeout and request/response behavior. TypeScript's
  `installExtensionArchive` has no progress callback or caller cancellation;
  Rust adds both progress reporting and an optional `RestSseCancellation`.
- `match_turn_event`, `is_daemon_turn_error`,
  `is_stale_branch_point_error`, `is_subagent_session_not_found`,
  `is_session_level_not_found`, and `is_non_blocking_accepted` expose the
  corresponding shared TypeScript helpers. `DaemonCapabilityMissingError`
  and `DaemonSessionIdProtocolError` preserve their TypeScript error data.
  `reqwest::Client` is injectable; its standard environment proxy support is
  retained.

## Remaining DaemonClient parity gaps

- Rust endpoint methods keep request and response payloads as
  `serde_json::Value`; the TypeScript request/response interfaces and
  endpoint-specific Rust response structs are not ported. The separate
  workspace-prefixed `WorkspaceDaemonClient` projection is tracked outside
  this primary-client catalog.
- The browser `fetch` / `RequestInit` / `Response` surface and injectable
  `restFetch` are not ported. ACP routes accept JSON only; binary bodies,
  custom headers, and raw response bodies require REST mode. ACP HTTP does not
  forward client IDs, while ACP WebSocket maps them to RPC metadata.
- `wait_for_extension_operation` now polls until the operation leaves
  `queued`/`running`, with the TypeScript default one-second interval,
  ten-minute deadline, infinite-deadline option, and caller cancellation. A
  deadline or cancellation only drops the local poll; it does not cancel the
  server operation. Generated workspace/session content uses the bounded SSE
  JSON parser, validates the TypeScript version-1 event variants, preserves
  accepted events' extra fields in `raw`, and requires a terminal event for
  workspace generation only. Progress callbacks are available for primary
  and workspace-scoped file uploads and extension archive installation;
  other binary requests do not expose progress callbacks. Archive progress
  uses the same 64 KiB transport-consumption boundary, and archive upload
  keeps the 120-second timeout. TypeScript's archive method does not expose a
  caller abort signal. Active-session export projects text, MIME type, format,
  and the content-disposition filename.
- `DaemonAuthFlowHandle` keeps the start response under `initial`; TypeScript
  projects its fields directly on the returned handle. Its cancellation and
  polling methods are provided, but the ergonomic handle shape does not match.
- Rust cancellation uses `RestSseCancellation` and reports a Rust
  `Cancelled` error; it cannot preserve arbitrary JavaScript `AbortSignal`
  reasons or `DOMException` identity. The async Rust event subscription also
  connects before returning, as documented above.
- Generated-content streaming is available through the REST raw-fetch path.
  ACP transports do not expose raw fetch responses, so these two methods return
  the transport's unsupported-operation error when used with ACP. The Rust SSE
  parser uses its 48 MiB unread-frame bound; JavaScript cancellation/error
  objects are represented by Rust errors.
- Restore calls use the advertised daemon restore timeout plus 10 seconds, or
  70 seconds when that capability has not been fetched. TypeScript instead
  uses an explicitly configured client fetch timeout for restore calls; Rust
  does not distinguish an explicitly supplied default-valued timeout from the
  SDK default.

## Serve-bridge MCP stdio slice

- `serve_bridge::ServeBridge` exposes a newline-delimited MCP JSON-RPC server
  over stdio, with `initialize`, `ping`, `tools/list`, and `tools/call`. It
  advertises all 31 source tools: two infrastructure tools, eight session
  tools, both agent tools, ten workspace-read tools, and nine workspace-write
  tools from `packages/sdk-typescript/src/daemon-mcp/serve-bridge/tools/`.
  The workspace-read tools are
  `file_read`, `file_read_bytes`, `file_stat`, `dir_list`, `glob`,
  `workspace_mcp_status`, `workspace_skills`, `workspace_providers`,
  `workspace_env`, and `workspace_preflight`.
- Tool names, descriptions, exposed argument fields, required string fields,
  optional string/number validation, and object stripping follow those source
  definitions. Successful daemon JSON is projected as pretty-printed text in
  `content`; daemon and validation errors are returned with `isError: true`.
  Diagnostics go to stderr. The server accepts MCP protocol version
  `2024-11-05`, bounds each stdio frame to 8 MiB, uses the daemon client's
  1 MiB JSON response bound, and defaults daemon requests to 30 seconds.
  `ServeBridgeOptions::from_env` reads `QWEN_DAEMON_URL`,
  `QWEN_DAEMON_TOKEN`, and `QWEN_WORKSPACE_CWD`.
- Workspace-write coverage includes hash-checked file write/edit payloads,
  approval-mode restrictions, settings tool toggle,
  workspace init and MCP restart, memory read/write, and list/get/create/update/
  delete agent operations. Tool fields are validated by type, enum, array and
  record shape; unknown top-level fields are stripped to match Zod's default
  object behavior. Conditional agent operation checks, empty-string handling,
  opt-in scope restrictions, and the source error text are preserved.
- The session tools preserve their source schemas and unknown-field stripping.
  `session_create`, `session_load`, and `session_resume` select the returned
  session as the default only after success; create/load/resume use the
  explicit `workspace_cwd` or `QWEN_WORKSPACE_CWD` fallback. Load calls the
  daemon's history-replay `/load` route, while resume calls `/resume`. Close
  clears the matching default session even when the daemon request fails;
  update-metadata, list, set-model, and context project the same client results
  as the TypeScript handlers. Restore uses the daemon client's bounded restore
  deadline (advertised timeout plus 10 seconds, or 70 seconds before the
  capability has been cached); other calls retain the client's request timeout
  and 1 MiB JSON response bound.
- `prompt` requires the session's persistent SSE subscription, enforces one
  active collector per session, sends the source prompt envelope, and waits for
  the daemon client's prompt-ID-correlated terminal event. The persistent
  session stream collects `agent_message_chunk` text and completes on the
  final chunk's `_meta`; daemon update names containing `error` or `fail` and
  stream termination interrupt the collector. After `DaemonClient.prompt`
  resolves, the bridge applies the source 30-second `_meta` guard, best-effort cancels on
  timeout, and returns the same partial-text timeout/interruption shapes.
  `prompt_cancel` best-effort calls the daemon cancel route, then resolves the
  active collector as interrupted and returns `{ok, sessionId}`.
- Session create/load/resume start an SSE subscription after success; switching
  the default session stops the prior stream. Close stops its stream even when
  the daemon close fails. The stream reader uses the SDK's reconnecting SSE
  subscription, updates activity on prompt use and message chunks, removes a
  stream after disconnect, and clears the default when that stream was current.
  Stdio EOF cancels active prompts and stops all subscriptions. Requests run
  concurrently so a `prompt_cancel` request can be read while a prompt is
  waiting; the bridge permits 64 ordinary in-flight requests and reserves eight
  additional slots for cancellation requests.
- Memory and connection lifetime are bounded: each prompt collector stores at
  most 1 MiB of UTF-8 text, at most 64 persistent session streams are kept, and
  streams idle for 30 minutes are cleaned up on a five-minute interval. At the
  stream cap, the least-recently-active stream is evicted and its active
  collector is interrupted. Output text over 1 MiB is returned as a collected
  prefix with an error and truncation warning. If the stdio in-flight limit is
  reached, identified requests receive a JSON-RPC server-busy error.
- `QWEN_BRIDGE_ALLOW_GLOBAL_SCOPE` enables the source's privileged approval
  modes and persistence, tool toggle, MCP restart, global memory writes, and
  global agent create/update/delete. Global agent `get` remains ungated, as in
  the TypeScript handler. `session_set_approval_mode` and the session controls
  resolve an explicit `session_id` or the bridge's default ID. The TypeScript
  handler does not preflight the daemon's advertised write feature flags, so
  this port also lets daemon route responses provide those errors.
- Persistent SSE and prompt collection are implemented. Remaining differences
  are the explicit Rust limits above (1 MiB collected text, 64 streams, and
  bounded concurrent requests), plus the public `set_default_session_id` hook
  only selects a session and does not open an SSE stream; normal session tool
  flows do open it. The bridge ignores notifications, does not support
  JSON-RPC batches or cancellation notifications, and accepts only protocol
  version `2024-11-05`. No standalone Cargo binary or package/CLI wiring is
  included in this module.
