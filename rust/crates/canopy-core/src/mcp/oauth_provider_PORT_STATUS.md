# MCP OAuth provider port status

The native provider core is implemented in `oauth_provider.rs` and exported
from `mcp`. It covers the authorization-code/PKCE path from
`packages/core/src/mcp/oauth-provider.ts`, using the existing Rust OAuth
metadata/discovery helpers and `TokenStorage` abstraction.

## Implemented

- MCP OAuth settings and provider response types, including source-compatible
  camel-case config fields and token-storage credential fields.
- Protected-resource and authorization-server discovery, including the
  `WWW-Authenticate` resource-metadata challenge path and the metadata fallback
  used to find a dynamic registration endpoint.
- Public-client dynamic registration with the source client name, default
  redirect, `authorization_code`/`refresh_token` grants, code response type,
  `none` endpoint auth, S256 support, and requested scopes.
- PKCE verifier/challenge and high-entropy state generation, authorization URL
  construction with scopes, audiences, and the MCP `resource` parameter.
- A bounded loopback callback listener, path/state/code/error handling, a
  callback URL parser for host-managed redirects, and a five-minute callback
  deadline. The interactive helper binds before launching the browser.
- Bounded token and registration requests, JSON or form-encoded token parsing,
  authorization-code exchange, refresh-token exchange, expiry validation, and
  refresh persistence/deletion behavior.
- Persistence through existing `OAuthCredentials` and `OAuthToken` types, so
  saved records keep the existing `serverName`, `token`, `clientId`,
  `tokenUrl`, `mcpServerUrl`, and `updatedAt` JSON shape.

## Host integration and remaining gaps

- The interactive CLI HTTP/SSE path now resolves credentials for each
  OAuth-enabled server during transport setup. `ConfiguredTokenStorage` uses
  plaintext by default and selects protected storage under the explicit
  environment flag. It reads `~/.canopy/mcp-oauth-tokens.json` through
  `PlainFileTokenStorage`; with
  `CANOPY_CODE_FORCE_ENCRYPTED_FILE_STORAGE=true`, it selects the macOS
  Keychain when available, with `mcp-oauth-tokens-v2.json` as the encrypted
  fallback. `CANOPY_CODE_FORCE_FILE_STORAGE=true` forces that fallback. It calls
  `client_runtime.rs::get_valid_mcp_oauth_token`, which refreshes and persists
  expiring credentials. An explicitly supplied build-option token takes
  precedence; when no such token is supplied, a configured `Authorization`
  header remains authoritative and prevents stored-token injection.
- Interactive `canopy run` enables automatic browser authorization for a
  challenged HTTP server, a challenged SSE server with
  `oauth.enabled: true`, and an explicitly OAuth-enabled server that has no
  usable saved token. The
  native transport captures its `WWW-Authenticate` challenge in
  `McpOAuthRecoveryState`; `mcp_host.rs::authenticate_mcp_oauth` discovers the
  challenged resource, calls `McpOAuthProvider::authenticate`, persists the
  PKCE result, enables OAuth for the retry, and rediscovers the server. For an
  explicit OAuth configuration rejected before transport setup, it discovers
  metadata from the server URL and starts the same provider flow. A configured
  `Authorization` header and non-dynamic auth providers skip automatic login.
  The CLI retries discovery after authorization.
- `client_runtime.rs` also skips stored-token resolution and OAuth-required
  rejection when the server has an explicit `Authorization` header, so it can
  be used as configured without being replaced by an OAuth token.
- The OAuth discovery client has a 10-second connect timeout and 20-second
  request timeout. The provider's localhost callback wait is bounded to five
  minutes. The existing HTTP/SSE transport timeouts and challenge capture
  remain in place.
- The ACP host's `acp_server.rs::authorize_acp_mcp_servers` now detects the
  same eligible authentication failures after MCP discovery. Before opening a
  browser, it sends `session/request_permission` through
  `ProtocolOutput::request_client_for_session`; only an explicit
  `proceed_once` response starts login. It calls
  `McpOAuthProvider::authenticate`, stores credentials in the shared token
  file, enables OAuth in the retry settings, and rediscovers the MCP session.
  Consent waits are bounded to 30 seconds, the host flow to eight minutes,
  and the provider's loopback callback to five minutes. ACP
  `session/cancel` resolves a pending permission request and cancels the
  provider flow. Reject, unavailable permission UI, malformed responses, and
  timeouts all fail closed without opening a browser; the MCP server remains
  unavailable for that session.
- ACP still has no OAuth auth-URL event or in-client callback page. The native
  host opens the browser on the machine running Canopy and accepts the OAuth
  redirect on its loopback interface. Consent is requested while `session/new`
  or session restore is still being prepared, before that operation returns
  its session state; clients that cannot route `session/request_permission`
  during this phase will reject or time out the request, and login is skipped.
  Remote ACP clients cannot complete the loopback redirect on their own
  machine. If `session/cancel` arrives during the callback wait, the host stops
  the OAuth flow and does not exchange or store a token; the provider's
  detached callback listener can remain bound until its five-minute deadline.
  Source UI events and the `/mcp` authentication dialog are not ported.
  Stored-token fallback for servers without `oauth.enabled: true` is still
  missing outside the temporary retry config used after automatic authorization.
- Automatic callback listening supports HTTP loopback redirects. Other
  redirect schemes/hosts must be handled by the host and passed through
  `parse_callback_url`.
- Rust's Keychain adapter currently targets macOS; Windows and Linux use the
  encrypted-file fallback. Keychain access compiled successfully but has not
  been exercised against a live user keychain, and cross-runtime Keytar/Rust
  credential reads still need runtime verification. The selected protected
  backend implements `SecretStorage`, but Rust extension settings do not use it
  yet; cross-process file read/modify/write coordination is also not
  implemented.

## Verification

- `rustfmt --edition 2024` completed for the provider and module export.
- `cargo check -p canopy-core --locked --offline` completed successfully.
- The CLI token resolver and per-server connect hook pass
  `cargo check -p canopy-cli --locked --offline`.
- The automatic challenge-to-browser host path and explicit-Authorization
  precedence pass `cargo check -p canopy-cli --locked --offline`.
- ACP consent-gated OAuth startup, session cancellation, and retry integration
  pass `cargo check -p canopy-cli --locked --offline`.
- No tests were added or run for this port slice.
