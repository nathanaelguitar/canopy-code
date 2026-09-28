# External-context Rust port

The standalone canopy-external-context binary implements the version 1
stdio MCP surface:

- Strict version 1 and version 2 configuration validation, credential lookup,
  loopback-only plain HTTP validation, bounded config reads, and provider
  selection.
- context_search for Generic HTTP Search V1 and Mem0 Platform V3, bounded
  responses, redirect rejection, provider timeouts, item validation, trusted
  output limits, JSON-RPC tools/list and tools/call, and request cancellation.
- Optional Mem0 context_remember, Unicode content validation, the exact
  Direct Import request, and distinct stored/accepted/failed/unknown outcomes.

The version 2 `auto-recall` executable implements the TypeScript
`UserPromptSubmit` Hook contract. It accepts only the submitted-prompt
provenance field, resolves and bounds the working directory to the configured
repository root, sanitizes the query, and emits the same additional-context
envelope. It caps Hook input at 1 MiB, sanitizer input at 4,096 Unicode scalar
values, query text at 512, and the full process lifetime at 6.5 seconds. The
provider timeout remains bounded by the version 2 config (1–5 seconds); an
expired timeout cancels the request future and all failures emit only `{}`.
Build the Hook with `npm run build:rust:auto-recall --workspace
@qwen-code/external-context`, then use the Rust-specific managed settings
example for the target platform.

The local REST provider Extension example now has a Rust MCP executable in
addition to its TypeScript source. The Rust binary reads the same
`PROVIDER_CONTEXT_BASE_URL` and `PROVIDER_CONTEXT_TOKEN` environment variables,
posts the same `query`/`limit` body to `/v1/context/search`, and exposes the
same retrieval-only `context_search` profile under the example's server name.
Build it with `cargo build --manifest-path
integrations/external-context/rust/Cargo.toml --locked --release --bin
provider-context-local-example`. A Rust-specific managed MCP settings example
selects this binary. The TypeScript Extension manifest remains the default for
the copyable example.

`npm run build:rust:extension --workspace @qwen-code/external-context` now
builds and stages a linkable native extension under
`integrations/external-context/dist/rust-extension-<target>/`. It creates a
`canopy-extension.json` from the TypeScript manifest metadata and copies in
the Rust MCP executable. The original `qwen-extension.json` and TypeScript
`dist/main.js` entrypoint are not changed. The builder accepts Cargo's target
triple through `-- --target <triple>` or `RUST_TARGET`, and emits an explicit
`rust-extension-target.json` with the chosen target.

The local REST provider example has a separate package path:
`npm run build:rust:provider-context-extension --workspace
@qwen-code/external-context` stages
`integrations/external-context/dist/rust-provider-context-<target>/`. Its
generated manifest keeps the `provider-context-local-example` server name,
environment-variable map, timeout, and tool allowlist from the TypeScript
manifest. It removes only the TypeScript `dist/main.js` launcher argument and
preserves any other args. The TypeScript manifest and entrypoint remain
unchanged.

This output is target-specific by design. The extension manifest has one fixed
`command` and no operating-system or architecture selector; Rust executables
have target-specific formats and suffixes, so one manifest cannot choose a
portable binary at install or launch time. Build a separate package for every
supported target. Each generated folder can be linked with `qwen extensions
link <path>` or archived by the release process. The script does not publish
or sign those target archives.

The Rust client now installs an explicit proxy matcher using the TypeScript
Undici variable precedence (`http_proxy` before `HTTP_PROXY`, `https_proxy`
before `HTTPS_PROXY`) and HTTPS-to-HTTP proxy fallback. It parses and validates
proxy URLs at client startup, supports HTTP, HTTPS, and SOCKS5 proxy endpoints,
and checks the current `NO_PROXY`/`no_proxy` value on each request with
Undici's exact-host, subdomain, wildcard, and optional-port matching rules.

The implementation uses a 1 MiB MCP input-frame limit and a 16-request
in-flight cap to bound process memory. These are Rust-side resource limits;
the TypeScript wrapper's deployed SDK currently owns its frame and dispatch
limits.

`cargo check --manifest-path integrations/external-context/rust/Cargo.toml
--locked --offline` passes, including the auto-recall and MCP binaries. It has
not been exercised against an MCP client or live provider. No tests were added
or run.
