# Feishu media download port

`feishu_media.rs` ports `packages/channels/feishu/src/media.ts`. It validates
message and file IDs against Feishu's ASCII ID allowlist before constructing the
resource URL, sends a bearer-authenticated GET with a 30-second request
timeout, and returns the response bytes with its MIME type. Invalid inputs,
transport errors, response-read errors, non-success HTTP statuses, missing
bodies, and size-limit violations return `None`.

The downloader checks a parseInt-compatible `Content-Length` value before
reading and also enforces the 50 MiB limit while consuming streamed chunks.
Oversized bodies are canceled through an awaited async body seam; teardown
errors flow to the same null-on-failure path. HTTP error response text is read
for diagnostics, with read failures treated as an empty detail. Missing or
empty `content-type` falls back to `application/octet-stream`.

`download_media` accepts a pooled `reqwest::Client`. The public
`download_media_with_http` seam lets local/mock transports exercise the same
implementation without external HTTP. Reqwest releases a response by dropping
its byte stream, which is synchronous and cannot report a teardown error; the
async seam preserves awaited/fallible cancellation for transports that expose
it, and focused tests verify cancellation is awaited and errors are caught.

Focused coverage is in nine unit tests: successful bytes and MIME with URL,
bearer, and timeout assertions; path/empty ID rejection; HTTP failures and
error-body read failure; Content-Length and streamed overflow; pending and
failing cancellation; read/request failures; absent bodies; and MIME fallback.
Size-limit tests use a private injected limit to avoid allocating 50 MiB test
buffers and assert the production cap remains exactly 50 MiB.

Verification: the isolated offline Cargo harness passed **9/9** tests;
`rustfmt --check` and `git diff --check` passed. The harness is under `/tmp` and
does not alter workspace manifests or the lockfile.
