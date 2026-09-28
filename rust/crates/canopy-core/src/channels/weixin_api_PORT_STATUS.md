# Weixin API port

`weixin_api.rs` ports the exported helpers from
`packages/channels/weixin/src/api.ts`: header construction, updates polling,
message/config/typing calls, upload URL lookup, and CDN upload. JSON request
bodies preserve the source field names and include `base_info.channel_version`
`2.1.3`. API responses remain `serde_json::Value` so unmodeled protocol fields
pass through without a Rust-only response schema.

The reqwest-backed functions accept a pooled `Client`; matching
`*_with_http` entry points use an injected transport and runtime. The runtime
supplies random UIN bytes and a retry sleeper. Headers include the content type,
bot app ID, encoded protocol version, a fresh CSPRNG UIN, and bearer
`AuthorizationType`/`Authorization` headers only for a nonempty token.

Requests have the source's 40-second timeout. `get_updates` accepts an optional
timeout override and cancellation token, and converts either abort to an empty
response retaining the polling cursor. Dropping the timed-out/cancelled reqwest
future cancels its in-flight request. `send_message`, `get_upload_url`, and
`upload_to_cdn` make up to three retries after the first attempt, with injected
1, 2, and 4 second waits. Network failures and HTTP 5xx/429 retry; errcode -1
and 45011 override other fields as retryable, errcode -14 is never retried, and
other nonzero `ret` values prevent retry. Timeouts, cancellation, successful
HTTP bodies with invalid JSON, and other local errors do not retry.

`get_upload_url` checks `ret`/`errcode` before selecting a URL and prefers
`upload_full_url` over `upload_param`. CDN uploads send the encrypted bytes as
`application/octet-stream`, parse the `x-encrypted-param` result header, and
validate full URLs as HTTPS with the exact `novac2c.cdn.weixin.qq.com` hostname
before sending. Host validation follows the source's `URL.hostname` check, so a
port is not part of the hostname comparison. Parameter-only values are
percent-encoded with `encodeURIComponent` rules when the upload URL is built.

Seventeen focused offline tests cover every exported endpoint's request shape,
headers and UIN, response parsing, timeout/cancellation, retry classification
and the 1/2/4-second schedule, upload URL precedence/fallback, CDN request
encoding/header/body, case-insensitive HTTPS, HTTP and hostile-host rejection,
CDN failure retries, and missing response headers.

Verification: the isolated offline Cargo harness passed **17/17** tests;
`rustfmt --check` and scoped `git diff --check` passed. It uses existing
workspace dependencies only and lives under `/tmp`; no manifest or lockfile
was changed. Integration needs `pub mod weixin_api;` in `channels/mod.rs`.
