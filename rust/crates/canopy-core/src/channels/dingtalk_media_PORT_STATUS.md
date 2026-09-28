# DingTalk media Rust port status

Source: `packages/channels/dingtalk/src/media.ts` and
`packages/channels/dingtalk/src/media.test.ts`.

## Implemented

- `download_media` uses a supplied pooled `reqwest::Client`; the injectable
  `DingtalkMediaHttpClient` transport powers the same implementation in tests.
- Step one POSTs JSON containing `downloadCode` and `robotCode` to the DingTalk
  download-code endpoint, with the access-token and JSON content-type headers.
  The API request has no added timeout, matching the source.
- The payload uses the top-level `downloadUrl` when non-null and otherwise
  falls back to `data.downloadUrl`. Step two performs an unauthenticated GET
  with a 30-second timeout.
- File responses default an absent or empty MIME type to
  `application/octet-stream`. Both advertised `Content-Length` and streamed
  chunks enforce the strict greater-than 50 MiB limit.
- Oversize rejection awaits body cancellation. Transport, read, parse, timeout,
  and cancellation errors return `None`, matching the source's catch behavior.

## Verification and remaining integration

The module has 13 offline tests covering request shape and headers, direct and
nested URL selection, HTTP/JSON failures, MIME and body fallback, 50 MiB
constant, size boundaries, JavaScript `parseInt` behavior, and awaited
cancellation on both size rejection paths. All pass in an isolated offline
Cargo harness using the workspace `bytes`, `futures-util`, `reqwest`,
`serde_json`, and `tokio` dependencies.

The Rust channel module registry must add `pub mod dingtalk_media;` to
`rust/crates/canopy-core/src/channels/mod.rs`. No DingTalk adapter call site is
wired yet.
