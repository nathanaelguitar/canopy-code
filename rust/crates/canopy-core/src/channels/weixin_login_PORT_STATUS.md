# Weixin login port

`weixin_login.rs` ports `packages/channels/weixin/src/login.ts`. It requests
QR codes at `/ilink/bot/get_bot_qrcode?bot_type=3`, reports the returned QR
image URL and scan prompt through an injected status-output interface, and
polls `/ilink/bot/get_qrcode_status?qrcode=...` with
`weixin_api::build_headers(None)`. QR IDs use `encodeURIComponent`-compatible
UTF-8 escaping.

The polling loop uses an injected wall clock and sleeper. Its default deadline
is eight minutes, each status request has a 60-second abort timeout while
waiting for response headers, and each non-abort status iteration waits one
second before polling again. As in the source, the timeout is cleared once
fetch returns, before response JSON is read. Confirmed responses return the
bot token/base URL/user ID; `scaned` reports the waiting message; and the third
`expired` status returns the max-retries result, refreshing the QR after the
first two. Abort errors immediately continue the loop; other HTTP, network, and
JSON errors propagate.

The HTTP, clock/sleeper, and output traits support offline tests. Production
uses reqwest, Tokio timers, the system clock, and stderr. Rust returns typed
optional credential fields in `LoginResult`; non-string JSON values for those
source-typed string fields are treated as absent. The source repository has no
`login.test.ts` file, so the focused Rust suite exercises the behavior directly.

Nine focused tests cover QR request/output and failures, confirmed credentials
and request headers/timeout, `scaned` and unknown statuses, QR refresh through
three expirations, the eight-minute default and shorter deadlines, abort versus
other errors, and QR URL encoding.

Verification: the standalone offline Cargo harness passed **26/26** tests,
including nine login tests and the exported API's 17 focused tests;
`rustfmt --check` and scoped `git diff --check` passed. The harness is under
`/tmp` and no manifest or lockfile was changed. Integration needs
`pub mod weixin_login;` in `channels/mod.rs`.
