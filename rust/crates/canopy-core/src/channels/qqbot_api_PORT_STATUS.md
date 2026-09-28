# QQ Bot API Rust port status

Source: `packages/channels/qqbot/src/api.ts` and
`packages/channels/qqbot/src/api.test.ts`.

## Implemented

- Fixed token, production API, and sandbox API endpoints; `get_api_base` selects
  the same production or sandbox host.
- Access-token POST with the exact `appId`/`clientSecret` JSON keys and
  `Content-Type` header, nonempty token validation, and the 7200-second default
  expiration.
- Production/sandbox gateway GET with its `QQBot` authorization header,
  required response URL, strict `wss:` protocol and `.qq.com` hostname suffix,
  and userinfo removal from the returned canonical URL.
- Message POST with exact JSON body/header shape. It returns the response
  without interpreting HTTP status, matching the source's raw `Response`.
- Fixed 15-second default request timeout. Injectable transport functions take
  an explicit timeout and receive it in `QqbotHttpRequest`.
- HTTP error paths cancel the response body before returning a status-only
  error. Token failures log only the HTTP status; response content is not
  included in either error message.

Nine focused tests cover endpoint selection, request JSON/headers/timeouts,
token defaults and missing fields, status-only errors and body cancellation,
gateway validation/userinfo stripping, and raw message-response behavior.

## Verification and remaining integration

The isolated offline harness passed **9/9 QQ Bot API tests** (35/35 total when
run alongside the existing Weixin API/types/send-utility tests).
`rustfmt --check` and a trailing-whitespace scan passed. The harness is under `/tmp`;
workspace manifests and the lockfile were not changed.

`qqbot_api.rs` still needs an export from `channels/mod.rs`; the parent port task
owns that shared file. The QQ Bot Rust login/channel caller must connect token
and gateway acquisition to its lifecycle, and send callers must consume the
returned response body when needed. The Rust response wrapper provides
`json`, `read_body`, and `cancel_body` for that use.
