# Weixin typing lifecycle port status

## Implemented

`weixin_typing.rs` ports the module-wide `typingTickets` cache and the
`startTyping`, `stopTyping`, and `setTyping` behavior from
`packages/channels/weixin/src/WeixinAdapter.ts`. The default controller shares
a process-wide cache keyed only by user ID. A caller can inject a cache for
tests or an explicitly shared cache for multiple channel instances.

`set_typing` uses the existing `GetConfigResp`, `SendTypingReq`, and
`TypingStatus` types. It fetches the user's context token through an injected
lookup, caches only non-empty tickets, reuses cached tickets, constructs the
typing/cancel request, and turns API failures or missing tickets into `false`.
`ReqwestWeixinTypingApi` delegates to the shared retrying `weixin_api` helpers.

The lifecycle controller deduplicates active starts/stops, clears active chats
on disconnect, allows failed starts to retry, repeats cancel if a successful
start resolves after terminal cleanup, and checks connection identity after a
start resolves so a late result from a disconnected/replaced connection does
not send a post-disconnect cancel.

## Focused coverage

Nine Rust tests cover context-token lookup and request fields, ticket reuse
within and across controllers, the default process-wide cache, missing tickets,
best-effort config/send failures, failed-start retry, duplicate start/stop
events, terminal-before-start-resolution cleanup, disconnect clearing, and the
disconnect/reconnect generation guard.

## Caller wiring still required

- Construct `ReqwestWeixinTypingApi` with the channel's shared reqwest client,
  base URL, and token; supply `weixin_monitor::get_context_token` as the context
  lookup.
- Call `connect()` when the adapter installs its active connection controller;
  call `disconnect()` when it aborts that connection. Map `onPromptStart`,
  `onPromptEnd`, and task lifecycle events to the controller methods. The
  caller should map the source's complete terminal-event predicate to
  `TypingLifecycleEvent::Terminal`.
- The controller uses Tokio fire-and-forget tasks, so event methods need an
  active Tokio runtime. The current `ChannelBase` adapter wiring is not part of
  this slice.

## Verification

- `rustfmt --edition 2024` completed on `weixin_typing.rs`.
- An isolated harness ran with
  `cargo test -p canopy-core --test weixin_typing_harness --offline` from
  `rust/`: 30 passed, 0 failed (9 typing tests plus 21 existing Weixin API and
  wire-type tests). The temporary harness was removed after the run; shared
  exports and manifests were not changed by the worker.
- The module is now exported from `channels/mod.rs`; its 9 in-file tests pass
  in the integrated channel suite. The latest full offline workspace run
  passed 1,412 tests.
