# ACP generation stream queue port status

`generation_stream.rs` ports the request-scoped `GenerationStreamQueue<T>`
from `packages/acp-bridge/src/generation-stream.ts`. The Rust handle provides a
bounded FIFO, non-blocking `push`, idempotent close, failure propagation,
`recv().await`, and a `futures_util::Stream` adapter. Buffered values drain
before close or failure; failure remains observable on later receives; and a
second simultaneous pending receiver gets an explicit error without disturbing
the first. A zero-capacity queue preserves the source's rendezvous behavior.

Rust cancellation can drop a pending receive future, which JavaScript promises
cannot do. The waiter is therefore unregistered on cancellation, and a racing
producer recovers the value into the bounded queue when space remains. The
module is standalone and is not declared or re-exported by `acp_bridge::mod.rs`
yet; the ACP bridge integration owner will wire it after this port lands.

Verification for this slice: `rustfmt --edition 2024` and `git diff --check`.
No tests were added or run.
