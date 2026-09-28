# Proactive delivery error port

`proactive_delivery_error.rs` ports the disposition (`permanent` or
`transient`), stable error code, message, and optional cause from
`packages/channels/base/src/ChannelProactiveDeliveryError.ts`.

Rust callers classify the concrete error type. This is narrower than the
TypeScript guard, which also accepts any object with matching `code`,
`disposition`, and string `message` fields. The daemon delivery adapters that
produce and consume this error remain to be wired.

Three focused unit tests cover both dispositions, message/code retention,
source chaining, and rejection of unrelated errors.
