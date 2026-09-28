# Tool Result Boundary Diagnostics Port Status

Source: `packages/core/src/utils/tool-result-boundary-diagnostics.ts` and its Vitest tests.

The Rust module preserves JSON-string UTF-8 threshold accounting, UTF-16 code-unit length-prefixing for HMAC values, value and identifier redaction, tool-name canonicalization, artifact state/kind normalization, per-representation slots, and rate-limit window/suppression behavior.

Parity audit found that the Rust observer reserved a log slot and cleared pending suppression counts before invoking the injected HMAC-key/context providers. A provider panic was caught, but incorrectly consumed quota and lost the count. The observer now reserves only after provider, measurement, and HMAC work succeeds, while keeping the serialization and slot reservation protected against concurrent observers. Added a regression test proving provider failure leaves the slot and suppressed count available for the next event.

`rustfmt --edition 2024 rust/crates/canopy-core/src/services/tool_result_boundary_diagnostics.rs` passed. Cargo tests were held for the parent agent's consolidated run.

Remaining interface differences: the Rust module is provider-neutral and requires callers to inject enabled checks and session/prompt context; runtime boundary integration is not part of this slice. Rust `String` cannot represent lone JavaScript UTF-16 surrogates, so their exact raw-byte, JSON-byte, and HMAC treatment is not portable through this API. Rust's default process key uses two UUIDv4 values (244 random bits) rather than Node's 32 random bytes (256 bits); both remain process-local and are only used as HMAC keys. JavaScript proxy/throwing artifact inputs also cannot be represented by the typed Rust input structs.
