# Rust LLM framework evaluation

## Decision

Keep Canopy's current Rust `AgentRuntime` and provider boundary while the Rust
port proceeds. Do not replace the runtime with Rig. If reducing handwritten
provider protocol code becomes a priority, evaluate `rust-genai` as a provider
client behind the existing runtime boundary, one provider at a time.

This preserves Canopy's tool loop, permission decisions, session persistence,
cancellation, retry policy, and bounded-stream behavior while allowing a
provider-client experiment to be reversible.

## What the alternatives provide

| Project                                                 | License and current release                                                                               | Scope                                                                                                                                        | Fit for Canopy                                                                                                                                                                                                 |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Rig](https://github.com/0xPlaygrounds/rig)             | `rig`, `rig-core`, and `rig-agent` 0.42.0 are MIT; released 2026-08-17.                                   | `rig-core` supplies provider, message, stream, and tool contracts. `rig-agent` supplies an agent loop, tools, hooks, and streaming.          | Replacing Canopy's runtime would require translating or reimplementing its existing session, permission, cancellation, retry, and tool behavior. The provider contracts alone do not remove that adapter work. |
| [rust-genai](https://github.com/jeremychone/rust-genai) | Stable 0.6.5 is MIT OR Apache-2.0. The newest prerelease listed by docs.rs is 0.7.0-beta.24 (2026-09-23). | A provider client for native OpenAI, Anthropic, Gemini, Ollama, Bedrock, and other APIs, with streaming and custom endpoint/auth resolution. | A narrower provider-only experiment can leave `AgentRuntime` intact, but still needs translation between Canopy's request and event types.                                                                     |

Primary metadata: [Rig core manifest](https://docs.rs/crate/rig-core/0.42.0/source/Cargo.toml.orig), [Rig agent manifest](https://docs.rs/crate/rig-agent/0.42.0/source/Cargo.toml.orig), [Rig 0.42.0 release](https://github.com/0xPlaygrounds/rig/releases/tag/rig-v0.42.0), [genai 0.6.5 manifest](https://docs.rs/crate/genai/0.6.5/source/Cargo.toml.orig), and [genai API documentation](https://docs.rs/genai/0.6.5/genai/).

## Repository constraints

The workspace is Apache-2.0 and currently uses `reqwest` 0.12. `AgentRuntime`
selects Canopy's OpenAI-compatible, Anthropic, and Gemini clients and owns the
tool loop. The provider clients enforce request and response byte caps, SSE
event limits, connect/request timeouts, stream idle and lifetime limits, and
retry behavior. These are runtime requirements for any adapter, not optional
polish.

`rust-genai` 0.6.5 depends on `reqwest` 0.13. Adopting it as-is would either
add a second reqwest version and HTTP client stack, or require a deliberate
workspace-wide reqwest upgrade. Its custom-reqwest injection API does not make
the existing 0.12 client type compatible with 0.13. The default TLS features
and feature selection also need review for binary size and target packaging.

Neither library is evidence that Canopy or CUA will use less memory or stop
crashing. The runtime and CUA process need separate RSS measurements; a provider
client change should be judged on protocol parity and memory measurements, not
framework reputation.

## Next evaluation step

If a provider-client experiment becomes worthwhile, make a small `rust-genai`
0.6.5 adapter for one provider behind `AgentRuntime`. Compare request construction,
tool-call streaming, reasoning and finish events, cancellation, retry behavior,
timeouts, body/event bounds, custom endpoints, and proxy handling against the
current client. Measure Canopy and CUA RSS separately. Keep it out of the
default runtime until those comparisons show equivalent behavior and an actual
benefit.
