# Hook event input builder port status

`event_inputs.rs` ports the payload construction and matcher-context selection
from all 22 public `fire...Event` methods in the TypeScript hook event handler.
It preserves common-field insertion order, event-specific field order,
TypeScript default values, omission of undefined optional properties, the
truthy `tool_call_id` rule, submitted-prompt trimming checks, and matcher
context fields.

The host must supply `HookBaseInput` (including its timestamp). Current
background-task and cron snapshots are passed to each Stop or SubagentStop
builder call, so they are sampled at event time rather than builder creation.
Stop context-usage values are injected as the source's three optional numeric
properties. The host remains responsible
for config access and live snapshot creation. The module is exported from
`hooks/mod.rs` and passes `cargo check --workspace --locked`. Hook dispatch,
abort handling, logging, telemetry, and result aggregation remain separate.
