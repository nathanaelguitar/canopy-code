# Browser-control MCP

Canopy Code uses the `browser-control` MCP server for browser automation. It is
the preferred path for web tasks because it exposes a named-tab model and
browser-native observations instead of making the agent guess from screenshots.

## Source of truth

The maintained fork lives alongside this repository in `repos/browser-control`
and is published at `nathanaelguitar/browser-control`. Canopy's MCP
configuration points at that checkout's `scripts/mcp-wrapper.sh` rather than a
checked-in or globally installed binary.

The wrapper builds the current Rust binary with Cargo and then starts its MCP
server. This is intentional: it prevents a stale `target/release` binary from
silently running after source changes. If browser behavior looks inconsistent,
check the fork's source, wrapper, and build output together before debugging the
agent prompt.

The fork contains Canopy-specific integration work for semantic clicks,
covered-label recovery, file inputs, PDF/data URLs, tab aliases, dropdown
verification, and the self-building MCP wrapper. Keep those changes in the fork
when reviewing upstream updates; do not replace the fork wholesale with an
upstream release.

## Operating rules

For a browser task:

1. Use the MCP server named `browser-control` and create one named tab.
2. Reuse that tab for the rest of the task. Do not open a replacement tab to
   work around an error.
3. After navigation, wait for `networkidle` and take a fresh snapshot.
4. Prefer the accessible role and visible name from the snapshot. Use CSS only
   when there is no reliable semantic target.
5. Treat tool errors as recoverable. Correct the request in the same tab and
   verify the result with a later snapshot.
6. Copy visible labels exactly. A URL, record number, or internal DOM value is
   not a substitute for the user-facing value.

Canopy's generated model tool names use the `mcp__browser-control__` prefix.
The repository-level guidance in [`CANOPY.md`](../../../CANOPY.md) is the short
version of these rules.

## Upstream review policy

The fork was compared with `rickardp/browser-control` during the 2026-09-18
maintenance pass. The upstream additions were useful reference material, but a
wholesale merge would risk replacing the local wrapper, extension integration,
and source-of-truth behavior. The following ideas remain candidates for a
separate, tested change rather than an automatic upgrade:

- native accessibility snapshots and input support;
- passive console/network capture;
- foreground emulation and keyboard-series helpers.

Any future update should be applied selectively, followed by the fork's Rust
tests and a real MCP smoke test against a named tab.

## Verification evidence

The local fork's library suite passed with 292 tests on 2026-09-18. A temporary
checkout of the upstream revision passed 389 library tests. The full upstream
integration suite was not allowed to run to completion because it launches
long-lived browser processes, so this is not a claim that every browser
integration path was exhaustively tested.

## Canopy changes recorded today

The same maintenance pass also covered:

- exact remote-control session routing and `Mac`/`Spark` session labels;
- resilience to detached terminal output in the TUI;
- persistent advisor mode and restart restoration;
- ChatGPT OAuth GPT model selection in `/models` and `/advisor`;
- large-context compression fitting for self-hosted/Qwen sessions near a
  256K-token limit.

The CUA/Apple screen-understanding investigation is recorded separately in
[`docs/research/2026-09-18-cua-apple-screen-understanding.md`](../../research/2026-09-18-cua-apple-screen-understanding.md).
