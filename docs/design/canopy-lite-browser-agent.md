# Canopy Lite browser-agent profile

## Decision

Canopy Lite can execute real browser-control MCP workflows, but it should not
use the full Canopy Code tool catalog at the iOS chat context size. Keep the
iOS chat context at 12,288 tokens. Run Lite browser-agent sessions with at
least a 16,384-token server context and a task-specific MCP allowlist.

The tested browser allowlist is:

```json
[
  "browser_tab_new",
  "browser_navigate",
  "browser_snapshot",
  "browser_wait_for",
  "browser_click"
]
```

Set `alwaysLoadTools: true` for that server. For short browser-only runs,
exclude unrelated core tools (especially `tool_search`, file tools, and
subagent tools) so the model does not spend its first turn exploring the
repository or loading more schemas.

## Required behavior

The high-priority run instructions should require one named tab, the `tab`
field on every subsequent call, `networkidle` followed by a fresh snapshot,
semantic clicks from the snapshot, recovery in the same tab after errors, and
verification before reporting success. `CANOPY.md` contains the persistent
project version of these rules, but a default full CLI session can still drift;
the browser-agent launcher should append the focused protocol explicitly.

## Evidence — 2026-09-09

- At 12,288 tokens, the full Canopy Code tool catalog produced an initial
  request of about 18K tokens and was rejected by the Lite server.
- At 12,288 tokens, one eagerly loaded browser tool fit initially, but the
  request after reinjecting the tool result exceeded the context limit.
- At 16,384 tokens, Lite completed a real browser-control workflow: it opened
  `example.com`, waited, clicked `Learn more`, waited again, and verified the
  IANA destination heading `Example Domains`; warm decode was roughly
  15–16 tokens/second on this host.
- The same profile opened the signed-in App Store Connect subscription page
  and read the monthly product status as `Waiting for Review`.
- A run relying on persistent `CANOPY.md` guidance alone still skipped the
  required click, so the focused launcher prompt remains necessary for
  headless/browser-only sessions.
- A 32,768-token full-catalog run fit, but was too slow and began repeating
  tool turns; larger context alone is not the fix.

## Fine-tuning assessment

No QAT/fine-tuning change is justified by these tests. The failures were caused
by context/tool-surface overhead and missing execution constraints. Re-test
with the focused profile first; only consider training if Lite still fails
repeatedly when the required tools, context, and protocol are supplied.
