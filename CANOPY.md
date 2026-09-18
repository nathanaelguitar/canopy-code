# Canopy Code project guidance

## Browser-control tasks

When a task asks for browser work, use the MCP server named `browser-control`
directly. Do not inspect repository files, invoke `tool_search`, or use a
different tool unless the task requires it.

- Create one named tab with `browser_tab_new`, including the requested `url`
  when that field is available.
- Reuse the returned tab name in the `tab` field for every later call. Do not
  mix `tab` and `target`, and do not create a second tab to recover from an
  error.
- After navigation, wait for `load_state: networkidle`, then take a fresh
  `browser_snapshot`. A sparse first snapshot is not proof that the page is
  empty or unavailable.
- For clicks, use the visible accessible name and role from the snapshot
  (`element` plus `role`); use a CSS selector only when the semantic target is
  genuinely unavailable.
- Treat tool errors as recoverable: correct the arguments and continue in the
  same tab. Never claim an action succeeded until a later snapshot verifies
  the resulting state.
- When extracting data, copy values from their visible labels exactly. Never
  substitute a numeric URL/record ID for a labeled Product ID, and do not add
  claims (such as approval, purchase, or activity) that the page does not show.

Canopy Code prefixes these MCP tools as `mcp__browser-control__...` in model
tool calls. Keep browser-only runs focused and report only verified results.
