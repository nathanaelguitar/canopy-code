# Token usage service port status

Rust module `services::token_usage` ports the per-request `token-usage-YYYY-MM.jsonl` record contract, local date/month buckets, non-negative token fallback rules, day/month query aggregation, deterministic groups, and JSON/CSV exports from `packages/core/src/services/tokenUsageService.ts`.

Hosts provide the runtime directory, session ID, response usage fields, and current time. Writes use the shared serialized, synced JSONL append path. The CSV formatter retains the source spreadsheet-formula prefix defense.

Native `AgentRuntime` now records successful provider turns with usage metadata through a best-effort blocking write, so filesystem telemetry cannot fail a model turn or block the async executor. ACP and interactive CLI map `privacy.usageStatisticsEnabled` (default true), the active provider auth type, and their respective `acp`/`main` source labels into the runtime config. The runtime excludes IDs recognized by the internal-prompt filter; current foreground runs use generated UUIDs, while native background prompt-ID propagation is not implemented.

The native interactive CLI now exposes `/stats` and `/usage` for model/tool
details, daily/monthly summaries, and CSV/JSON export; see
`rust/crates/canopy-cli/src/stats_command_PORT_STATUS.md`. Native skill metrics
are unavailable, and the full TypeScript stats dialog, non-interactive
dispatch, and ACP command dispatch remain incomplete. The Rust event input accepts RFC 3339
timestamps (the source telemetry emits ISO 8601); invalid timestamps retain
the provided string but use the supplied current time for local date bucketing.
Group tie sorting uses Rust string order instead of JavaScript locale
collation. Tests were not run or added for this slice.
