# ACP fatal diagnostic reports port status

`fatal_diagnostic_reports.rs` ports the fatal-report directory controls from
`packages/acp-bridge/src/fatalDiagnosticReports.ts`:

- `CANOPY_DISABLE_FATAL_REPORTS=1` disables both Rust panic reporting and the
  Node startup flags returned for ACP children.
- `CANOPY_FATAL_REPORT_DIR` overrides the destination when it is non-empty;
  otherwise reports use `~/.canopy/fatal-reports`. Relative paths resolve
  against the current working directory.
- Startup setup is best-effort. A directory creation or panic-hook setup
  failure does not prevent the ACP process from starting.
- On Unix, the directory is tightened to mode `0700` and individual report
  files are created with mode `0600`. Permission tightening is attempted but,
  as in the TypeScript implementation, failure to chmod an ACL-managed or
  read-only directory is non-fatal. The standard library does not expose a
  portable owner-only ACL operation on non-Unix platforms.
- Node ACP child arguments retain `--report-on-fatalerror`,
  `--report-exclude-env`, `--report-exclude-network`, and the resolved report
  directory.

Rust panic reports contain a timestamp, process ID, and symbolized backtrace.
Panic payloads and the explicit panic location are omitted because arbitrary
panic text can include user data or secrets. Symbolized backtraces can include
local source paths, but no environment variables, network data, or credentials
are collected. Panic hooks do not cover every termination: explicit aborts,
process kills, and many out-of-memory failures may terminate without a usable
panic report. Node fatal reports are produced by Node's own runtime and retain
its corresponding abort/OOM limitations.

This module has not yet been wired into `acp_bridge::mod` or the ACP startup
path. Formatting and whitespace checks are the requested verification for this
slice; no tests were added or run.
