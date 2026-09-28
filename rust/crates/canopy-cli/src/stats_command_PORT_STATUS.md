# Native CLI stats command status

The interactive Rust CLI recognizes `/stats` and the `/usage` alias before
prompt preprocessing. The command module has a session-metrics entry point for
the TypeScript detail views, plus the token usage subcommands:

- `model`: API requests, errors and error rates, average latency, and per-model
  token counts. Model rows split by source when any non-`main` source exists.
- `tools`: total calls/success/fail, per-tool call counts, success rates, and
  average duration.
- `skills`: total calls/success/fail and per-skill call counts and success
  rates, sorted by descending call count.
- `daily [YYYY-MM-DD]` and `day [YYYY-MM-DD]`
- `monthly [YYYY-MM]` and `month [YYYY-MM]`
- `export <daily|monthly> [date|month] [--format csv|json] [--output path]`

The detail renderers consume `canopy_core::services::usage_history::SessionMetrics`
through `execute_with_session_metrics(runtime_base_dir, project_root, args,
Option<&metrics>)`. The interactive CLI supplies the active runtime's snapshot
at the `/stats` dispatch. The existing `execute` remains compatible with
daily/monthly/export dispatch. An absent snapshot makes the `model` and
`tools` views return an explicit unavailable error; callers should not
substitute a default metrics value. The `skills` view reports that native
skill statistics are unavailable.

`AgentRuntime` now owns a bounded in-memory collector and exposes
`session_metrics_snapshot(session_id) -> Option<SessionMetrics>`. It counts one
logical model request per runtime provider stream call after retries complete,
including failed/incomplete responses; its duration includes retry waits and
stream consumption. Tool counts, outcomes, and durations cover executor calls
and exclude dispatch and result finalization. Like TypeScript's
`uiTelemetryService`, these ephemeral session counters are collected even when
the usage-statistics privacy setting is disabled and include internal prompts.
The existing persisted token-usage journal remains gated by the privacy
setting and internal-prompt check. The collector retains aggregate numbers
and bounded model/source/tool names only, never prompt text, call arguments,
or tool/model output. It keeps at most 32 sessions, 64 model labels per
session, 16 sources per model, and 256 tool labels per session; overflow is
aggregated under an `other` label.

The views currently include model request/error/latency and token counts,
tool call/success/failure/duration, and an explicit unavailable state for
skills. Estimated model cost is not included because this call path does not
receive settings pricing. User decision counts and file-line changes are not
available in the native collector. The aggregate total tool duration is
included in `/stats tools`.

Summary and export data come from canopy_core::services::token_usage. Exports
default to CSV and to a generated filename in the project directory. Explicit
paths may be relative or absolute if they resolve within that directory. The
writer rejects traversal and symlink escapes, validates the existing parent
and target, creates a private same-directory temporary file, syncs it, and
renames it into place. Both line-mode and full-screen interactive sessions
display command results without sending them to the model.

The runtime snapshot is wired inside `run_interactive_prompt_loop`. ACP also
intercepts plain-text `/stats` and `/usage` commands before prompt processing
and emits a bounded response scoped to the active session. ACP's bare summary
shows session request, token, and tool totals; detailed commands use the same
stats handler. ACP replies are not persisted into the session transcript, and
an export cannot be interrupted once its synchronous file operation begins.
Each `AgentRuntime` snapshot covers only metrics accumulated during that
runtime instance; a resumed process does not restore older request/tool
counters. Native skill launch events are not available, so `/stats skills`
says so explicitly.

The native full-screen TUI now has an isolated live stats view with Session,
Models, Tools, and Skills tabs. It re-samples a caller-provided
`Option<SessionMetrics>` every 250 ms, bounds each detail list to 100 rows,
supports Tab/Shift+Tab and Left/Right tab switching, number-key selection,
vertical and page scrolling, and Escape/q close. A missing skill collection is
shown as unavailable; an explicitly present but empty collection is shown as
zero calls. The view includes current session totals, model/source request and
token details, and tool/skill call outcomes and durations where available.

Bare `/stats` and `/usage` open `ChatTerminal::show_live_stats` in the
full-screen terminal. The view re-samples the active runtime snapshot while
open. `model`, `tools`, `skills`, period, and export arguments keep their
existing text-rendered path. Line-mode sessions keep text output.

`canopy run --prompt "/stats …"` and `--prompt "/usage …"` now use the same
`execute_with_session_metrics` dispatcher as interactive detailed commands.
This interception occurs after the runtime is created, reads its current
session snapshot, prints the result to stdout (or errors to stderr), and does
not send the slash command to the model or trigger automatic memory extraction.
Only exact `/stats` or `/usage` command names are intercepted; ordinary prompts
and all other `--prompt` behavior are unchanged. In one-shot mode, bare `/stats`
prints dispatcher usage because there is no live dashboard. `model` and `tools`
can report unavailable when the runtime has not collected metrics; a resumed
process also does not restore earlier request/tool counters. `daily`, `monthly`,
and `export` continue to read the persisted token-usage journal. Native skill
statistics remain unavailable. Provider/model configuration is still initialized
by `canopy run` before dispatch, and an interrupted session still requires its
normal recovery flow before a new `--prompt` can run.

Historical activity charts, date-range selection, and the TypeScript efficiency
dashboard remain unimplemented.

The TUI slice and its command-loop wiring passed `cargo check -p canopy-cli
--locked --offline`; targeted formatting is also clean. The compile reports
three existing `mcp_host.rs` warnings. Tests were not run.
