# Native memory diagnostics command status

The Rust interactive prompt exposes `/doctor memory`, matching the Node
command's process-memory report. It uses the native RSS, CPU, peak-RSS, file
descriptor, process-tree, and host/cgroup memory probes. `--sample` collects
three RSS readings one second apart, and `--json` returns the bounded
diagnostic object and optional samples. The report classifies RSS against the
same configured soft, hard, and critical limits used by native pressure
cleanup. Slash completion and TUI help list the command. The ACP command
catalog also publishes `/doctor memory`; its output is sanitized and capped.
The ACP prompt path recognizes `/doctor rollback` but returns the TypeScript
ACP-mode error instead of changing the installation. Interactive rollback
still uses the standalone updater.

The native runtime has no V8 heap, Node external-memory counters, JavaScript
active handles, or heap object graph. The report labels V8 counters unavailable
and does not implement the Node `--snapshot` option. General `/doctor` checks
and CPU profiling remain unported. `/doctor rollback` is available for detected
standalone installations, with manual recovery guidance on Windows.
