# Conditional rule injection status

The native CLI and ACP server build a session-scoped
`ConditionalRulesRegistry` from startup memory's conditional rules and project
root. After each successful tool response is finalized, `WorkspaceTools`
combines filesystem path candidates from the tool arguments with result paths
reported by `glob` and `grep`, deduplicates them in first-seen order, and
consumes matching rules once. Only tools accepted by the filesystem-path
allowlist contribute candidates. The runtime adds reminder wrapping and
escaping after output truncation.
