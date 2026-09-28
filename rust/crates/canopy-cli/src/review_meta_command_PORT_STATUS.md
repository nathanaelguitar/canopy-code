# Native `canopy review meta` port status

`review_meta_command.rs` implements the standalone `meta [pr_number]` review
subcommand. It emits one JSON object on stdout with the GitHub platform, host,
and canonical `ownerRepo`. When a positive integer PR number is supplied, it
also includes the PR number, live head SHA, and canonical web URL.

## Behavior implemented

- Resolves the current repository with `gh repo view --json
owner,name,url,parent`. When the result is a fork, it uses the parent
  owner's repository name while deriving the host from the fork's own URL,
  matching the TypeScript GitHub reader.
- Accepts `--repo owner/repo` to skip local repository discovery and `--host`
  to route `gh` at a GitHub Enterprise host. Without `--host`, the inherited
  `GH_HOST` is honored; without either, the discovered host or `github.com`
  supplies the output label.
- Uses argument arrays with `std::process::Command`, never a shell. Runs
  `gh auth status` before repository or PR reads. Auth failure is reported as
  `gh CLI is not authenticated. Run \`gh auth login\` and retry.`
- Validates a supplied PR number as a positive integer before host validation
  or authentication. It validates `--host` and `--repo` before the auth gate,
  preserving the TypeScript invocation-repair behavior.
- Validates the effective routed host even when it came from `GH_HOST` or a
  discovered repository URL. An unroutable environment/discovered host is a
  runtime error; a malformed `--host` flag is a usage error.
- Retries read commands up to twice for HTTP 5xx and the transient server
  messages recognized by the TypeScript `gh` helper. Auth status retries once
  after two seconds, except when `gh` is missing. Captured output above 64 MiB
  is rejected; `Command::output` buffers it before this size check.
- `ReviewMetaError` keeps usage failures at exit code 2 and authentication,
  environment, and GitHub failures at exit code 1. `diagnostic()` formats the
  source-compatible `meta: ...` stderr prefix for the dispatcher.

## Differences and limits

- The TypeScript `platform/registry.ts` currently returns only the GitHub
  reader. This Rust slice hardcodes `platform: "github"`; there is no native
  review-platform registry yet.
- `gh` failure text is reconstructed from stderr and exit status, so it may
  differ from Node's `execFileSync` error message. The network/CLI failure
  remains exit 1 and the auth failure remains actionable.
- PR numbers are represented as positive `u64` values. Real GitHub PR numbers
  are far below this bound; JavaScript can represent some larger integral
  `Number` values that the native argument parser rejects.
- Missing `headRefOid` or `url` properties are omitted from JSON. Normal `gh`
  responses provide both fields.
- No live GitHub request, tests, or build was run for this port.

## Dispatcher integration

Add `mod review_meta_command;` to `main.rs` and route `canopy review meta`
arguments after the `meta` token to `review_meta_command::run(&args)`. The
module returns `ReviewMetaError`; the dispatcher must write
`error.diagnostic()` to stderr and propagate `error.exit_code()`. The current
top-level `main.rs` converts every `Result<(), String>` error to exit 2, so
preserving the source exit-1 runtime class requires a small dispatcher/top-level
exit-code adjustment. This module and note do not edit `main.rs`.
