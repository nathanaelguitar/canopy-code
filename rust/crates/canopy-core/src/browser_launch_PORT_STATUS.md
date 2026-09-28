# Secure browser launch port status

Implemented in `browser_launch.rs`:

- HTTP and HTTPS URL validation, optional `file:` access with an exact resolved-path allow-list, and control-character rejection.
- `BROWSER` command tokenization, quote grouping, `%s` substitution, the `www-browser` blocklist, detached launch, and fallback to native platform openers.
- CI, noninteractive Debian, SSH, and Linux display checks, plus manual-open warnings when automatic launch is unavailable or fails.
- Public APIs: `open_browser_securely`, `open_browser_securely_with_options`, `should_launch_browser`, `should_attempt_browser_launch`, `should_attempt_browser_launch_with_options`, and `is_browser_command_blocked`.

Remaining parity limits:

- The native CLI and ACP use the secure opener through the Artifact tool's
  optional auto-open path; other TypeScript call sites such as docs, insights,
  extension pages, and the web shell have not been migrated.
- The API is synchronous. Detached launches return after process creation, while platform opener and fallback commands wait for process exit.
- Launch failures are written to stderr; the source TypeScript helper writes to the console warning stream.
- File allow-list comparison is lexical, matching `path.resolve` behavior; it does not resolve symlinks.
