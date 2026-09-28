# Native Git branch operations

`git_branch_operations.rs` provides reusable Rust implementations of the
TypeScript Git checkout, branch creation, push, pull/fetch, and commit helpers.
Checkout and branch names are validated before being passed as argv. Remote
tracking checkouts create a local tracking branch when needed; branch creation
checks for an existing target before rollback, then restores the original branch
and removes only a partially created branch if a post-checkout failure leaves
Git switched to it. Push-with-upstream preserves an existing upstream and
otherwise resolves the configured push remote in Git's precedence order.
Commit-with-all snapshots the index before staging everything and restores the
snapshot if staging or commit fails.

Each Git child uses a sanitized environment, a 30-second deadline, and 10 MiB
stdout and stderr retention limits. Processes are started without a shell.
Route handlers still need to validate request bodies, recheck workspace trust,
redact workspace paths from Git errors, and apply daemon mutation admission
before calling these helpers.

The helpers are registered from `canopy_core::git_branch_operations`.
Workspace-qualified HTTP mutations are not wired yet. No tests or Cargo checks
were run specifically for this module.
