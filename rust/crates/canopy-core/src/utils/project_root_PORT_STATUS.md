# Project root utility port status

`project_root.rs` ports nearest-ancestor `.git` detection. It accepts a `.git`
directory or regular file, uses `symlink_metadata` so symlinks do not count,
normalizes the starting path without resolving symlinks, and terminates at the
filesystem root. The async API is `find_project_root(start_dir) -> Option<PathBuf>`.

The Rust helper quietly treats every metadata error as a missing marker. The
TypeScript helper also continues upward but logs non-`ENOENT` errors outside
tests; logging is left to callers. Hierarchical memory discovery reuses this
helper, preserving its non-following `.git` behavior instead of maintaining a
private project-root walk.
