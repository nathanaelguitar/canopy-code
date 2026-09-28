#!/usr/bin/env bash
set -euo pipefail

# Build the Rust CLI and assemble the executable-adjacent layout used by
# resolve_bundled_skills_dir(): <package>/canopy plus <package>/bundled/skills.
if [[ $# -gt 1 ]]; then
  printf 'Usage: %s [cargo-target-triple]\n' "$0" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
skills_source="$repo_root/packages/core/src/skills/bundled"
extension_examples_source="$repo_root/packages/cli/src/commands/extensions/examples"
host_triple="$(rustc -vV | sed -n 's/^host: //p')"
target_triple="${1:-$host_triple}"

if [[ -z "$host_triple" ]]; then
  printf 'Could not determine the Rust host target triple.\n' >&2
  exit 1
fi
if [[ ! "$target_triple" =~ ^[A-Za-z0-9][A-Za-z0-9_.+-]*$ ]]; then
  printf 'Invalid Cargo target triple: %s\n' "$target_triple" >&2
  exit 2
fi
if [[ ! -d "$skills_source" ]]; then
  printf 'Bundled skill source directory is missing: %s\n' "$skills_source" >&2
  exit 1
fi
if [[ ! -d "$extension_examples_source" ]]; then
  printf 'Extension example source directory is missing: %s\n' "$extension_examples_source" >&2
  exit 1
fi

# Fix the default target directory so the copied binary path is predictable;
# retain an explicit Cargo override when a caller already set one.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$script_dir/target}"
case "$CARGO_TARGET_DIR" in
  /*) target_dir="$CARGO_TARGET_DIR" ;;
  *) target_dir="$script_dir/$CARGO_TARGET_DIR" ;;
esac

cd "$script_dir"
if [[ $# -eq 1 ]]; then
  cargo build --release --locked -p canopy-cli --target "$target_triple"
  binary_source="$target_dir/$target_triple/release/canopy"
else
  cargo build --release --locked -p canopy-cli
  binary_source="$target_dir/release/canopy"
fi

binary_name="canopy"
if [[ "$target_triple" == *windows* ]]; then
  binary_name="canopy.exe"
  binary_source+=".exe"
fi
if [[ ! -f "$binary_source" ]]; then
  printf 'Cargo built no CLI binary at expected path: %s\n' "$binary_source" >&2
  exit 1
fi

dist_dir="$script_dir/dist"
package_dir="$dist_dir/$target_triple"
mkdir -p "$dist_dir"
stage_dir="$(mktemp -d "$dist_dir/.package.XXXXXX")"
trap 'rm -rf "$stage_dir"' EXIT
mkdir -p "$stage_dir/bundled"
cp "$binary_source" "$stage_dir/$binary_name"
cp -R "$skills_source" "$stage_dir/bundled/skills"
cp -R "$extension_examples_source" "$stage_dir/examples"

# Replace only this script's generated target package; the staged copy keeps an
# interrupted build from leaving a half-populated package directory.
rm -rf "$package_dir"
mv "$stage_dir" "$package_dir"
trap - EXIT
printf 'Packaged %s at %s\n' "$target_triple" "$package_dir"
