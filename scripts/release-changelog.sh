#!/usr/bin/env bash
# cargo-release pre-release-hook (release.toml). Regenerates CHANGELOG.md from
# the Conventional Commit history with the version being cut as the newest
# section, so the release commit carries it. cargo-release runs the hook once
# per package with that package's directory as cwd, so every path is absolute
# under $WORKSPACE_ROOT and each run writes the same bytes. A preview
# (`cargo release` without --execute) sets DRY_RUN=true; the hook then writes
# nothing, so the preview leaves the tree clean.
set -euo pipefail
: "${WORKSPACE_ROOT:?set by cargo-release}" "${NEW_VERSION:?set by cargo-release}"
if [ "${DRY_RUN:-false}" = "true" ]; then
  exit 0
fi
if ! command -v git-cliff >/dev/null 2>&1; then
  echo "git-cliff not found on PATH (cargo install git-cliff)" >&2
  exit 1
fi
out="$WORKSPACE_ROOT/CHANGELOG.md"
git-cliff --repository "$WORKSPACE_ROOT" --config "$WORKSPACE_ROOT/cliff.toml" \
  --tag "v$NEW_VERSION" --output "$out"
# Every section ends in the blank line that separates it from the next, the
# last one included. End the file in a single newline instead, or the
# pre-commit end-of-file-fixer rewrites it under the release commit.
printf '%s\n' "$(<"$out")" > "$out"
