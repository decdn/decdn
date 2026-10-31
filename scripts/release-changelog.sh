#!/usr/bin/env bash
# cargo-release pre-release-hook (release.toml). Regenerates CHANGELOG.md from
# the Conventional Commit history with the version being cut as the newest
# section, so the release commit carries it. cargo-release runs the hook once
# per package with that package's directory as cwd, so every path is absolute
# under $WORKSPACE_ROOT and each run writes the same file. A preview
# (`cargo release` without --execute) sets DRY_RUN=true; the hook then renders
# and checks the changelog but does not write it, so a missing tool, a broken
# cliff.toml or a mis-shaped render fails the preview, not the release.
set -euo pipefail
: "${WORKSPACE_ROOT:?set by cargo-release}" "${NEW_VERSION:?set by cargo-release}" \
  "${PREV_VERSION:?set by cargo-release}"
case "${DRY_RUN:?set by cargo-release}" in
  true | false) ;;
  *)
    echo "DRY_RUN must be 'true' or 'false', got '$DRY_RUN'" >&2
    exit 1
    ;;
esac
if ! command -v git-cliff >/dev/null 2>&1; then
  echo "git-cliff not found on PATH (cargo install git-cliff)" >&2
  exit 1
fi
config="$WORKSPACE_ROOT/cliff.toml"
# git-cliff falls back to its built-in template, with a warning and exit 0,
# when the config path does not resolve.
if [ ! -f "$config" ]; then
  echo "$config not found" >&2
  exit 1
fi
out="$WORKSPACE_ROOT/CHANGELOG.md"
# git-cliff opens --output before it renders, so a render error would leave a
# truncated CHANGELOG.md behind. Render beside it and move the finished file
# over; the trap removes the temporary file when anything fails first.
tmp=$(mktemp "$out.XXXXXX")
trap 'rm -f "$tmp"' EXIT
git-cliff --repository "$WORKSPACE_ROOT" --config "$config" \
  --tag "v$NEW_VERSION" --output "$tmp"
# git-cliff exits 0 on renders a release must not ship: an empty file, no
# section for the new version (a NEW_VERSION older than an existing tag, or
# nothing but skipped commits since the last one), or every release folded
# into one section (a tag_pattern or repository mismatch).
if [ ! -s "$tmp" ]; then
  echo "git-cliff wrote an empty changelog" >&2
  exit 1
fi
if ! grep -qF "## [$NEW_VERSION]" "$tmp"; then
  echo "the changelog has no section for $NEW_VERSION" >&2
  exit 1
fi
if ! grep -qF "## [$PREV_VERSION]" "$tmp"; then
  echo "the changelog has no section for the previous release $PREV_VERSION" >&2
  exit 1
fi
if [ "$DRY_RUN" = true ]; then
  exit 0
fi
# git-cliff leaves blank lines after the oldest section (the separator the
# body template adds, plus its own newline). End the file in a single newline,
# or the pre-commit end-of-file-fixer rewrites it and fails the release commit.
printf '%s\n' "$(<"$tmp")" > "$tmp"
mv -f "$tmp" "$out"
