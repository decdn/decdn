#!/usr/bin/env bash
# Posts/updates a sticky PR comment with contracts line-coverage summary
# (with optional vs-base delta). Mirrors the gas-snapshot sticky pattern.
#
# Usage: contracts-coverage-comment.sh <head.lcov> [<base.lcov>]
#
# LCOV format we care about:
#   LF:<lines-found>    — total instrumented lines in a file
#   LH:<lines-hit>      — covered lines in a file
# We sum LF/LH across all SF blocks to get totals.
set -euo pipefail

MARKER="<!-- decdn-contracts-coverage-comment -->"
HEAD="${1:?head lcov path required}"
BASE="${2:-}"

if [[ ! -s "$HEAD" ]]; then
  echo "head lcov '$HEAD' missing or empty" >&2
  exit 1
fi

# Sum LF / LH across the file (works for any number of SF blocks).
totals() {
  awk -F: '
    /^LF:/ { lf += $2 }
    /^LH:/ { lh += $2 }
    END    { printf "%d %d\n", lf, lh }
  ' "$1"
}

read -r HEAD_LF HEAD_LH < <(totals "$HEAD")
if (( HEAD_LF == 0 )); then
  HEAD_PCT="n/a"
else
  HEAD_PCT=$(awk -v lh="$HEAD_LH" -v lf="$HEAD_LF" \
    'BEGIN { printf "%.2f%%", (lh / lf) * 100 }')
fi

DELTA_LINE=""
if [[ -n "$BASE" && -s "$BASE" ]]; then
  read -r BASE_LF BASE_LH < <(totals "$BASE")
  if (( BASE_LF > 0 && HEAD_LF > 0 )); then
    DELTA_LINE=$(awk -v hlh="$HEAD_LH" -v hlf="$HEAD_LF" \
                     -v blh="$BASE_LH" -v blf="$BASE_LF" \
      'BEGIN {
         h = (hlh / hlf) * 100
         b = (blh / blf) * 100
         d = h - b
         sign = (d >= 0) ? "▲" : "▼"
         printf "%s %+.2fpp vs base (%.2f%% → %.2f%%)", sign, d, b, h
       }')
  fi
elif [[ -n "$BASE" ]]; then
  DELTA_LINE="_No baseline available (first PR on this stack, or baseline artifact expired)._"
fi

RUN_ID="${GITHUB_RUN_ID:-?}"
RUN_URL="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-?/?}/actions/runs/${RUN_ID}"

# printf-based body assembly avoids the $'...' / '...' / "..." quoting maze
# that bit the earlier version (literal \n in single-quoted segments).
BODY=$(printf '\n%s\n## Contracts coverage\n\nLine coverage: **%s** (%d/%d lines)\n' \
         "$MARKER" "$HEAD_PCT" "$HEAD_LH" "$HEAD_LF")
if [[ -n "$DELTA_LINE" ]]; then
  BODY+=$(printf '\n%s\n' "$DELTA_LINE")
fi
BODY+=$(printf '\n_Run: [%s](%s)_\n' "$RUN_ID" "$RUN_URL")

# In CI we post via gh; locally (for testing) just print the body.
if [[ -z "${GH_PR_NUMBER:-}" ]]; then
  printf '%s' "$BODY"
  exit 0
fi

REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY required when posting}"
EXISTING_ID=$(gh api "repos/${REPO}/issues/${GH_PR_NUMBER}/comments" --paginate \
  --jq "[.[] | select(.user.login==\"github-actions[bot]\" and (.body|contains(\"${MARKER}\")))] | .[0].id // empty")

if [[ -n "$EXISTING_ID" ]]; then
  gh api "repos/${REPO}/issues/comments/${EXISTING_ID}" -X PATCH -f "body=${BODY}" > /dev/null
else
  gh api "repos/${REPO}/issues/${GH_PR_NUMBER}/comments" -X POST -f "body=${BODY}" > /dev/null
fi
