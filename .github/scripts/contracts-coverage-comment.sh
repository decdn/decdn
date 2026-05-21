#!/usr/bin/env bash
# Posts/updates a sticky PR comment with contracts line-coverage summary
# (with optional vs-base delta). Mirrors the gas-snapshot sticky pattern.
#
# Usage: contracts-coverage-comment.sh <head.lcov> [<base.lcov>] [<absent-reason>]
#
#   head.lcov         required; current-revision coverage report
#   base.lcov         optional; main-baseline coverage report
#   absent-reason     optional; path to a marker file written by the
#                     ci.yml "Tag baseline status" step. Contains
#                     "not-found" (no artifact) or "download-failed"
#                     (artifact API outage) so the comment can
#                     distinguish the two cases.
#
# LCOV format we care about (one of each per SF block):
#   LF:<lines-found>    — total instrumented lines in a file
#   LH:<lines-hit>      — covered lines in a file
# Regexes anchor `^LF:N$` / `^LH:N$` so a colon-bearing token inside
# another LCOV field (e.g. a test function name) can't be mis-parsed.
set -euo pipefail

MARKER="<!-- decdn-contracts-coverage-comment -->"
HEAD="${1:?head lcov path required}"
BASE="${2:-}"
ABSENT_REASON_FILE="${3:-}"

if [[ ! -s "$HEAD" ]]; then
  echo "::error::head lcov '$HEAD' missing or empty" >&2
  exit 1
fi

# Sum LF / LH across the file (works for any number of SF blocks). On
# malformed input awk produces zeros; we surface that as a job error so
# the comment never reports a spurious 0% as a "real" measurement.
totals() {
  awk '
    /^LF:[0-9]+$/ { sub("^LF:", "", $0); lf += $0 }
    /^LH:[0-9]+$/ { sub("^LH:", "", $0); lh += $0 }
    END           { printf "%d %d\n", lf, lh }
  ' "$1"
}

# Helper: parse "<lf> <lh>" into named globals, fail loudly on missing.
parse_totals() {
  local lcov="$1" prefix="$2"
  local out
  if ! out=$(totals "$lcov"); then
    echo "::error::failed to read $lcov" >&2
    return 1
  fi
  read -r "${prefix}_LF" "${prefix}_LH" <<<"$out"
  # awk-empty-output guard: read succeeds with empty values, so detect
  # that and refuse to render a coverage report from nothing.
  local lf_var="${prefix}_LF" lh_var="${prefix}_LH"
  if [[ -z "${!lf_var:-}" || -z "${!lh_var:-}" ]]; then
    echo "::error::lcov $lcov produced no LF/LH totals — possibly malformed" >&2
    return 1
  fi
}

parse_totals "$HEAD" HEAD
if (( HEAD_LF == 0 )); then
  HEAD_PCT="n/a"
else
  HEAD_PCT=$(awk -v lh="$HEAD_LH" -v lf="$HEAD_LF" \
    'BEGIN { printf "%.2f%%", (lh / lf) * 100 }')
fi

DELTA_LINE=""
if [[ -n "$BASE" && -s "$BASE" ]]; then
  parse_totals "$BASE" BASE
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
elif [[ -n "$ABSENT_REASON_FILE" && -s "$ABSENT_REASON_FILE" ]]; then
  # No baseline, but the workflow recorded a reason — disambiguate
  # "first PR / artifact expired" (not-found) from "artifact API
  # failed" (download-failed). Otherwise a persistent outage would be
  # indistinguishable from a legitimate first PR.
  reason=$(<"$ABSENT_REASON_FILE")
  case "$reason" in
    not-found)
      DELTA_LINE="_No baseline available (first PR on this stack, or baseline artifact expired)._"
      ;;
    download-failed)
      DELTA_LINE=":warning: _Baseline artifact present but download failed — delta unavailable. Check the workflow log._"
      ;;
    *)
      DELTA_LINE="_No baseline available (status: ${reason})._"
      ;;
  esac
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

# Capture gh's response body so a 4xx (rate limit, validation error,
# missing scope) surfaces in the run log instead of being swallowed by
# `> /dev/null`. Trap fires on any non-zero exit.
RESP_FILE=$(mktemp)
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "::error::gh api failed (exit $rc); response body:"; cat "$RESP_FILE" >&2 || true; fi; rm -f "$RESP_FILE"' EXIT

if [[ -n "$EXISTING_ID" ]]; then
  gh api "repos/${REPO}/issues/comments/${EXISTING_ID}" -X PATCH -f "body=${BODY}" > "$RESP_FILE"
else
  gh api "repos/${REPO}/issues/${GH_PR_NUMBER}/comments" -X POST -f "body=${BODY}" > "$RESP_FILE"
fi
