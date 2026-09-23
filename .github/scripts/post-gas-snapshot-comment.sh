#!/usr/bin/env bash
# Posts (or updates) a sticky PR comment with the forge gas-snapshot diff.
# The comment is found again on later pushes by its marker.
set -euo pipefail

MARKER="<!-- decdn-gas-snapshot-comment -->"
DIFF_FILE="${1:?diff file required}"
# All GH_/GITHUB_ vars guarded so a local dry-run (without `act` etc.)
# fails with a useful message rather than `set -u` "unbound variable".
GITHUB_SERVER_URL="${GITHUB_SERVER_URL:?GITHUB_SERVER_URL required}"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY required}"
GITHUB_RUN_ID="${GITHUB_RUN_ID:?GITHUB_RUN_ID required}"
PR_NUM="${GH_PR_NUMBER:?GH_PR_NUMBER required}"
RUN_URL="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"

# Fence character chosen to survive a triple-backtick inside the diff
# body (e.g. a malicious test name). HTML <pre><code> doesn't render
# inside <details><summary>...</summary> consistently across the GH
# web/mobile renderers, so we use a tilde fence (CommonMark allows
# either ``` or ~~~; mixing them lets one survive the other).
if [[ ! -s "$DIFF_FILE" ]]; then
  read -r -d '' BODY <<EOF || true
${MARKER}
## Gas snapshot

No gas changes vs base.
EOF
else
  # Read the diff via $(<file) — a single bash builtin, no fork — and
  # let `set -e` abort if the file vanishes between the size check and
  # the read. The earlier $(cat "$DIFF_FILE") subshell hid that failure.
  DIFF_BODY="$(<"$DIFF_FILE")"
  read -r -d '' BODY <<EOF || true
${MARKER}
## Gas snapshot

<details><summary>Gas changes vs base</summary>

~~~
${DIFF_BODY}
~~~

</details>

_Run: [${GITHUB_RUN_ID}](${RUN_URL})_
EOF
fi

# Find existing comment by marker + bot author.
EXISTING_ID=$(gh api "repos/${GITHUB_REPOSITORY}/issues/${PR_NUM}/comments" --paginate \
  --jq "[.[] | select(.user.login==\"github-actions[bot]\" and (.body|contains(\"${MARKER}\")))] | .[0].id // empty")

# Capture gh's response body so a 4xx (rate limit, validation error,
# missing scope) surfaces in the run log instead of being swallowed by
# `> /dev/null`. The trap fires on any unexpected exit and emits the
# captured body.
RESP_FILE=$(mktemp)
trap 'rc=$?; if [[ $rc -ne 0 ]]; then echo "::error::gh api failed (exit $rc); response body:"; cat "$RESP_FILE" >&2 || true; fi; rm -f "$RESP_FILE"' EXIT

if [[ -n "$EXISTING_ID" ]]; then
  gh api "repos/${GITHUB_REPOSITORY}/issues/comments/${EXISTING_ID}" -X PATCH -f "body=${BODY}" > "$RESP_FILE"
else
  gh api "repos/${GITHUB_REPOSITORY}/issues/${PR_NUM}/comments" -X POST -f "body=${BODY}" > "$RESP_FILE"
fi
