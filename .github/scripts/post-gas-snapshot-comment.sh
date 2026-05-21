#!/usr/bin/env bash
# Posts (or updates) a sticky PR comment with the forge gas-snapshot diff.
# Mirrors the coverage-comment sticky pattern in ci.yml; pinned by marker.
set -euo pipefail

MARKER="<!-- decdn-gas-snapshot-comment -->"
DIFF_FILE="${1:?diff file required}"
RUN_URL="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"

if [[ ! -s "$DIFF_FILE" ]]; then
  BODY=$'\n'"${MARKER}"$'\n## Gas snapshot\n\nNo gas changes vs base.'$'\n'
else
  DIFF_BODY=$(cat "$DIFF_FILE")
  BODY=$'\n'"${MARKER}"$'\n## Gas snapshot\n\n<details><summary>Gas changes vs base</summary>\n\n```\n'"${DIFF_BODY}"$'\n```\n\n</details>\n\n_Run: ['"${GITHUB_RUN_ID}"']('"${RUN_URL}"')_\n'
fi

PR_NUM="${GH_PR_NUMBER:?PR number required}"
REPO="${GITHUB_REPOSITORY}"

# Find existing comment by marker + bot author.
EXISTING_ID=$(gh api "repos/${REPO}/issues/${PR_NUM}/comments" --paginate \
  --jq "[.[] | select(.user.login==\"github-actions[bot]\" and (.body|contains(\"${MARKER}\")))] | .[0].id // empty")

if [[ -n "$EXISTING_ID" ]]; then
  gh api "repos/${REPO}/issues/comments/${EXISTING_ID}" -X PATCH -f "body=${BODY}" > /dev/null
else
  gh api "repos/${REPO}/issues/${PR_NUM}/comments" -X POST -f "body=${BODY}" > /dev/null
fi
