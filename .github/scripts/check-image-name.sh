#!/usr/bin/env bash
# Fails if a container image repository is spelled inconsistently.
#
# There are two images: `<repo>-node` (daemon + CLI) and `<repo>` (CLI only).
# Each name lives in three places that must agree:
#   * release.yml's docker matrix — each `target:` (the Dockerfile stage, which
#     names <stage>-image-digest.txt) with the `image:` it pushes to
#   * sign-release.sh's IMAGES list and GHCR_IMAGE table — the list picks which
#     digest files are signed and which images promoted; the table
#     regex-validates each file, then tags from it
#   * security.yml's scan matrix
#
# A mismatch is otherwise caught only when sign-release.sh rejects a digest
# file — after a full release build, with the tag already pushed.
#
# This lives in a script rather than inline in the workflow ON PURPOSE. GitHub
# Actions expands `${{ … }}` inside a `run:` block before bash ever sees it, so
# an inline grep for the literal `${{ github.repository }}` searches for the
# expanded value instead and never matches. In a file the runner does not
# template, single quotes mean what they say.
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

# Single-quoted, and never passed through a workflow expression.
# shellcheck disable=SC2016  # not expanding it is the entire point: this is the
# literal text that must appear in the workflow file.
GH_REPO='${{ github.repository }}'

fail=0
# Whole-line match after stripping indentation: `image: <repo>` is a prefix of
# `image: <repo>-node`, so a substring match would let the CLI line go missing
# unnoticed.
expect() {
  local file="$1" needle="$2"
  # -F: the needles contain ${{ }} and ${}, none of it meant as a regex. No -q:
  # grep would exit at the first match, and sed's SIGPIPE fails the pipeline
  # under pipefail.
  sed 's/^[[:space:]]*//' "$file" | grep -xF -- "$needle" >/dev/null || {
    echo "error: $file has no line: $needle" >&2
    fail=1
  }
}

# The `image:` that follows `- target: <stage>` in release.yml's matrix, before
# the next entry starts. A plain line check would pass with the two `image:`
# lines swapped between entries, which pushes each image to the other's name.
expect_pair() {
  local file="$1" stage="$2" image="$3" got
  got=$(sed 's/^[[:space:]]*//' "$file" | awk -v t="- target: ${stage}" '
    $0 == t { on = 1; next }
    on && /^- / { exit }
    on && /^image: / { print substr($0, 8); exit }
  ')
  [[ "$got" == "$image" ]] || {
    echo "error: $file: target $stage pushes to '${got}', expected: $image" >&2
    fail=1
  }
}

# The list is checked whole: dropping a stage from it would leave that image's
# digest unsigned and its tags unpromoted, and the table lines alone would
# still pass.
expect .github/scripts/sign-release.sh 'IMAGES=( "decdn-node" "decdn" )'

# Stage name → repository suffix.
for pair in "decdn-node:-node" "decdn:"; do
  stage="${pair%%:*}" suffix="${pair#*:}"
  expect_pair .github/workflows/release.yml "$stage" "${GH_REPO}${suffix}"
  # shellcheck disable=SC2016  # ${REPO} is literal text in the target file
  expect .github/scripts/sign-release.sh "[${stage}]="'"ghcr.io/${REPO}'"${suffix}"'"'
  expect .github/workflows/security.yml  "- ghcr.io/${GH_REPO}${suffix}"
done

if (( fail )); then
  cat >&2 <<EOF

Each image repository must be spelled the same in all three places, or
sign-release.sh will reject the digest file that release.yml wrote —
after a full release build, with the tag already pushed.
EOF
  exit 1
fi

echo "image names consistent across release.yml, security.yml and sign-release.sh"
