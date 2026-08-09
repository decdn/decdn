#!/usr/bin/env bash
# Fails if the container image repository is spelled inconsistently.
#
# The name lives in three places that must agree:
#   * release.yml's IMAGE_NAME  — builds the image and writes image-digest.txt
#   * sign-release.sh's IMAGE   — regex-validates that file, then tags from it
#   * security.yml's scan target
#
# A mismatch is otherwise caught only when sign-release.sh rejects
# image-digest.txt — after a full release build, with the tag already pushed.
#
# This lives in a script rather than inline in the workflow ON PURPOSE. GitHub
# Actions expands `${{ … }}` inside a `run:` block before bash ever sees it, so
# an inline grep for the literal `${{ github.repository }}` searches for the
# expanded value instead and never matches. In a file the runner does not
# template, single quotes mean what they say.
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

# The image is the daemon only, so it is not named after the repository.
SUFFIX="-node"
# Single-quoted, and never passed through a workflow expression.
# shellcheck disable=SC2016  # not expanding it is the entire point: this is the
# literal text that must appear in the workflow file.
GH_REPO='${{ github.repository }}'

fail=0
expect() {
  local file="$1" needle="$2"
  # -F: the needles contain ${{ }} and ${}, none of it meant as a regex.
  grep -qF -- "$needle" "$file" || {
    echo "error: $file does not contain: $needle" >&2
    fail=1
  }
}

expect .github/workflows/release.yml   "IMAGE_NAME: ${GH_REPO}${SUFFIX}"
# shellcheck disable=SC2016  # ${REPO} is literal text in the target file
expect .github/scripts/sign-release.sh 'IMAGE="ghcr.io/${REPO}'"${SUFFIX}"'"'
expect .github/workflows/security.yml  "ghcr.io/${GH_REPO}${SUFFIX}:latest"

if (( fail )); then
  cat >&2 <<EOF

The image repository must be spelled the same in all three places, or
sign-release.sh will reject the image-digest.txt that release.yml wrote —
after a full release build, with the tag already pushed.
EOF
  exit 1
fi

echo "image name consistent across release.yml, security.yml and sign-release.sh"
