#!/usr/bin/env bash
# Fails if the container images are named inconsistently.
#
# The image list lives in release_plan.py (which crate's release carries which
# image). These must agree with it:
#   * Dockerfile                — one final target per image (`FROM base AS <image>`)
#   * release.yml's docker job  — pushes `ghcr.io/<owner>/<image>` and writes
#                                 <image>-image-digest.txt
#   * sign-release.sh           — validates that file against the same name,
#                                 then tags from it
#   * security.yml's scan matrix
#
# A mismatch is otherwise caught only when sign-release.sh rejects a digest
# file — after a full release build, with the tag already pushed.
#
# This lives in a script rather than inline in the workflow ON PURPOSE. GitHub
# Actions expands `${{ … }}` inside a `run:` block before bash ever sees it, so
# an inline grep for the literal `${{ github.repository_owner }}` searches for
# the expanded value instead and never matches. In a file the runner does not
# template, single quotes mean what they say.
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

mapfile -t IMAGES < <(python3 .github/scripts/release_plan.py --images)
(( ${#IMAGES[@]} > 0 )) || {
  echo "error: release_plan.py lists no images — this check inspected nothing" >&2
  exit 1
}

# shellcheck disable=SC2016  # literal workflow-expression text, never expanded
RELEASE_IMAGE='IMAGE: ghcr.io/${{ github.repository_owner }}/${{ needs.release.outputs.image }}'
# shellcheck disable=SC2016  # literal workflow-expression text, never expanded
SCAN_IMAGE='IMAGE: ghcr.io/${{ github.repository_owner }}/${{ matrix.image }}'
joined=$(printf '%s, ' "${IMAGES[@]}")
MATRIX="image: [${joined%, }]"

fail=0
expect() {
  local file="$1" needle="$2"
  # -F: the needles contain ${{ }} and brackets, none of it meant as a regex.
  grep -qF -- "$needle" "$file" || {
    echo "error: $file does not contain: $needle" >&2
    fail=1
  }
}

expect .github/workflows/release.yml   "$RELEASE_IMAGE"
expect .github/workflows/security.yml  "$SCAN_IMAGE"
expect .github/workflows/security.yml  "$MATRIX"
# shellcheck disable=SC2016  # ${OWNER} is literal text in the target file
expect .github/scripts/sign-release.sh 'GHCR_NAMESPACE="ghcr.io/${OWNER}"'
for image in "${IMAGES[@]}"; do
  grep -qE "^FROM [^ ]+ AS ${image}\$" Dockerfile || {
    echo "error: Dockerfile has no final target \`AS ${image}\`" >&2
    fail=1
  }
done

if (( fail )); then
  cat >&2 <<EOF

The images (${IMAGES[*]}) must be named the same in release_plan.py, the
Dockerfile, release.yml, security.yml and sign-release.sh, or sign-release.sh
will reject the digest file release.yml wrote — after a full release build,
with the tag already pushed.
EOF
  exit 1
fi

echo "image names consistent across release_plan.py, Dockerfile, release.yml, security.yml and sign-release.sh"
