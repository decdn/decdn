#!/usr/bin/env bash
# Fails if a container image repository is spelled inconsistently, or if an
# image has no Docker Hub overview file.
#
# There are four images: `<repo>-node` (daemon + CLI), `<repo>` (CLI only),
# and `<owner>/sponsord` and `<owner>/sponsord-onramp` (one server each, named
# for their binaries under the organisation rather than the repository). Each
# name lives in three places that must agree:
#   * release.yml's docker matrix — each `target:` (the Dockerfile stage, which
#     names <stage>-image-digest.txt) with the `image:` it pushes to; and the
#     stage list again in its `sbom` matrix and its `attach` upload loop, which
#     scan and attach each image's files
#   * sign-release.sh's IMAGES list and GHCR_IMAGE table — the list picks which
#     digest files are signed and which images promoted; the table
#     regex-validates each file, then tags from it
#   * security.yml's scan matrix
#
# A mismatch is otherwise caught only when sign-release.sh rejects a digest
# file — after a full release build, with the tag already pushed.
#
# Each stage also needs its Docker Hub overview, docker/README.<stage>.md,
# which sign-release.sh pushes to the Hub repository after the mirror. A
# missing one otherwise shows up only as a warning at release time, with the
# Hub page left stale. This checks only that the file exists; the
# scripts/tests suite checks its contents.
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
# shellcheck disable=SC2016  # same
GH_OWNER='${{ github.repository_owner }}'

fail=0
missing_overview=0
# Whole-line match after stripping indentation: `image: <repo>` is a prefix of
# `image: <repo>-node`, so a substring match would let the CLI line go missing
# unnoticed.
expect() {
  local file="$1" needle="$2"
  [[ -f "$file" ]] || { echo "error: $file not found" >&2; fail=1; return; }
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
# awk reads to the end of the file rather than exiting at the match: an early
# exit while sed still has output buffered kills sed with SIGPIPE once the
# file outgrows the pipe buffer, and pipefail turns that into a failed check.
expect_pair() {
  local file="$1" stage="$2" image="$3" got
  [[ -f "$file" ]] || { echo "error: $file not found" >&2; fail=1; return; }
  got=$(sed 's/^[[:space:]]*//' "$file" | awk -v t="- target: ${stage}" '
    found { next }
    $0 == t { on = 1; next }
    on && /^- / { on = 0; found = 1; next }
    on && /^image: / { print substr($0, 8); found = 1 }
  ')
  [[ "$got" == "$image" ]] || {
    echo "error: $file: target $stage pushes to '${got}', expected: $image" >&2
    fail=1
  }
}

# The list is checked whole: dropping a stage from it would leave that image's
# digest unsigned and its tags unpromoted, and the table lines alone would
# still pass.
expect scripts/sign-release.sh 'IMAGES=( "decdn-node" "decdn" "sponsord" "sponsord-onramp" )'

# Whole, for the same reason: a stage missing from `sbom` or `attach` is
# pushed but never reaches the draft, and sign-release.sh refuses the release
# only after the tag and images are public.
expect .github/workflows/release.yml 'target: [decdn-node, decdn, sponsord, sponsord-onramp]'
expect .github/workflows/release.yml 'for t in decdn-node decdn sponsord sponsord-onramp; do'

# Stage name → the image as the workflows spell it → as sign-release.sh spells
# it. The decdn images hang off the repository (`${REPO}` in the script), the
# sponsord images off its owner (`${OWNER}`).
# shellcheck disable=SC2016  # ${REPO} and ${OWNER} are literal text in the target file
for triple in \
  "decdn-node|${GH_REPO}-node|"'ghcr.io/${REPO}-node' \
  "decdn|${GH_REPO}|"'ghcr.io/${REPO}' \
  "sponsord|${GH_OWNER}/sponsord|"'ghcr.io/${OWNER}/sponsord' \
  "sponsord-onramp|${GH_OWNER}/sponsord-onramp|"'ghcr.io/${OWNER}/sponsord-onramp'
do
  IFS='|' read -r stage image script_image <<<"$triple"
  expect_pair .github/workflows/release.yml "$stage" "$image"
  expect scripts/sign-release.sh "[${stage}]=\"${script_image}\""
  expect .github/workflows/security.yml  "- ghcr.io/${image}"
  [[ -f "docker/README.${stage}.md" ]] || {
    echo "error: stage $stage has no Docker Hub overview: docker/README.${stage}.md" >&2
    missing_overview=1
  }
done

if (( fail )); then
  cat >&2 <<EOF

Each image repository must be spelled the same in all three places, or
sign-release.sh will reject the digest file that release.yml wrote —
after a full release build, with the tag already pushed.
EOF
fi
if (( missing_overview )); then
  cat >&2 <<EOF

Each image needs docker/README.<stage>.md (see RELEASING.md § Signing and
publishing), or its Docker Hub page is left stale at release time.
EOF
fi
if (( fail || missing_overview )); then
  exit 1
fi

echo "image names consistent across release.yml, security.yml and sign-release.sh; every image has an overview file"
