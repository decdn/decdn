#!/usr/bin/env bash
# Opens the PR that moves the decdn pin to a decdn release
# (RELEASING.md § Pinning decdn).
#
# Usage:  .github/scripts/open-decdn-bump.sh [--dry-run] [vX.Y.Z]
#
# With no tag it takes decdn's latest published release. decdn's
# publish-crates.sh runs it with the tag it just published; a maintainer runs
# it by hand from a clone whose `origin` is decdn/sponsord, with push access.
#
# It works in a detached worktree at origin/main, so the working copy and its
# branch stay as they are. It commits as your git identity, pushes with your
# `origin` credentials and opens the PR with your `gh` login — a PR opened
# that way runs CI, which one opened by Actions' GITHUB_TOKEN would not.
#
# Nothing happens when the pin is already at that release or a later one
# (SemVer precedence), or when a PR from bump/decdn-<tag> exists in any state:
# a closed bump PR is never reopened, so close one to skip that release.
#
# --dry-run  make the change in the worktree, print its diff, and stop before
#            the commit
#
# Exit: 0 done or nothing to do; 1 error (no PR opened unless the output says
# so), or the PR is open but the new lock fails `cargo metadata --locked` /
# check-decdn-pin.sh; 2 usage; 3 crates.io does not serve the release yet (run
# it again later).
set -euo pipefail

DECDN=decdn/decdn
# The helpers come from beside this script, so they match it; they act on the
# repository of the current directory, which is the worktree below.
SCRIPTS=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SPONSORD=decdn/sponsord
READY_ATTEMPTS=10
READY_INTERVAL=60

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '5p' "${BASH_SOURCE[0]}" | sed 's/^# //' >&2; exit 2; }

DRY_RUN="" TAG=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage ;;
    v*) [[ -z "$TAG" ]] || usage; TAG="$arg" ;;
    *) usage ;;
  esac
done

# ---- preconditions -------------------------------------------------------

for tool in git cargo gh python3; do
  command -v "$tool" >/dev/null || die "$tool not found on PATH"
done
# Only the account gh will use: a stale login elsewhere must not block this.
gh auth status --hostname github.com --active >/dev/null ||
  die "gh is not logged in to github.com; run \`gh auth login\`"

REPO_ROOT=$(git rev-parse --show-toplevel) || die "not inside a git repository"
origin=$(git -C "$REPO_ROOT" remote get-url origin) || die "no \`origin\` remote"
# The PR goes to $SPONSORD, so the branch must too: a fork's origin would push
# somewhere `gh pr create` cannot see.
[[ "$origin" =~ [/:]${SPONSORD}(\.git)?/?$ ]] ||
  die "origin is $origin, not $SPONSORD; run this from a $SPONSORD checkout"

if [[ -n "$TAG" ]]; then
  [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
    die "$TAG is not a decdn release tag (vX.Y.Z)"
  state=$(gh release view "$TAG" -R "$DECDN" --json isDraft,isPrerelease \
    --jq 'if .isDraft then "draft" elif .isPrerelease then "prerelease" else "published" end') ||
    die "could not read $DECDN release $TAG"
  [[ "$state" == published ]] || die "$DECDN $TAG is a $state, not a published release"
fi

# ---- worktree at origin/main ---------------------------------------------

git -C "$REPO_ROOT" fetch --quiet origin main || die "could not fetch origin main"

WORKTREE=$(mktemp -d)
BODY=""
cleanup() {
  # Removed through git, so the parent repository keeps no stale entry.
  git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" >/dev/null 2>&1 ||
    rm -rf "$WORKTREE"
  git -C "$REPO_ROOT" worktree prune >/dev/null 2>&1 || true
  [[ -z "$BODY" ]] || rm -f "$BODY"
}
trap cleanup EXIT
rmdir "$WORKTREE"   # git worktree add wants to create the directory itself
git -C "$REPO_ROOT" worktree add --quiet --detach "$WORKTREE" origin/main ||
  die "could not create a worktree at origin/main"
cd "$WORKTREE"
bump() { "$SCRIPTS/bump-decdn.sh" "$@"; }

# ---- which release -------------------------------------------------------

CURRENT=$(bump pinned) || die "could not read the decdn pin on origin/main"
if [[ -z "$TAG" ]]; then
  TAG=$(bump latest) || die "could not read decdn's latest release"
fi
# Never move the pin backwards: not to a patch release on an older line, nor
# to an older tag given by hand. A pin off a tag (untagged) always moves.
relation=$(bump compare "$TAG") || die "could not compare the pin with $TAG"
case "$relation" in
  same)
    echo "sponsord main already pins decdn $TAG; nothing to do"
    exit 0 ;;
  ahead)
    echo "sponsord main pins decdn $CURRENT, later than $TAG; nothing to do"
    exit 0 ;;
  behind|untagged) ;;
  *) die "unexpected comparison of the pin with $TAG: $relation" ;;
esac

BRANCH="bump/decdn-${TAG}"
existing=$(gh pr list -R "$SPONSORD" --head "$BRANCH" --state all \
  --json url,state --jq '.[0] | select(.) | "\(.url) (\(.state | ascii_downcase))"') ||
  die "could not list $SPONSORD PRs"
if [[ -n "$existing" ]]; then
  echo "a PR for the bump to $TAG exists: $existing; nothing to do"
  exit 0
fi

# publish-crates.sh runs this right after its uploads, and the crates.io index
# can trail them by minutes.
attempt=1
until bump ready "$TAG"; do
  rc=$?
  (( rc == 3 )) || die "could not check crates.io for decdn $TAG; no PR opened"
  if (( attempt >= READY_ATTEMPTS )); then
    echo "crates.io does not serve decdn $TAG yet; run this again later" >&2
    exit 3
  fi
  echo "    waiting ${READY_INTERVAL}s for crates.io (attempt $attempt/$READY_ATTEMPTS)" >&2
  sleep "$READY_INTERVAL"
  attempt=$((attempt + 1))
done

# ---- bump ----------------------------------------------------------------

echo "==> Pinning decdn $TAG (from ${CURRENT:-a non-tag ref})"
OLD_SHA=$(bump locked-sha) || die "could not read the locked decdn commit"
bump rewrite "$TAG" || die "could not point the decdn aliases at $TAG; no PR opened"
cargo update --quiet -p decdn-client -p decdn-common -p decdn-incentive -p decdn-e2e ||
  die "cargo update to decdn $TAG failed; no PR opened"
bump changelog "$TAG" || die "could not add the changelog entries; no PR opened"
NEW_SHA=$(bump locked-sha) || die "could not read the new locked decdn commit"

# A lock that resolved but fails these checks still gets a PR, for a human to
# finish; the exit code says so. One that does not resolve stopped above.
CHECK=ok
if ! cargo metadata --locked --format-version 1 >/dev/null || ! "$SCRIPTS/check-decdn-pin.sh"; then
  CHECK=failed
  echo "warning: the new lock does not check out; the PR needs work" >&2
fi

if [[ -n "$DRY_RUN" ]]; then
  git --no-pager diff
  echo
  echo "dry run: would push $BRANCH and open \"build: pin decdn to $TAG\""
  [[ "$CHECK" == ok ]]
  exit
fi
git --no-pager diff --stat

# ---- commit, push, PR ----------------------------------------------------

git add Cargo.toml Cargo.lock crates/*/CHANGELOG.md || die "git add failed; no PR opened"
# --no-verify: CI is the gate on this PR, and the hooks would compile the whole
# workspace in the middle of decdn's publish-crates.sh.
git commit --quiet --no-verify -F - <<EOF || die "git commit failed; no PR opened"
build: pin decdn to ${TAG}

Lock the decdn crates at decdn's ${TAG} release tag (${NEW_SHA})
instead of ${CURRENT:-the previous pin} (${OLD_SHA}), and require ${TAG#v},
which is on crates.io.
EOF
# No local branch: the commit is pushed from the detached worktree. Forced,
# because the branch may be left over from a run that pushed it and opened no
# PR; a run stops above when a PR exists.
git push --quiet --force origin "HEAD:refs/heads/${BRANCH}" ||
  die "could not push $BRANCH; no PR opened"

others=$(gh pr list -R "$SPONSORD" --state open --json number,headRefName \
  --jq "[.[] | select(.headRefName | startswith(\"bump/decdn-\")) | select(.headRefName != \"${BRANCH}\") | \"#\\(.number)\"] | join(\", \")") ||
  others=""   # only a note in the body; not worth stranding the pushed branch
BODY=$(mktemp)
{
  echo "Moves the decdn pin to [decdn ${TAG}](https://github.com/${DECDN}/releases/tag/${TAG})."
  echo
  echo "- From: \`${CURRENT:-(not a tag)}\` (\`${OLD_SHA}\`)"
  echo "- To: \`${TAG}\` (\`${NEW_SHA}\`), on crates.io"
  echo "- decdn changes: https://github.com/${DECDN}/compare/${OLD_SHA}...${NEW_SHA}"
  echo
  if [[ "$CHECK" != ok ]]; then
    echo "**The lock does not check out:** \`cargo metadata --locked\` or"
    echo "\`check-decdn-pin.sh\` failed when this PR was opened."
    echo
  fi
  if [[ -n "$others" ]]; then
    echo "Older bump PRs still open: ${others}. This one supersedes them."
    echo
  fi
  echo "Opened by \`.github/scripts/open-decdn-bump.sh\` (RELEASING.md § Pinning decdn)."
  echo "A red CI means the new decdn needs code changes here: push them to"
  echo "this branch."
} > "$BODY"
url=$(gh pr create -R "$SPONSORD" --base main --head "$BRANCH" \
  --title "build: pin decdn to ${TAG}" --body-file "$BODY") ||
  die "pushed $BRANCH but could not open the PR; run this again to retry"
echo "==> Opened $url"

[[ "$CHECK" == ok ]]
