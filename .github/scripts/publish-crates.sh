#!/usr/bin/env bash
# Publishes the workspace to crates.io, after sign-release.sh has published the
# GitHub Release.
#
# Like signing, this is a human step with no counterpart in Actions: there is no
# crates.io token in Actions secrets, and a `push`-triggered workflow runs the
# workflow definition from the pushed ref, so a token reachable from release.yml
# would be reachable from any tag anyone with push access could craft.
#
# Usage:  .github/scripts/publish-crates.sh v0.1.2
#
# Environment:
#   DECDN_REPO             override the owner/repo (default: decdn/decdn)
#   DECDN_SIGNING_KEY      key the tag is expected to be signed by (default: any
#                          key published in KEYS)
#   CARGO_REGISTRY_TOKEN   crates.io token, if not in the cargo credentials file
#
# Unlike sign-release.sh this is NOT freely re-runnable: a crates.io version is
# immutable and can never be replaced or re-uploaded (only yanked, which does not
# free the version). If it fails partway, the crates already uploaded stay
# uploaded — see the recovery note printed on failure.
set -euo pipefail

TAG="${1:?tag required, e.g. v0.1.2}"
VERSION="${TAG#v}"
REPO="${DECDN_REPO:-decdn/decdn}"
SIGNING_KEY="${DECDN_SIGNING_KEY:-}"

die() { echo "error: $*" >&2; exit 1; }

# ---- preconditions -------------------------------------------------------

for tool in cargo gh gpg git curl; do
  command -v "$tool" >/dev/null || die "$tool not found on PATH"
done

gh auth status >/dev/null 2>&1 || die "gh is not authenticated; run \`gh auth login\`"

REPO_ROOT=$(git rev-parse --show-toplevel) || die "not inside a git repository"
KEYS_FILE="$REPO_ROOT/KEYS"
[[ -f "$KEYS_FILE" ]] || die "KEYS not found at $KEYS_FILE"

# Checked up front rather than at the first upload: without it, cargo would
# publish nothing and fail on the very first crate, but only after the tag and
# release checks have already passed, which reads like a deeper problem.
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]] &&
   ! grep -qs 'token' "$CARGO_HOME_DIR/credentials.toml" "$CARGO_HOME_DIR/credentials"; then
  die "no crates.io token found.
Run \`cargo login\`, or set CARGO_REGISTRY_TOKEN."
fi

# A gpg home containing ONLY the published maintainer keys — the same
# construction sign-release.sh uses, and for the same reason: verifying against
# your own keyring only proves you can read a signature you already trust.
KEYS_HOME=$(mktemp -d)
chmod 700 "$KEYS_HOME"
# Single EXIT trap for the whole script. WORKTREE does not exist yet, hence the
# :- guard; it is a git worktree, so it must be removed through git rather than
# with rm, or the parent repo keeps a dangling administrative entry.
cleanup() {
  local rc=$?
  rm -rf "$KEYS_HOME"
  if [[ -n "${WORKTREE:-}" ]]; then
    git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" >/dev/null 2>&1 ||
      echo "warning: could not remove the worktree at $WORKTREE" >&2
  fi
  return $rc
}
trap cleanup EXIT

gpg --homedir "$KEYS_HOME" --import "$KEYS_FILE" >/dev/null 2>&1 ||
  die "KEYS is not a valid OpenPGP keyring"

# ---- the tag must be the one that was released ---------------------------

# `git fetch --tags` does NOT update a tag that already exists locally, so
# without --force a stale local tag verifies happily while crates.io would
# receive code from a different commit than the release shipped.
git fetch --tags --force origin >/dev/null 2>&1 ||
  die "cannot reach origin to confirm the tag"
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null ||
  die "tag $TAG does not exist"

echo "==> Verifying $TAG"
GNUPGHOME="$KEYS_HOME" git verify-tag "$TAG" ||
  die "$TAG is not signed by a key published in KEYS"

if [[ -n "$SIGNING_KEY" ]]; then
  GNUPGHOME="$KEYS_HOME" git verify-tag --raw "$TAG" 2>&1 |
    grep -q "VALIDSIG.*${SIGNING_KEY}" ||
    die "$TAG is signed, but not by $SIGNING_KEY"
fi

LOCAL_TAG=$(git rev-parse "refs/tags/${TAG}^{commit}")
REMOTE_TAG=$(gh api "repos/${REPO}/git/ref/tags/${TAG}" --jq .object.sha) ||
  die "tag $TAG not found on $REPO"
REMOTE_COMMIT=$(git rev-parse "${REMOTE_TAG}^{commit}")
[[ "$LOCAL_TAG" == "$REMOTE_COMMIT" ]] || die \
  "local tag $TAG ($LOCAL_TAG) differs from origin ($REMOTE_COMMIT).
crates.io would receive code that was never released. Reconcile the tags first."

# The signed release is the gate. Publishing first would put code on crates.io —
# permanently, since a version cannot be replaced — that no signature vouches
# for and that could still be pulled from the release if signing then failed.
IS_DRAFT=$(gh release view "$TAG" --repo "$REPO" --json isDraft --jq .isDraft) || die \
  "could not read release $TAG from $REPO.
Check: gh release view $TAG --repo $REPO"
case "$IS_DRAFT" in
  false) ;;
  true) die "release $TAG is still a draft.
Run .github/scripts/sign-release.sh $TAG first — crates.io publishes are
irreversible, so nothing goes out before the signed release does." ;;
  *) die "unexpected isDraft value from gh: '$IS_DRAFT' (expected true or false)" ;;
esac

# ---- publish from the tagged tree ----------------------------------------

# A detached worktree at the tag, not the current checkout: the working copy may
# carry uncommitted edits or sit on a different branch entirely, and cargo would
# happily upload whatever is on disk.
WORKTREE=$(mktemp -d)
rmdir "$WORKTREE"   # git worktree add wants to create the directory itself
git -C "$REPO_ROOT" worktree add --detach "$WORKTREE" "$TAG" >/dev/null 2>&1 ||
  die "could not create a worktree at $TAG"
cd "$WORKTREE"

# Belt and braces over release.yml's own check: this asserts the tree being
# uploaded carries the tag's version, on the machine doing the uploading.
mapfile -t CRATES < <(
  cargo metadata --no-deps --format-version 1 |
    python3 -c '
import json, sys
for p in sorted(json.load(sys.stdin)["packages"], key=lambda p: p["name"]):
    if p.get("publish") == []:          # publish = false
        continue
    print(p["name"], p["version"])
'
)
(( ${#CRATES[@]} > 0 )) || die "no publishable crates found"

for entry in "${CRATES[@]}"; do
  read -r name ver <<<"$entry"
  [[ "$ver" == "$VERSION" ]] ||
    die "$name is at $ver, but the tag says $VERSION"
done
echo "==> ${#CRATES[@]} crates at $VERSION"

echo "==> Dry run"
cargo publish --workspace --locked --dry-run ||
  die "the dry run failed; nothing has been uploaded"

cat <<EOF

About to publish ${#CRATES[@]} crates to crates.io as version $VERSION:

$(printf '  %s\n' "${CRATES[@]%% *}")

This CANNOT be undone. A published version is immutable; yanking hides it from
new resolutions but never frees the version or removes the code.
EOF
read -r -p "Type the version ($VERSION) to continue: " confirm < /dev/tty
[[ "$confirm" == "$VERSION" ]] || die "aborted"

echo "==> Publishing"
# --workspace resolves the dependency order itself and waits for each crate to
# appear in the index before publishing its dependents.
cargo publish --workspace --locked || die \
  "publishing failed partway. Crates uploaded before the failure ARE published
and cannot be re-uploaded. Do NOT re-run this script — it would fail on the
first already-published crate. Check which succeeded:

  $(printf '%s\n' "${CRATES[@]%% *}" | sed 's|^|  https://crates.io/crates/|' | head -3)
  ...

then publish only the remainder, in dependency order:

  cargo publish -p <crate> --locked"

# ---- confirm ---------------------------------------------------------------

# cargo returning 0 means the uploads were accepted, not that the index has
# caught up. Ask crates.io what it actually serves.
echo "==> Confirming on crates.io"
missing=()
for entry in "${CRATES[@]}"; do
  read -r name ver <<<"$entry"
  found=""
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if curl -sf -A "decdn-publish-crates" \
        "https://crates.io/api/v1/crates/${name}/${ver}" >/dev/null 2>&1; then
      found=1
      break
    fi
    sleep 3
  done
  if [[ -n "$found" ]]; then
    echo "    https://crates.io/crates/${name}/${ver}"
  else
    missing+=("${name} ${ver}")
  fi
done

(( ${#missing[@]} == 0 )) || die \
  "cargo reported success but crates.io does not serve these yet:
$(printf '  %s\n' "${missing[@]}")
This is usually index lag — re-check in a minute before assuming a failure."

echo
echo "Published ${#CRATES[@]} crates at $VERSION."
