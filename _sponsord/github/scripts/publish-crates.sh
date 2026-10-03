#!/usr/bin/env bash
# Publishes one crate to crates.io, after sign-release.sh has published its
# GitHub Release. Each crate is versioned and released on its own, so the tag
# (`<crate>-v<version>`) names the one crate this run uploads.
#
# Like signing, this is a human step with no counterpart in Actions: there is no
# crates.io token in Actions secrets, and a `push`-triggered workflow runs the
# workflow definition from the pushed ref, so a token reachable from release.yml
# would be reachable from any tag anyone with push access could craft.
#
# Usage:  .github/scripts/publish-crates.sh sponsord-core-v0.1.2
#
# The uploaded manifest resolves every path and git dependency by its
# `version`, so crates.io must already serve each of them (registry_deps.py
# lists them): a sibling crate's own release (publish sponsord-api and
# sponsord-core before sponsord), and the decdn release whose tag is the
# commit Cargo.lock pins. The readiness check below refuses until then.
#
# Environment:
#   SPONSORD_REPO             override the owner/repo (default: decdn/sponsord)
#   SPONSORD_SIGNING_KEY      key the tag is expected to be signed by (default:
#                             any key published in KEYS)
#   CARGO_REGISTRY_TOKEN      crates.io token, if not in the cargo credentials file
#
# Unlike sign-release.sh this is NOT re-runnable once the upload succeeds: a
# crates.io version is immutable and can never be replaced or re-uploaded (only
# yanked, which does not free the version).
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

TAG="${1:?tag required, e.g. sponsord-core-v0.1.2}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
command -v python3 >/dev/null || die "python3 not found on PATH"
PLAN=$(python3 "$SCRIPT_DIR/release_plan.py" "$TAG") ||
  die "$TAG is not a release tag (<crate>-vMAJOR.MINOR.PATCH[-pre])"
CRATE=$(sed -n 's/^crate=//p' <<<"$PLAN")
VERSION=$(sed -n 's/^version=//p' <<<"$PLAN")
ARCHIVES=$(sed -n 's/^archives=//p' <<<"$PLAN")
REPO="${SPONSORD_REPO:-decdn/sponsord}"
SIGNING_KEY="${SPONSORD_SIGNING_KEY:-}"

# ---- preconditions -------------------------------------------------------

# python3 is in here because it reads the crate list out of `cargo metadata`
# below. Bare coreutils (sed, head, cat) are not: they are assumed the way bash
# itself is.
for tool in cargo gh gpg git curl python3; do
  command -v "$tool" >/dev/null || die "$tool not found on PATH"
done

gh auth status >/dev/null 2>&1 || die "gh is not authenticated; run \`gh auth login\`"

REPO_ROOT=$(git rev-parse --show-toplevel) || die "not inside a git repository"
KEYS_FILE="$REPO_ROOT/KEYS"
[[ -f "$KEYS_FILE" ]] || die "KEYS not found at $KEYS_FILE"

# Checked up front rather than at the first upload: without it, cargo would
# publish nothing and fail on the very first crate, but only after the tag and
# release checks have already passed, which reads like a deeper problem.
#
# Scoped to the crates.io entry specifically. A bare `token` grep also matches a
# `[registries.internal]` token, or the comment `# token goes here`, and passing
# on either reproduces exactly the late failure this exists to prevent. cargo
# writes the crates.io credential under `[registry]`, so anchor on that section.
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"
has_crates_io_token() {
  local f
  for f in "$CARGO_HOME_DIR/credentials.toml" "$CARGO_HOME_DIR/credentials"; do
    [[ -f "$f" ]] || continue
    # The `[registry]` section, up to the next section header, containing a
    # `token =` assignment.
    awk '/^\[registry\]/ {in_reg = 1; next}
         /^\[/           {in_reg = 0}
         in_reg && /^[[:space:]]*token[[:space:]]*=/ {found = 1}
         END {exit !found}' "$f" && return 0
  done
  return 1
}
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]] && ! has_crates_io_token; then
  die "no crates.io token found.
Run \`cargo login\`, or set CARGO_REGISTRY_TOKEN."
fi

# A gpg home containing ONLY the published maintainer keys — the same
# construction sign-release.sh uses, and for the same reason: verifying against
# your own keyring only proves you can read a signature you already trust.
KEYS_HOME=$(mktemp -d)
chmod 700 "$KEYS_HOME"
# Single EXIT trap for the whole script. The worktree does not exist yet, hence
# the :- guard; it is a git worktree, so it must be removed through git rather
# than with rm, or the repo keeps a dangling administrative entry.
remove_worktree() {
  local repo="$1" tree="$2" err
  # -d, not -n: the path is assigned before `git worktree add` runs, so on an
  # add failure it does not exist and removing it would print a warning on top
  # of the real error.
  [[ -n "$tree" && -d "$tree" ]] || return 0
  # Keep git's explanation. Discarding it leaves the operator knowing cleanup
  # failed but not why, with a stale worktree registered in the parent repo.
  err=$(git -C "$repo" worktree remove --force "$tree" 2>&1) || {
    echo "warning: could not remove the worktree at $tree: $err" >&2
    echo "         run \`git -C $repo worktree prune\` to clear the stale entry" >&2
  }
}
cleanup() {
  local rc=$?
  rm -rf "$KEYS_HOME"
  remove_worktree "$REPO_ROOT" "${WORKTREE:-}"
  # Never fails: errexit applies inside the trap, and a cleanup hiccup must
  # not turn a successful, irreversible publish into a red exit.
  if [[ -n "${WORK_PARENT:-}" ]]; then
    rmdir "$WORK_PARENT" 2>/dev/null || true
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
  # Compare full fingerprints, field by field. A `grep "VALIDSIG.*$KEY"` looks
  # equivalent and is not: VALIDSIG is
  #   VALIDSIG <fpr> <date> <ts> <expire> <ver> <res> <alg> <hash> <class> <pri>
  # so `.*` reaches the timestamps and algorithm numbers too — `0` or a slice of
  # the unix timestamp matches ANY signature, making the assertion vacuous,
  # while a uid/email (which sign-release.sh accepts for this same variable)
  # matches nothing and fails a tag that is genuinely the operator's.
  # Compare PRIMARY key fingerprints. VALIDSIG's field 3 is the key that made
  # the signature, which is the signing SUBKEY whenever one exists — as it does
  # for the maintainer key in KEYS — so matching that against a primary
  # fingerprint would reject a perfectly good tag. The last field is the primary
  # key's fingerprint; older gpg omits it, hence the fallback.
  sig_fpr=$(GNUPGHOME="$KEYS_HOME" git verify-tag --raw "$TAG" 2>&1 |
    awk '$2 == "VALIDSIG" {print (NF >= 12 ? $NF : $3); exit}')
  [[ -n "$sig_fpr" ]] || die "could not read the signature fingerprint from $TAG"

  # Resolved against the KEYS-only keyring, so a uid, short id or fingerprint
  # all work — the same input forms sign-release.sh takes. Only the `fpr` record
  # that follows a `pub` record is taken: gpg also emits one per subkey, and
  # counting those would make a single unambiguous key look like several.
  mapfile -t want_fprs < <(
    gpg --homedir "$KEYS_HOME" --list-keys --with-colons "$SIGNING_KEY" 2>/dev/null |
      awk -F: '/^pub:/ {want = 1} /^fpr:/ && want {print $10; want = 0}'
  )
  case ${#want_fprs[@]} in
    0) die "SPONSORD_SIGNING_KEY '$SIGNING_KEY' matches no key published in KEYS" ;;
    1) ;;
    # gpg substring-matches uids, so several hits means the value names more
    # than one maintainer key and picking the first would silently accept a key
    # nobody intended.
    *) die "SPONSORD_SIGNING_KEY '$SIGNING_KEY' is ambiguous — it matches ${#want_fprs[@]} keys in KEYS.
Use a full fingerprint." ;;
  esac

  [[ "$sig_fpr" == "${want_fprs[0]}" ]] ||
    die "$TAG is signed by $sig_fpr, not by $SIGNING_KEY (${want_fprs[0]})"
fi

LOCAL_TAG=$(git rev-parse "refs/tags/${TAG}^{commit}")
REMOTE_TAG=$(gh api "repos/${REPO}/git/ref/tags/${TAG}" --jq .object.sha) ||
  die "tag $TAG not found on $REPO"
REMOTE_COMMIT=$(git rev-parse "${REMOTE_TAG}^{commit}")
[[ "$LOCAL_TAG" == "$REMOTE_COMMIT" ]] || die \
  "local tag $TAG ($LOCAL_TAG) differs from ${REPO}'s ($REMOTE_COMMIT).
crates.io would receive code that was never released. Reconcile the tags first.
Note the fetch above used the git remote \`origin\`, while this compares against
the GitHub API for $REPO; if SPONSORD_REPO points somewhere your origin does not,
reconcile those first."

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

# Out-of-draft is not the same as signed, and the invariant above is about the
# signature. A release can leave draft by other routes — `gh release edit
# --draft=false` by hand, or an aborted sign-release.sh finished manually — so
# check for the signature itself rather than inferring it from publication.
# SHA256SUMS.asc is the one sign-release.sh always uploads for a crate with
# archives. A crate without them (sponsord-core) carries no assets to sign: its
# signed tag, verified above against KEYS, is the attestation.
if (( ARCHIVES > 0 )); then
  gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name' 2>/dev/null |
    grep -qx 'SHA256SUMS.asc' || die \
    "release $TAG carries no SHA256SUMS.asc — it is published but was never signed.
Nothing goes to crates.io that no maintainer signature vouches for.
Run .github/scripts/sign-release.sh $TAG."
fi

# ---- its path dependencies must be on crates.io first --------------------

# The uploaded manifest carries each path dependency's `version` and no path,
# so crates.io must already serve exactly that version. Checked before anything
# is packaged: the dry run below would also fail, but with a resolver error
# that does not say what to do about it.
echo "==> Checking $CRATE's path dependencies are on crates.io"
# Captured to a variable first: neither `set -e` nor `pipefail` reaches into a
# process substitution, so a failed listing would otherwise yield an empty list
# and a vacuous pass.
DEPS_RAW=$(cd "$REPO_ROOT" && python3 "$SCRIPT_DIR/registry_deps.py" "$TAG" "$CRATE") ||
  die "could not list $CRATE's dependencies at $TAG"
DEPS=()
[[ -z "$DEPS_RAW" ]] || mapfile -t DEPS <<<"$DEPS_RAW"

# The decdn commit this tag builds, from its Cargo.lock. What crates.io
# serves for decdn must be that same code, so the locked commit has to be the
# commit of decdn's release tag for the version the aliases require.
DECDN_SHA=$(git show "${TAG}:Cargo.lock" |
  grep -m1 -oE 'git\+https://github\.com/decdn/decdn\?[^#]*#[0-9a-f]{40}' |
  cut -d'#' -f2) || true
DECDN_VERSION=""
for entry in "${DEPS[@]}"; do
  read -r _ ver origin <<<"$entry"
  [[ "$origin" == "decdn" ]] || continue
  [[ -n "$DECDN_SHA" ]] || die "Cargo.lock at $TAG pins no decdn commit"
  DECDN_VERSION="$ver"
  TAG_SHA=$(git ls-remote https://github.com/decdn/decdn \
    "refs/tags/v${ver}^{}" "refs/tags/v${ver}" | awk 'NR==1 {print $1}') ||
    die "cannot reach github.com/decdn/decdn to look up its v${ver} tag"
  [[ -n "$TAG_SHA" ]] || die \
    "decdn has no v${ver} tag, so ${ver} is not a decdn release.
crates.io can only resolve a published decdn version, so a release meant for
crates.io must lock a decdn release: move Cargo.lock to decdn's v<version> tag
and set the decdn-* versions to match (RELEASING.md), then cut a new release.
This tag's binaries and images are unaffected."
  [[ "$TAG_SHA" == "$DECDN_SHA" ]] || die \
    "Cargo.lock at $TAG locks decdn at $DECDN_SHA, but decdn v${ver} is $TAG_SHA.
What crates.io would build is not what this release was built and tested
against. Move the lock to decdn v${ver} (RELEASING.md) and cut a new release."
  break
done

for entry in "${DEPS[@]}"; do
  read -r name ver origin <<<"$entry"
  if [[ "$origin" == "decdn" && "$ver" != "$DECDN_VERSION" ]]; then
    die "$name requires '$ver' at $TAG, but the other decdn crates require $DECDN_VERSION"
  fi
  code=$(curl -s -o /dev/null -w '%{http_code}' -A "sponsord-publish-crates" \
    "https://crates.io/api/v1/crates/${name}/${ver}") ||
    die "cannot reach crates.io to check $name $ver"
  case "$code" in
    200) echo "    ${name} ${ver} is on crates.io" ;;
    404)
      if [[ "$origin" == "decdn" ]]; then
        die "${name} ${ver} is not on crates.io.
Publish decdn v${DECDN_VERSION} first (decdn's .github/scripts/publish-crates.sh),
then re-run this script. Nothing has been uploaded."
      fi
      die "${name} ${ver} is not on crates.io.
Release and publish ${name}-v${ver} first (RELEASING.md § Publishing to
crates.io), then re-run this script. Nothing has been uploaded." ;;
    *) die "crates.io returned HTTP $code for $name $ver; refusing to guess" ;;
  esac
done

# ---- publish from the tagged tree ----------------------------------------

# A detached worktree at the tag, not the current checkout: the working copy
# may carry uncommitted edits or sit on a different branch, and cargo would
# happily upload whatever is on disk. Cargo fetches decdn itself, at the
# commit the tag's Cargo.lock pins.
WORK_PARENT=$(mktemp -d)
WORKTREE="$WORK_PARENT/sponsord"
git -C "$REPO_ROOT" worktree add --detach "$WORKTREE" "$TAG" >/dev/null 2>&1 ||
  die "could not create a worktree at $TAG"
cd "$WORKTREE"

# Belt and braces over release.yml's own check: the tree being uploaded carries
# the tag's version, on the machine doing the uploading.
META=$(cargo metadata --no-deps --format-version 1) ||
  die "cargo metadata failed; cannot read $CRATE's version"
ACTUAL=$(printf '%s' "$META" | python3 -c '
import json, sys
for p in json.load(sys.stdin)["packages"]:
    if p["name"] == sys.argv[1]:
        print(p["version"])
' "$CRATE") || die "could not parse cargo metadata"
[[ "$ACTUAL" == "$VERSION" ]] ||
  die "$CRATE is at '${ACTUAL}' in the tagged tree, but the tag says $VERSION"
echo "==> $CRATE at $VERSION"

echo "==> Dry run"
# On a re-run after a successful publish this is where cargo stops, because the
# version already exists in the registry — so the message must not claim
# nothing was uploaded. That claim is exactly the belief that leads someone to
# retry a publish that already happened.
cargo publish -p "$CRATE" --locked --dry-run || die \
  "the dry run failed.

If it reports the version already exists, THE PUBLISH ALREADY SUCCEEDED — this
is a re-run, and nothing further is needed. Confirm before doing anything else:

  https://crates.io/crates/${CRATE}/${VERSION}

Otherwise this is a genuine packaging failure and nothing has been uploaded."

printf '\nAbout to publish %s %s to crates.io.\n\n' "$CRATE" "$VERSION"
printf 'This CANNOT be undone. A published version is immutable; yanking hides it\n'
printf 'from new resolutions but never frees the version or removes the code.\n'
read -r -p "Type the version ($VERSION) to continue: " confirm < /dev/tty
[[ "$confirm" == "$VERSION" ]] || die "aborted"

echo "==> Publishing"
cargo publish -p "$CRATE" --locked || die \
  "cargo publish failed. If it got as far as the upload, the version may be
published already — check before doing anything else:

  https://crates.io/crates/${CRATE}/${VERSION}

If it is not there, nothing was uploaded; fix the problem and re-run."

# ---- confirm ---------------------------------------------------------------

# cargo returning 0 means the upload was accepted, not that the index has
# caught up. Ask crates.io what it actually serves.
#
# Nothing below exits non-zero. The publish already happened and is
# irreversible, so a red exit here would misrepresent a successful release — and
# send the operator looking for something to retry, which is the one thing they
# must not do. Anything unresolved is reported as a warning.
echo "==> Confirming on crates.io"
found="" transport=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  # -f makes curl exit 22 on an HTTP >=400, which is the "not served yet"
  # signal. Any other non-zero is a transport problem — DNS, proxy, TLS — and
  # reporting that as index lag would send the operator to the wrong place.
  rc=0
  curl -sf -A "sponsord-publish-crates" \
    "https://crates.io/api/v1/crates/${CRATE}/${VERSION}" >/dev/null 2>&1 || rc=$?
  if (( rc == 0 )); then
    found=1
    break
  fi
  (( rc == 22 )) || transport=1
  sleep 3
done

echo
echo "Published $CRATE $VERSION: https://crates.io/crates/${CRATE}/${VERSION}"
if [[ -z "$found" && -n "$transport" ]]; then
  echo "warning: could not reach crates.io to confirm it (network, not lag)." >&2
  echo "Verify manually. Do NOT re-run this script." >&2
elif [[ -z "$found" ]]; then
  echo "warning: crates.io does not serve it yet. This is normally index lag;" >&2
  echo "the upload itself succeeded. Re-check in a minute. Do NOT re-run." >&2
fi
