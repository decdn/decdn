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
#   DECDN_ALLOW_RATE_LIMIT set to 1 to publish more than 5 brand-new crates in
#                          one run — only once crates.io has raised this repo's
#                          publish-new limit (see the check below)
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
ALLOW_RATE_LIMIT="${DECDN_ALLOW_RATE_LIMIT:-}"

die() { echo "error: $*" >&2; exit 1; }

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
# Single EXIT trap for the whole script. WORKTREE does not exist yet, hence the
# :- guard; it is a git worktree, so it must be removed through git rather than
# with rm, or the parent repo keeps a dangling administrative entry.
cleanup() {
  local rc=$? err
  rm -rf "$KEYS_HOME"
  # -d, not -n: WORKTREE is assigned before `git worktree add` runs, so on an
  # add failure the path does not exist and removing it would print a warning
  # on top of the real error.
  if [[ -n "${WORKTREE:-}" && -d "${WORKTREE:-}" ]]; then
    # Keep git's explanation. Discarding it leaves the operator knowing cleanup
    # failed but not why, with a stale worktree registered in the parent repo.
    err=$(git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" 2>&1) || {
      echo "warning: could not remove the worktree at $WORKTREE: $err" >&2
      echo "         run \`git worktree prune\` to clear the stale entry" >&2
    }
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
    0) die "DECDN_SIGNING_KEY '$SIGNING_KEY' matches no key published in KEYS" ;;
    1) ;;
    # gpg substring-matches uids, so several hits means the value names more
    # than one maintainer key and picking the first would silently accept a key
    # nobody intended.
    *) die "DECDN_SIGNING_KEY '$SIGNING_KEY' is ambiguous — it matches ${#want_fprs[@]} keys in KEYS.
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
the GitHub API for $REPO; if DECDN_REPO points somewhere your origin does not,
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
# SHA256SUMS.asc is the one sign-release.sh always uploads.
gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name' 2>/dev/null |
  grep -qx 'SHA256SUMS.asc' || die \
  "release $TAG carries no SHA256SUMS.asc — it is published but was never signed.
Nothing goes to crates.io that no maintainer signature vouches for.
Run .github/scripts/sign-release.sh $TAG."

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
#
# Captured to a variable first, NOT piped straight into `mapfile < <(...)`:
# neither `set -e` nor `pipefail` reaches into process substitution, so a
# `cargo metadata` that dies after emitting some packages yields a silently
# TRUNCATED list with rc=0. That matters because `cargo publish --workspace`
# below publishes every crate regardless — a short list would narrow the version
# assertion while leaving the unchecked crates to upload irreversibly.
META=$(cargo metadata --no-deps --format-version 1) ||
  die "cargo metadata failed; cannot enumerate the crates to publish"

mapfile -t CRATES < <(printf '%s' "$META" | python3 -c '
import json, sys
for p in sorted(json.load(sys.stdin)["packages"], key=lambda p: p["name"]):
    if p.get("publish") == []:          # publish = false
        continue
    print(p["name"], p["version"])
') || die "could not parse cargo metadata"

# Cross-check the count against the manifest set, so a partial parse is caught
# rather than silently shrinking what gets asserted.
EXPECTED=$(printf '%s' "$META" |
  python3 -c 'import json,sys; print(sum(1 for p in json.load(sys.stdin)["packages"] if p.get("publish") != []))')
(( ${#CRATES[@]} > 0 )) || die "no publishable crates found in the workspace"
(( ${#CRATES[@]} == EXPECTED )) ||
  die "crate list is truncated: parsed ${#CRATES[@]} of $EXPECTED publishable crates"

for entry in "${CRATES[@]}"; do
  read -r name ver <<<"$entry"
  [[ "$ver" == "$VERSION" ]] ||
    die "$name is at $ver, but the tag says $VERSION"
done
echo "==> ${#CRATES[@]} crates at $VERSION"

# crates.io rate-limits crate CREATION far harder than updates: the PublishNew
# limiter allows a burst of 5 and then refills roughly one per 10 minutes, while
# PublishUpdate is a burst of 30. Publishing more than 5 brand-new crates in one
# `cargo publish --workspace` run therefore gets a 429 partway through — landing
# in exactly the unrecoverable state described at the top of this file, with
# some crates permanently uploaded and the version spent.
#
# Failing here is free; failing after the fifth upload is not.
echo "==> Checking how many crates are new to crates.io"
NEW_CRATES=()
for entry in "${CRATES[@]}"; do
  read -r name _ <<<"$entry"
  code=$(curl -s -o /dev/null -w '%{http_code}' -A "decdn-publish-crates" \
    "https://crates.io/api/v1/crates/${name}") ||
    die "cannot reach crates.io to check which crates already exist"
  case "$code" in
    200) ;;
    404) NEW_CRATES+=("$name") ;;
    *)   die "crates.io returned HTTP $code for $name; refusing to guess" ;;
  esac
done

PUBLISH_NEW_BURST=5
if (( ${#NEW_CRATES[@]} > PUBLISH_NEW_BURST )) && [[ -z "$ALLOW_RATE_LIMIT" ]]; then
  die "${#NEW_CRATES[@]} crates do not exist on crates.io yet, over the burst of
$PUBLISH_NEW_BURST that the PublishNew rate limit allows:

$(printf '  %s\n' "${NEW_CRATES[@]}")

A single run would be rate-limited (429) partway through, leaving some crates
permanently published and the version spent. Do one of these first:

  1. Ask the crates.io team to raise this repo's publish-new limit, then re-run.
  2. Publish the new crates by hand, in dependency order, spacing them out; the
     tagged release then only performs PublishUpdate, which is not constrained.

Set DECDN_ALLOW_RATE_LIMIT=1 to override if the limit has already been raised."
fi
[[ ${#NEW_CRATES[@]} -eq 0 ]] ||
  echo "    ${#NEW_CRATES[@]} new, ${#CRATES[@]} total (within the burst of $PUBLISH_NEW_BURST)"

echo "==> Dry run"
# On a re-run after a successful publish this is where cargo stops, because the
# version already exists in the registry — so the message must not claim
# nothing was uploaded. That claim is exactly the belief that leads someone to
# retry a publish that already happened.
cargo publish --workspace --locked --dry-run || die \
  "the dry run failed.

If it reports the version already exists, THE PUBLISH ALREADY SUCCEEDED — this
is a re-run, and nothing further is needed. Confirm before doing anything else:

  https://crates.io/crates/${CRATES[0]%% *}/$VERSION

Otherwise this is a genuine packaging failure and nothing has been uploaded."

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
#
# Nothing below exits non-zero. The publish already happened and is irreversible,
# so a red exit here would misrepresent a fully successful release — and send the
# operator looking for something to retry, which is the one thing they must not
# do. Anything unresolved is reported as a warning after the success banner.
echo "==> Confirming on crates.io"
missing=() unreachable=()
for entry in "${CRATES[@]}"; do
  read -r name ver <<<"$entry"
  found="" transport=""
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    # -f makes curl exit 22 on an HTTP >=400, which is the "not served yet"
    # signal. Any other non-zero is a transport problem — DNS, proxy, TLS — and
    # reporting that as index lag would send the operator to the wrong place.
    # rc is captured rather than read from $? after the `if`, so the meaning
    # does not depend on which command ran last.
    rc=0
    curl -sf -A "decdn-publish-crates" \
      "https://crates.io/api/v1/crates/${name}/${ver}" >/dev/null 2>&1 || rc=$?
    if (( rc == 0 )); then
      found=1
      break
    fi
    (( rc == 22 )) || transport=1
    sleep 3
  done
  if [[ -n "$found" ]]; then
    echo "    https://crates.io/crates/${name}/${ver}"
  elif [[ -n "$transport" ]]; then
    unreachable+=("${name} ${ver}")
  else
    missing+=("${name} ${ver}")
  fi
done

echo
echo "Published ${#CRATES[@]} crates at $VERSION."

if (( ${#missing[@]} > 0 )); then
  echo
  echo "warning: crates.io does not serve these yet:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  echo "This is normally index lag; the upload itself succeeded. Re-check in a" >&2
  echo "minute. Do NOT re-run this script." >&2
fi

if (( ${#unreachable[@]} > 0 )); then
  echo
  echo "warning: could not reach crates.io to confirm these (network, not lag):" >&2
  printf '  %s\n' "${unreachable[@]}" >&2
  echo "Verify manually at https://crates.io/crates/<name>. Do NOT re-run this" >&2
  echo "script — the publish above already succeeded." >&2
fi

echo
echo "Published ${#CRATES[@]} crates at $VERSION."
