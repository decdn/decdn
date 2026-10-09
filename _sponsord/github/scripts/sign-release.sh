#!/usr/bin/env bash
# Signs and publishes a release draft staged by .github/workflows/release.yml.
#
# The workspace is released as a whole, from a `v<version>` tag, and
# release_plan.py says what every release carries: the archives of sponsord,
# sponsord-onramp and decdn-sponsored under one SHA256SUMS, and a container
# image per binary with its SBOM and digest file.
#
# The workflow builds all of that but signs nothing and publishes nothing — the
# GitHub Release is left as a draft and the image manifest is pushed untagged.
# This script is the human step: a maintainer verifies what CI produced, signs
# it with their own GPG key, promotes the image tags, and publishes. There is no
# signing key in Actions secrets.
#
# Usage:  .github/scripts/sign-release.sh v0.1.2
#
# Environment:
#   SPONSORD_REPO            override the owner/repo (default: decdn/sponsord)
#   SPONSORD_SIGNING_KEY     key to sign with. Unset, the script uses the one
#                            secret key with a live @decdn.org uid that is
#                            published in KEYS, else the one secret key
#                            published in KEYS; two or more candidates stop
#                            the script. gpg.conf's default-key is not read.
#                            Whatever it resolves to must be published in KEYS.
#   SPONSORD_SKIP_IMAGE_TAGS set to 1 to publish without creating ANY pullable
#                            image tag — the release then ships with only the
#                            signed digest in its <image>-image-digest.txt
#   SPONSORD_SKIP_DOCKERHUB  set to 1 to tag on GHCR only, skipping the Docker
#                            Hub mirror (implied by SPONSORD_SKIP_IMAGE_TAGS)
#   SPONSORD_DOCKERHUB_NAMESPACE  override the Docker Hub namespace the image
#                            is mirrored under (default: the repo owner)
#
# Re-running is safe at any point before the release is published: signatures
# are re-uploaded with --clobber and the tag promotion is idempotent. Once the
# release is out of draft the script refuses to run again.
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

TAG="${1:?tag required, e.g. v0.1.2}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
command -v python3 >/dev/null || die "python3 not found on PATH"
# The same plan release.yml staged the draft from; refuses a tag it does not
# know rather than guessing what to sign.
PLAN=$(python3 "$SCRIPT_DIR/release_plan.py" "$TAG") ||
  die "$TAG is not a release tag (vMAJOR.MINOR.PATCH[-pre])"
plan_get() { sed -n "s/^$1=//p" <<<"$PLAN"; }
VERSION=$(plan_get version)
ARCHIVES=$(plan_get archives)

REPO="${SPONSORD_REPO:-decdn/sponsord}"
OWNER="${REPO%%/*}"
# One image per binary, named after it. release.yml pushed each as
# `ghcr.io/<owner>/<image>` and wrote that into its digest file
# (check-image-name.sh keeps the names in step).
read -r -a IMAGES <<<"$(plan_get images)"
(( ${#IMAGES[@]} > 0 )) || die "release_plan.py lists no images for $TAG"
GHCR_NAMESPACE="ghcr.io/${OWNER}"
# Derived from $REPO, exactly like the GHCR names. Hard-defaulting this to
# `decdn` would mean a SPONSORD_REPO=<fork> rehearsal promotes GHCR tags on the
# fork while pushing `latest` to the PRODUCTION Docker Hub repositories — which
# the maintainer is logged in to, so it would succeed silently.
DOCKERHUB_NAMESPACE="docker.io/${SPONSORD_DOCKERHUB_NAMESPACE:-${OWNER}}"
SIGNING_KEY="${SPONSORD_SIGNING_KEY:-}"

# The files a signature covers. SHA256SUMS transitively covers every archive,
# so the archives carry no individual .asc.
SIGN_TARGETS=("SHA256SUMS")
for name in "${IMAGES[@]}"; do
  SIGN_TARGETS+=("${name}-image-digest.txt" "${name}-${VERSION}-sbom.spdx.json")
done

# `1`/`true`/`yes` enable, `0`/`false`/`no`/empty do not, anything else is a
# typo and stops the script. A bare `[[ -n ]]` test would make
# SPONSORD_SKIP_DOCKERHUB=0 mean "skip" — the opposite of what someone typing 0
# intends — and SPONSORD_SKIP_IMAGE_TAGS=0 would ship a release with no pullable
# image tag at all.
enabled() {
  local name="$1" value="${2:-}"
  case "${value,,}" in
    ''|0|false|no) return 1 ;;
    1|true|yes)    return 0 ;;
    *) die "$name must be 1/true/yes or 0/false/no, got '$value'" ;;
  esac
}

SKIP_IMAGE_TAGS=""
if enabled SPONSORD_SKIP_IMAGE_TAGS "${SPONSORD_SKIP_IMAGE_TAGS:-}"; then
  SKIP_IMAGE_TAGS=1
fi
# Skipping every tag necessarily skips the mirror: there would be nothing to
# mirror, and pushing an untagged copy to a second registry helps nobody.
SKIP_DOCKERHUB="$SKIP_IMAGE_TAGS"
if enabled SPONSORD_SKIP_DOCKERHUB "${SPONSORD_SKIP_DOCKERHUB:-}"; then
  SKIP_DOCKERHUB=1
fi

# ---- preconditions -------------------------------------------------------

for tool in gh gpg git sha256sum; do
  command -v "$tool" >/dev/null || die "$tool not found on PATH"
done

gh auth status >/dev/null 2>&1 || die "gh is not authenticated; run \`gh auth login\`"

REPO_ROOT=$(git rev-parse --show-toplevel) || die "not inside a git repository"
KEYS_FILE="$REPO_ROOT/KEYS"
[[ -f "$KEYS_FILE" ]] || die "KEYS not found at $KEYS_FILE"

# Registry auth is the likeliest runtime failure (PATs expire) and the tag
# promotion is the last step, so check it up front rather than after signing.
if [[ -z "$SKIP_IMAGE_TAGS" ]] && (( ${#IMAGES[@]} > 0 )); then
  command -v docker >/dev/null ||
    die "docker not found on PATH (set SPONSORD_SKIP_IMAGE_TAGS=1 to skip tag promotion)"
  docker buildx version >/dev/null 2>&1 ||
    die "docker buildx not available (set SPONSORD_SKIP_IMAGE_TAGS=1 to skip tag promotion)"

  # A warning, not a die: some credential setups (per-registry `credHelpers`,
  # external tooling) keep no `auths` entry, so a miss here is not proof of a
  # missing login. Docker Hub is the second registry and
  # the newer requirement, so it is the one worth flagging early — an operator
  # who has only ever run `docker login ghcr.io` gets told now rather than
  # after the signatures are already uploaded.
  if [[ -z "$SKIP_DOCKERHUB" ]] &&
     ! grep -q 'index\.docker\.io' "${DOCKER_CONFIG:-$HOME/.docker}/config.json" 2>/dev/null; then
    echo "warning: no Docker Hub credentials found in the docker config." >&2
    echo "         Run \`docker login docker.io\`, or set SPONSORD_SKIP_DOCKERHUB=1" >&2
    echo "         to tag on GHCR only." >&2
  fi
fi

# A gpg home containing ONLY the published maintainer keys. Key selection reads
# it to learn which keys are published, and both the tag signature and the
# signatures produced below are verified against it rather than the personal
# keyring: verifying against your own keyring only proves you can read a
# signature you already trust, which was never in doubt. This is the question
# consumers will actually ask. It is a full home rather than a bare keyring file
# so `git verify-tag` can use it via GNUPGHOME.
KEYS_HOME=$(mktemp -d)
chmod 700 "$KEYS_HOME"
# Single EXIT trap for the whole script — a second `trap ... EXIT` later would
# replace this one and leak the key home. WORKDIR does not exist yet, hence the
# :- guard.
cleanup() {
  local rc=$?
  rm -rf "$KEYS_HOME"
  if [[ -n "${WORKDIR:-}" ]]; then
    if (( rc == 0 )); then
      rm -rf "$WORKDIR"
    else
      # Keep the artifacts on failure: a checksum mismatch or a bad signature
      # is exactly when the operator needs to look at the bytes, and
      # re-downloading costs several hundred megabytes.
      echo "artifacts kept for inspection: $WORKDIR" >&2
    fi
  fi
}
trap cleanup EXIT
gpg --homedir "$KEYS_HOME" --import "$KEYS_FILE" >/dev/null 2>&1 ||
  die "KEYS is not a valid OpenPGP keyring"

# gpg's stderr from the key listings below, shown when one fails. It lives in
# KEYS_HOME so the EXIT trap removes it.
GPG_ERR="$KEYS_HOME/list.err"

# Reads a `gpg --with-colons` key listing on stdin and prints the primary-key
# fingerprint of each key. Only the `fpr` right after a `sec`/`pub` record is
# taken: gpg emits one per subkey too, and a subkey would otherwise be mistaken
# for a second key.
primary_fprs() {
  awk -F: '/^(sec|pub):/ {want = 1} /^fpr:/ && want {if ($10 != "") print $10; want = 0}'
}

# Like primary_fprs, but prints only keys that can sign today: field 2 is not
# `r`/`e`/`i`/`d` (revoked, expired, invalid, disabled) and field 12 holds `S`,
# which gpg sets only while some part of the key is able to sign.
#
# With the argument `decdn`, a key must also carry a uid that is not revoked or
# expired and whose email is a @decdn.org address. The email is the one gpg's
# own `<address>` lookup uses: the text inside the uid's first `<…>`, whatever
# follows it, or the whole uid when it has no `<…>`. It must be one address
# with nothing around it, matched case-insensitively and anchored at both ends,
# so `decdn.org.example`, `notdecdn.org`, `eu.decdn.org`, `Name me@decdn.org`
# and `Name <me@example.com> <me@decdn.org>` do not qualify.
signing_fprs() {
  awk -F: -v decdn="${1:-}" '
    function flush() { if (fpr != "" && (hit || !decdn)) print fpr }
    /^(sec|pub):/ {
      flush(); fpr = ""; hit = 0; want = 1
      usable = ($2 !~ /^[reid]$/ && $12 ~ /S/)
      next
    }
    /^fpr:/ && want { if (usable) fpr = $10; want = 0; next }
    /^uid:/ && $2 !~ /^[re]$/ {
      email = tolower($10)
      if (match(email, /<[^<>]*>/)) email = substr(email, RSTART + 1, RLENGTH - 2)
      if (email ~ /^[^@<> \t]+@decdn\.org$/) hit = 1
    }
    END { flush() }
  '
}

# Every key KEYS publishes that can sign today. A key that KEYS marks revoked or
# expired is left out even when the local copy still looks live: consumers
# import KEYS, so its view of the key is the one that counts.
KEYS_LISTING=$(gpg --homedir "$KEYS_HOME" --list-keys --with-colons 2>"$GPG_ERR") ||
  die "gpg could not list the keys in KEYS:
$(<"$GPG_ERR")"
declare -A PUBLISHED=()
while read -r f; do
  PUBLISHED[$f]=1
done < <(signing_fprs <<<"$KEYS_LISTING")

# Reads fingerprints on stdin and prints those that are in PUBLISHED.
published_only() {
  local f
  while read -r f; do
    if [[ -n "${PUBLISHED[$f]:-}" ]]; then
      printf '%s\n' "$f"
    fi
  done
}

# Resolve the signing key to a full fingerprint, in this order:
#   1. SPONSORD_SIGNING_KEY, when set.
#   2. The one secret key that can sign, has a live @decdn.org uid and is
#      published in KEYS. A maintainer keyring usually holds a personal key as
#      well, so this rule keeps keyring order out of the choice.
#   3. The one secret key that can sign and is published in KEYS, whatever its
#      uids.
# Two or more candidates at step 2 or 3 stop the script. Taking the first would
# guess which published key to sign with, by keyring order. gpg.conf's
# default-key is not read: SPONSORD_SIGNING_KEY is how to name a key.
if [[ -n "$SIGNING_KEY" ]]; then
  SECRET_LISTING=$(gpg --list-secret-keys --with-colons "$SIGNING_KEY" 2>"$GPG_ERR") ||
    die "no gpg secret key matches SPONSORD_SIGNING_KEY '$SIGNING_KEY':
$(<"$GPG_ERR")"
  # Every match counts here, usable or not: the question is which key the
  # operator named, and an unusable one fails the KEYS check below instead.
  mapfile -t SECRET_FPRS < <(primary_fprs <<<"$SECRET_LISTING")
  (( ${#SECRET_FPRS[@]} > 0 )) ||
    die "no gpg secret key matches SPONSORD_SIGNING_KEY '$SIGNING_KEY'"

  # gpg substring-matches uids, so a loose SPONSORD_SIGNING_KEY can select more
  # than one key. Taking the first silently signs the release with a key the
  # operator did not name — which the KEYS check below would not catch, since
  # it only asks whether the key is *a* published maintainer key.
  (( ${#SECRET_FPRS[@]} == 1 )) ||
    die "SPONSORD_SIGNING_KEY '$SIGNING_KEY' is ambiguous — it matches ${#SECRET_FPRS[@]} secret keys:
$(printf '  %s\n' "${SECRET_FPRS[@]}")
Use a full fingerprint."
  FPR="${SECRET_FPRS[0]}"
  KEY_SOURCE="SPONSORD_SIGNING_KEY"
else
  # With no argument, gpg exits 0 and prints nothing on an empty keyring, and
  # also when gpg-agent is not running. The exit status only catches gpg itself
  # failing, so an empty listing is checked on its own.
  SECRET_LISTING=$(gpg --list-secret-keys --with-colons 2>"$GPG_ERR") ||
    die "gpg could not list your secret keys:
$(<"$GPG_ERR")"
  [[ -n "$SECRET_LISTING" ]] ||
    die "gpg lists no secret keys (is gpg-agent running, and is GNUPGHOME right?)"

  mapfile -t DECDN_FPRS < <(signing_fprs decdn <<<"$SECRET_LISTING" | published_only)
  (( ${#DECDN_FPRS[@]} <= 1 )) ||
    die "found ${#DECDN_FPRS[@]} @decdn.org secret keys published in KEYS:
$(printf '  %s\n' "${DECDN_FPRS[@]}")
Set SPONSORD_SIGNING_KEY to the full fingerprint of the one to sign with."

  if (( ${#DECDN_FPRS[@]} == 1 )); then
    FPR="${DECDN_FPRS[0]}"
    KEY_SOURCE="decdn.org key from KEYS"
  else
    mapfile -t CANDIDATES < <(signing_fprs <<<"$SECRET_LISTING" | published_only)
    case ${#CANDIDATES[@]} in
      1)
        FPR="${CANDIDATES[0]}"
        KEY_SOURCE="only secret key in KEYS"
        ;;
      0)
        mapfile -t SECRET_FPRS < <(primary_fprs <<<"$SECRET_LISTING")
        die "none of your secret keys is published in KEYS and able to sign:
$(printf '  %s\n' "${SECRET_FPRS[@]}")
Consumers follow SECURITY.md and would reject a signature from any of them. Add
your public key to KEYS first (RELEASING.md § One-time setup)."
        ;;
      *)
        die "found ${#CANDIDATES[@]} secret keys published in KEYS:
$(printf '  %s\n' "${CANDIDATES[@]}")
Set SPONSORD_SIGNING_KEY to the full fingerprint of the one to sign with."
        ;;
    esac
  fi
fi

# Steps 2 and 3 only ever pick a published key; this is the check that holds
# SPONSORD_SIGNING_KEY to the same rule.
[[ -n "${PUBLISHED[$FPR]:-}" ]] || die \
  "signing key $FPR is not published in KEYS, or KEYS marks it revoked or expired.
Consumers follow SECURITY.md and would reject this signature. Add your public
key to KEYS first (RELEASING.md § One-time setup), or point SPONSORD_SIGNING_KEY
at a key that is already there."

echo "==> Signing as $FPR ($KEY_SOURCE)"

# The local tag must match origin's. `git fetch --tags` does NOT update a tag
# that already exists locally, so without --force a stale or re-cut local tag
# verifies happily while the artifacts being signed were built from a different
# commit.
git fetch --tags --force origin >/dev/null 2>&1 ||
  die "cannot reach origin to confirm the tag"
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null ||
  die "tag $TAG does not exist"

# Verified against KEYS, not your own keyring — otherwise a tag signed by any
# key you happen to have imported would pass, while consumers following
# SECURITY.md would reject the release it produced.
echo "==> Verifying $TAG"
GNUPGHOME="$KEYS_HOME" git verify-tag "$TAG" ||
  die "$TAG is not signed by a key published in KEYS"

LOCAL_TAG=$(git rev-parse "refs/tags/${TAG}^{commit}")
REMOTE_TAG=$(gh api "repos/${REPO}/git/ref/tags/${TAG}" --jq .object.sha) ||
  die "tag $TAG not found on $REPO"
# An annotated tag's ref points at the tag object; dereference to the commit.
REMOTE_COMMIT=$(git rev-parse "${REMOTE_TAG}^{commit}")
[[ "$LOCAL_TAG" == "$REMOTE_COMMIT" ]] || die \
  "local tag $TAG ($LOCAL_TAG) differs from ${REPO}'s ($REMOTE_COMMIT).
The draft was built from that tag, so signing now would vouch for artifacts you
have not verified. Reconcile the tags first.
Note the fetch above used the git remote \`origin\`, while this compares against
the GitHub API for $REPO; if SPONSORD_REPO points somewhere your origin does not,
reconcile those first."

IS_DRAFT=$(gh release view "$TAG" --repo "$REPO" --json isDraft --jq .isDraft) || die \
  "could not read release $TAG from $REPO.
Either the workflow has not created the draft yet, or gh cannot reach GitHub.
Check: gh release view $TAG --repo $REPO"
case "$IS_DRAFT" in
  true) ;;
  false) die "release $TAG is already published; nothing to do" ;;
  *) die "unexpected isDraft value from gh: '$IS_DRAFT' (expected true or false)" ;;
esac

# ---- download and check --------------------------------------------------

# Picked up by the EXIT trap installed above; kept on failure so the operator
# can inspect the bytes rather than re-download them.
WORKDIR=$(mktemp -d)

echo "==> Downloading $TAG assets"
gh release download "$TAG" --repo "$REPO" --dir "$WORKDIR" ||
  die "could not download the draft's assets; is the workflow still running?"

cd "$WORKDIR"

for f in "${SIGN_TARGETS[@]}"; do
  [[ -f "$f" ]] || die "expected asset $f is missing from the draft"
done

# --strict, because without it a malformed line is only a warning: a truncated
# or mangled entry would scroll past amid a wall of OK lines and that archive
# would ship covered by nothing.
echo "==> Checking SHA256SUMS against the downloaded archives"
sha256sum --strict --check SHA256SUMS || die \
  "SHA256SUMS does not match the assets attached to the draft.
The release is corrupt or was tampered with. DO NOT re-run this script.
Delete the draft and the tag and cut the release again (RELEASING.md § Recovery)."

# `sha256sum --check` only answers "does every file the manifest names hash
# correctly?" — never "does the manifest name every file being published?".
# Without this, a manifest covering 8 of 10 archives verifies clean and gets
# signed as if it were complete.
echo "==> Checking every published archive is covered by the manifest"
uncovered=()
for a in ./*.tar.gz ./*.zip; do
  [[ -e "$a" ]] || continue
  grep -qF -- "  ${a#./}" SHA256SUMS || uncovered+=("${a#./}")
done
(( ${#uncovered[@]} == 0 )) || die \
  "these published archives are not covered by SHA256SUMS: ${uncovered[*]}
Signing would vouch for a release whose manifest is incomplete.
Re-run the upload-assets job, then retry."

# Neither check above notices a release that is merely SHORT: a manifest
# naming 9 of 10 archives, with only those attached, verifies clean. The
# plan says how many a release carries; hold both to it.
echo "==> Checking the release carries all $ARCHIVES archives"
shopt -s nullglob
attached=( ./*.tar.gz ./*.zip )
shopt -u nullglob
listed=$(grep -c '' SHA256SUMS)
(( ${#attached[@]} == ARCHIVES && listed == ARCHIVES )) || die \
  "expected $ARCHIVES archives, but the draft carries ${#attached[@]}
and SHA256SUMS lists $listed. Signing would vouch for an incomplete release.
Re-run the build and upload-assets jobs, then retry."

# Anchored match, not a prefix glob: a prefix test passes on a multi-line file
# whose second line names a different registry, and on a truncated digest.
declare -A DIGESTS
for name in "${IMAGES[@]}"; do
  image="${GHCR_NAMESPACE}/${name}"
  file="${name}-image-digest.txt"
  ref=$(tr -d '\r' < "$file" | head -n1)
  # `grep -c ''`, not `wc -l`: wc counts newlines, so a two-line file with no
  # trailing newline reports 1 and sails through — which is exactly the
  # "second line names a different registry" case this guard exists to catch.
  [[ $(grep -c '' "$file") -le 1 ]] ||
    die "$file has more than one line"
  [[ "$ref" =~ ^"${image}"@sha256:[0-9a-f]{64}$ ]] ||
    die "$file is not a single $image digest reference: $ref"
  DIGESTS[$name]="${ref#*@}"
done

# ---- sign ----------------------------------------------------------------

echo "==> Signing"
for f in "${SIGN_TARGETS[@]}"; do
  rm -f "${f}.asc"
  gpg --batch --yes --armor --detach-sign --local-user "$FPR" "$f" ||
    die "failed to sign $f (passphrase or gpg-agent problem?); nothing has been published"
  # Verified against the KEYS-only keyring, so this confirms what a consumer
  # will see rather than what this machine can already read.
  gpg --homedir "$KEYS_HOME" --verify "${f}.asc" "$f" 2>/dev/null ||
    die "signature on $f does not verify against KEYS"
  echo "    signed $f"
done

# ---- publish -------------------------------------------------------------

# Only the signatures just produced. `gh release download` fetched every asset,
# so a bare ./*.asc glob would also re-upload any stray signature left on the
# draft by an earlier or abandoned run, unverified.
echo "==> Uploading signatures"
gh release upload "$TAG" --repo "$REPO" --clobber "${SIGN_TARGETS[@]/%/.asc}" ||
  die "failed to upload signatures; the release is still a draft. Re-run this script."

for stray in ./*.asc; do
  [[ -e "$stray" ]] || continue
  case " ${SIGN_TARGETS[*]/%/.asc} " in
    *" ${stray#./} "*) ;;
    *) echo "warning: draft carries an unrecognised signature: ${stray#./}" >&2 ;;
  esac
done

# The repository's "Latest release", and the images' `:latest`: a stable
# release with no higher stable release above it, so a maintenance release cut
# under a newer one takes neither. Read from the tags fetched above, in the
# repository (this script has moved into the download directory by now).
LATEST=$(cd "$REPO_ROOT" && python3 "$SCRIPT_DIR/release_plan.py" "$TAG" --latest) ||
  die "could not decide whether $TAG is the latest release"

if [[ -n "$SKIP_IMAGE_TAGS" ]]; then
  echo "==> Skipping image tag promotion (SPONSORD_SKIP_IMAGE_TAGS set)"
  echo "    This release will ship with no pullable image tag."
else
  # A prerelease gets its exact version tag and nothing else. Moving `latest`
  # or `<major>.<minor>` to an RC would hand it to every unpinned pull, and
  # `${VERSION%.*}` on 0.2.0-rc1 yields 0.2 — clobbering the stable minor tag
  # with a candidate.
  if [[ "$VERSION" == *-* ]]; then
    PROMOTE_TAGS=( "$VERSION" )
    echo "==> $VERSION is a prerelease: promoting :$VERSION only"
  else
    PROMOTE_TAGS=( "$VERSION" "${VERSION%.*}" )
    # A maintenance release under a newer one keeps `latest` where it is.
    [[ "$LATEST" == "true" ]] && PROMOTE_TAGS=( "latest" "${PROMOTE_TAGS[@]}" )
  fi

  # Every repository whose tags already point at this release, so a later
  # failure can say which. With several images, a failure on a later one
  # leaves an earlier one's `:latest` serving a release that is still a draft —
  # an operator who stops for the day must be told that, not just "re-run".
  PROMOTED=()

  # Points every tag in PROMOTE_TAGS of image $1 at the signed digest of $2.
  #
  # `imagetools create` copies the source manifest verbatim, so this both
  # promotes within GHCR and copies to another registry without a rebuild or a
  # local pull — and the copy keeps the same digest, which is what lets one
  # signature over the digest file vouch for the mirror too. The inspect loop
  # is what PROVES that rather than assuming it: if a future buildx ever
  # rewrote the manifest, the digest would move and this would refuse to go on.
  promote_to() {
    local target="$1" name="$2" login_hint="$3" t got create_args=() already=""
    local source="${GHCR_NAMESPACE}/${name}@${DIGESTS[$name]}"
    if (( ${#PROMOTED[@]} > 0 )); then
      already="
These are ALREADY promoted, so their :latest serves an unpublished release:
$(printf '  %s\n' "${PROMOTED[@]}")
Either finish this script, or re-point those tags at the previous digests."
    fi
    for t in "${PROMOTE_TAGS[@]}"; do
      create_args+=( -t "${target}:${t}" )
    done
    docker buildx imagetools create "${create_args[@]}" "$source" || die \
      "failed to promote image tags on ${target} (registry auth? run \`docker login ${login_hint}\`).
Signatures are uploaded and the release is still a draft.
Fix the problem and re-run this script — it is idempotent.${already}"

    for t in "${PROMOTE_TAGS[@]}"; do
      got=$(docker buildx imagetools inspect "${target}:${t}" \
        --format '{{.Manifest.Digest}}' 2>/dev/null) || got=""
      [[ "$got" == "${DIGESTS[$name]}" ]] ||
        die "${target}:${t} resolves to '${got}', not the signed digest ${DIGESTS[$name]}${already}"
      echo "    ${target}:${t} -> ${DIGESTS[$name]}"
    done
    PROMOTED+=("$target")
  }

  # Tags the untagged manifests CI already pushed — no rebuild, no pull — so
  # every tag a user can pull resolves to a digest signed above.
  echo "==> Promoting image tags to the signed digests: ${PROMOTE_TAGS[*]}"
  for name in "${IMAGES[@]}"; do
    promote_to "${GHCR_NAMESPACE}/${name}" "$name" "ghcr.io"
  done

  if [[ -n "$SKIP_DOCKERHUB" ]]; then
    echo "==> Skipping the Docker Hub mirror (SPONSORD_SKIP_DOCKERHUB set)"
  else
    # Before the release leaves draft, not after: the two registries are
    # advertised as interchangeable, so publishing with only one populated
    # would send half of `docker pull` users to a tag that does not exist.
    echo "==> Mirroring the signed digests to ${DOCKERHUB_NAMESPACE}/"
    for name in "${IMAGES[@]}"; do
      promote_to "${DOCKERHUB_NAMESPACE}/${name}" "$name" "docker.io"
    done
  fi
fi

echo "==> Publishing $TAG"
gh release edit "$TAG" --repo "$REPO" --draft=false --latest="$LATEST" || die \
  "signatures are uploaded and any image tags are promoted on every registry,
but the release is STILL A DRAFT — an image's :latest may now serve an
unpublished release. Re-run this script, or publish manually:
gh release edit $TAG --repo $REPO --draft=false --latest=$LATEST"

echo
echo "Published: https://github.com/${REPO}/releases/tag/${TAG}"
