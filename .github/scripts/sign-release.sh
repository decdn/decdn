#!/usr/bin/env bash
# Signs and publishes a release draft staged by .github/workflows/release.yml.
#
# The workflow builds the archives, the container image and the SBOM, but signs
# nothing and publishes nothing — the GitHub Release is left as a draft and the
# image manifest is pushed untagged. This script is the human step: a maintainer
# verifies what CI produced, signs it with their own GPG key, promotes the image
# tags, and publishes. There is no signing key in Actions secrets.
#
# Usage:  .github/scripts/sign-release.sh v0.1.2
#
# Environment:
#   DECDN_REPO            override the owner/repo (default: decdn/decdn)
#   DECDN_SIGNING_KEY     key to sign with (default: gpg's default secret key).
#                         Whatever it resolves to must be published in KEYS.
#   DECDN_SKIP_IMAGE_TAGS set to 1 to publish without creating ANY pullable
#                         image tag — the release then ships with only the
#                         signed digest in image-digest.txt
#   DECDN_SKIP_DOCKERHUB  set to 1 to tag on GHCR only, skipping the Docker Hub
#                         mirror (implied by DECDN_SKIP_IMAGE_TAGS)
#   DECDN_DOCKERHUB_REPO  override the Docker Hub repository
#                         (default: decdn/decdn-node)
#
# Re-running is safe at any point before the release is published: signatures
# are re-uploaded with --clobber and the tag promotion is idempotent. Once the
# release is out of draft the script refuses to run again.
set -euo pipefail

TAG="${1:?tag required, e.g. v0.1.2}"
VERSION="${TAG#v}"
REPO="${DECDN_REPO:-decdn/decdn}"
# `-node`: the image carries the daemon only, so it is not named after the repo.
# Must match release.yml's IMAGE_NAME, which is what wrote image-digest.txt.
IMAGE="ghcr.io/${REPO}-node"
DOCKERHUB_IMAGE="docker.io/${DECDN_DOCKERHUB_REPO:-decdn/decdn-node}"
SIGNING_KEY="${DECDN_SIGNING_KEY:-}"
SKIP_IMAGE_TAGS="${DECDN_SKIP_IMAGE_TAGS:-}"
# Skipping every tag necessarily skips the mirror: there would be nothing to
# mirror, and pushing an untagged copy to a second registry helps nobody.
SKIP_DOCKERHUB="${DECDN_SKIP_DOCKERHUB:-${SKIP_IMAGE_TAGS:-}}"

# The three files a signature covers. SHA256SUMS transitively covers every
# archive, so the archives carry no individual .asc.
SIGN_TARGETS=(
  "SHA256SUMS"
  "image-digest.txt"
  "decdn-${VERSION}-sbom.spdx.json"
)

die() { echo "error: $*" >&2; exit 1; }

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
if [[ -z "$SKIP_IMAGE_TAGS" ]]; then
  command -v docker >/dev/null ||
    die "docker not found on PATH (set DECDN_SKIP_IMAGE_TAGS=1 to skip tag promotion)"
  docker buildx version >/dev/null 2>&1 ||
    die "docker buildx not available (set DECDN_SKIP_IMAGE_TAGS=1 to skip tag promotion)"

  # A warning, not a die: credential helpers keep no `auths` entry, so a miss
  # here is not proof of a missing login. Docker Hub is the second registry and
  # the newer requirement, so it is the one worth flagging early — an operator
  # who has only ever run `docker login ghcr.io` gets told now rather than
  # after the signatures are already uploaded.
  if [[ -z "$SKIP_DOCKERHUB" ]] &&
     ! grep -q 'index\.docker\.io' "${DOCKER_CONFIG:-$HOME/.docker}/config.json" 2>/dev/null; then
    echo "warning: no Docker Hub credentials found in the docker config." >&2
    echo "         Run \`docker login docker.io\`, or set DECDN_SKIP_DOCKERHUB=1" >&2
    echo "         to tag on GHCR only." >&2
  fi
fi

# Resolve the signing key to a full fingerprint. `gpg --list-secret-keys` with
# NO argument exits 0 even on a completely empty keyring, so testing its exit
# status proves nothing — extracting a fingerprint is the real check. Resolving
# to a fingerprint also avoids gpg's substring uid matching, where a loose
# DECDN_SIGNING_KEY could silently select a key nobody intended.
FPR=$(gpg --list-secret-keys --with-colons ${SIGNING_KEY:+"$SIGNING_KEY"} 2>/dev/null |
  awk -F: '/^fpr:/ {print $10; exit}') || true
# shellcheck disable=SC2016  # the quotes are inside a double-quoted ${:+}, so it does expand
[[ -n "$FPR" ]] || die "no usable gpg secret key${SIGNING_KEY:+ matching '$SIGNING_KEY'}"

# A gpg home containing ONLY the published maintainer keys. Both the tag
# signature and the signatures produced below are verified against this rather
# than the personal keyring: verifying against your own keyring only proves you
# can read a signature you already trust, which was never in doubt. This is the
# question consumers will actually ask. It is a full home rather than a bare
# keyring file so `git verify-tag` can use it via GNUPGHOME.
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

gpg --homedir "$KEYS_HOME" --list-keys "$FPR" >/dev/null 2>&1 || die \
  "signing key $FPR is not published in KEYS.
Consumers follow SECURITY.md and would reject this signature. Add your public
key to KEYS first (RELEASING.md § One-time setup), or point DECDN_SIGNING_KEY
at a key that is already there."

echo "==> Signing as $FPR"

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
  "local tag $TAG ($LOCAL_TAG) differs from origin ($REMOTE_COMMIT).
The draft was built from origin's tag, so signing now would vouch for
artifacts you have not verified. Reconcile the tags first."

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

# Anchored match, not a prefix glob: a prefix test passes on a multi-line file
# whose second line names a different registry, and on a truncated digest.
DIGEST_REF=$(tr -d '\r' < image-digest.txt | head -n1)
[[ $(wc -l < image-digest.txt) -le 1 ]] ||
  die "image-digest.txt has more than one line"
[[ "$DIGEST_REF" =~ ^"${IMAGE}"@sha256:[0-9a-f]{64}$ ]] ||
  die "image-digest.txt is not a single $IMAGE digest reference: $DIGEST_REF"

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

if [[ -n "$SKIP_IMAGE_TAGS" ]]; then
  echo "==> Skipping image tag promotion (DECDN_SKIP_IMAGE_TAGS set)"
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
    PROMOTE_TAGS=( "latest" "$VERSION" "${VERSION%.*}" )
  fi

  DIGEST="${DIGEST_REF#*@}"

  # Points every tag in PROMOTE_TAGS at the signed digest, in $1.
  #
  # `imagetools create` copies the source manifest verbatim, so this both
  # promotes within GHCR and copies to another registry without a rebuild or a
  # local pull — and the copy keeps the same digest, which is what lets one
  # signature over image-digest.txt vouch for the mirror too. The inspect loop
  # is what PROVES that rather than assuming it: if a future buildx ever
  # rewrote the manifest, the digest would move and this would refuse to go on.
  promote_to() {
    local target="$1" login_hint="$2" t got create_args=()
    for t in "${PROMOTE_TAGS[@]}"; do
      create_args+=( -t "${target}:${t}" )
    done
    docker buildx imagetools create "${create_args[@]}" "${IMAGE}@${DIGEST}" || die \
      "failed to promote image tags on ${target} (registry auth? run \`docker login ${login_hint}\`).
Signatures are uploaded and the release is still a draft.
Fix the problem and re-run this script — it is idempotent."

    for t in "${PROMOTE_TAGS[@]}"; do
      got=$(docker buildx imagetools inspect "${target}:${t}" \
        --format '{{.Manifest.Digest}}' 2>/dev/null) || got=""
      [[ "$got" == "$DIGEST" ]] ||
        die "${target}:${t} resolves to '${got}', not the signed digest ${DIGEST}"
      echo "    ${target}:${t} -> ${DIGEST}"
    done
  }

  # Tags the untagged manifest CI already pushed — no rebuild, no pull — so
  # every tag a user can pull resolves to the digest signed above.
  echo "==> Promoting image tags to the signed digest: ${PROMOTE_TAGS[*]}"
  promote_to "$IMAGE" "ghcr.io"

  if [[ -n "$SKIP_DOCKERHUB" ]]; then
    echo "==> Skipping the Docker Hub mirror (DECDN_SKIP_DOCKERHUB set)"
  else
    # Before the release leaves draft, not after: the two registries are
    # advertised as interchangeable, so publishing with only one populated
    # would send half of `docker pull` users to a tag that does not exist.
    echo "==> Mirroring the signed digest to $DOCKERHUB_IMAGE"
    promote_to "$DOCKERHUB_IMAGE" "docker.io"
  fi
fi

echo "==> Publishing $TAG"
gh release edit "$TAG" --repo "$REPO" --draft=false || die \
  "signatures are uploaded and the image tags are promoted on every registry,
but the release is STILL A DRAFT — :latest now serves an unpublished release.
Re-run this script, or publish manually:
gh release edit $TAG --repo $REPO --draft=false"

echo
echo "Published: https://github.com/${REPO}/releases/tag/${TAG}"
