#!/usr/bin/env bash
# Refuses to let a release reach crates.io unless a maintainer has signed it.
#
# Usage:  .github/scripts/check-release-signed.sh <tag> <KEYS file>
#
# A crates.io version can never be replaced, so the publish-crates workflow
# uploads nothing that no maintainer signature vouches for. Leaving draft is
# not proof of a signature: a release can leave draft by hand, or through an
# aborted sign-release.sh finished manually. This checks the signatures
# themselves:
#
# - the release is published, not a draft;
# - SHA256SUMS.asc is a good signature over SHA256SUMS by a key in the
#   <KEYS file> keyring, and nothing else, that has neither expired nor been
#   revoked. The caller passes the KEYS from origin/main, never the tag's own,
#   so the tag cannot supply its own trust root;
# - every archive SHA256SUMS names carries this release's version, so the
#   signed manifest of another release does not pass for this one;
# - SHA256SUMS.minisig is attached. `decdn self-update` checks it, so a
#   release without it strands every installed CLI on the previous version;
# - every archive SHA256SUMS names is attached, each with its <archive>.minisig,
#   which `cargo binstall` checks. sign-release.sh verified each minisign
#   signature under the maintainers' keys; this checks that they are attached.
#
# The repository is $GH_REPO, else $GITHUB_REPOSITORY, else decdn/decdn. gh
# reads its token from $GH_TOKEN.
set -euo pipefail

TAG="${1:?tag required, e.g. v0.1.2}"
VERSION="${TAG#v}"
KEYS_FILE="${2:?KEYS file required}"
REPO="${GH_REPO:-${GITHUB_REPOSITORY:-decdn/decdn}}"

die() { echo "error: $*" >&2; exit 1; }

for tool in gh gpg awk; do
  command -v "$tool" >/dev/null || die "$tool not found on PATH"
done
[[ -f "$KEYS_FILE" ]] || die "KEYS not found at $KEYS_FILE"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -m 700 "$WORK/gnupg"
mkdir "$WORK/assets"
if ! err=$(gpg --homedir "$WORK/gnupg" --batch --import "$KEYS_FILE" 2>&1); then
  die "$KEYS_FILE is not a valid OpenPGP keyring: $err"
fi

IS_DRAFT=$(gh release view "$TAG" --repo "$REPO" --json isDraft --jq .isDraft) ||
  die "could not read release $TAG from $REPO"
case "$IS_DRAFT" in
  false) ;;
  true) die "release $TAG is still a draft. Run scripts/sign-release.sh $TAG first." ;;
  *) die "unexpected isDraft value from gh: '$IS_DRAFT' (expected true or false)" ;;
esac

# gh's stderr goes to a file, not into the list: a warning on a successful
# call would otherwise become an asset name.
if ! ASSETS=$(gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name' \
                2>"$WORK/gh.err"); then
  die "cannot list the assets of release $TAG: $(cat "$WORK/gh.err")"
fi
for sig in SHA256SUMS SHA256SUMS.asc SHA256SUMS.minisig; do
  grep -qxF "$sig" <<<"$ASSETS" || die \
    "release $TAG carries no $sig, so no maintainer signature vouches for it.
Run scripts/sign-release.sh $TAG."
done

for asset in SHA256SUMS SHA256SUMS.asc; do
  gh release download "$TAG" --repo "$REPO" --pattern "$asset" --dir "$WORK/assets" ||
    die "cannot download $asset from release $TAG"
done
# `gpg --verify` exits 0 on a good signature by an expired or revoked key too,
# so the status lines decide: a GOODSIG and a VALIDSIG, and no line that marks
# the key or signature expired or revoked, or the signature bad. `git
# verify-tag` refuses those keys, and this check must not be weaker than it.
# The keyring holds nothing but KEYS.
status=$(gpg --homedir "$WORK/gnupg" --batch --status-fd 1 --verify \
  "$WORK/assets/SHA256SUMS.asc" "$WORK/assets/SHA256SUMS" 2>"$WORK/gpg.err") || true
if ! grep -q '^\[GNUPG:\] GOODSIG ' <<<"$status" ||
   ! grep -q '^\[GNUPG:\] VALIDSIG ' <<<"$status" ||
   grep -qE '^\[GNUPG:\] (BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG|KEYREVOKED|KEYEXPIRED) ' <<<"$status"
then
  die "SHA256SUMS.asc on release $TAG is not a good signature by a live key in KEYS.
Nothing goes to crates.io that no maintainer signature vouches for.
$(cat "$WORK/gpg.err")"
fi

NAMES=$(awk 'NF {print $2}' "$WORK/assets/SHA256SUMS") ||
  die "cannot read SHA256SUMS of release $TAG"
mapfile -t ARCHIVES <<<"$NAMES"
[[ -n "$NAMES" ]] || die "SHA256SUMS of release $TAG names no archive"
unsigned=()
for a in "${ARCHIVES[@]}"; do
  # release.yml names every archive <binary>-<version>-<target>.<ext>, and
  # every target starts with its architecture. Matching the architecture too
  # keeps v1.2.0-rc1's archives from passing for v1.2.0.
  case "$a" in
    *-"$VERSION"-x86_64-*|*-"$VERSION"-aarch64-*) ;;
    *) die "SHA256SUMS of release $TAG names $a, which is not a $VERSION archive.
It is the manifest of another release." ;;
  esac
  grep -qxF "$a" <<<"$ASSETS" || die "release $TAG does not carry $a, which SHA256SUMS names"
  grep -qxF "${a}.minisig" <<<"$ASSETS" || unsigned+=( "$a" )
done
(( ${#unsigned[@]} == 0 )) || die \
  "release $TAG carries no .minisig for: ${unsigned[*]}
cargo binstall would refuse those archives. Run scripts/sign-release.sh $TAG."

echo "release $TAG is signed: SHA256SUMS.asc verifies against KEYS, and" \
     "SHA256SUMS.minisig and ${#ARCHIVES[@]} archive signatures are attached"
