#!/usr/bin/env bash
# Puts decdn's `decdn` binary in <dist-dir>/{amd64,arm64}/, for the
# decdn-sponsored image (Dockerfile), which ships it beside the CLI that
# spawns it.
#
# The binary is decdn's own release archive at the tag every decdn alias in
# Cargo.toml names, the decdn the CLI is built and tested against. It is
# never compiled here: the archive is the one decdn's maintainers signed, the
# same kind the onramp's installers fetch (theirs at the operator's
# ONRAMP_DECDN_RELEASE, which need not be this tag).
#
# Verified before it is unpacked:
#   * the tag must still name the commit Cargo.lock pins, so a tag moved
#     after the pin was reviewed is refused rather than followed;
#   * the release must be immutable (GitHub refuses any change to its assets
#     or its tag once published). KEYS is read at the locked commit, so a key
#     revoked later still verifies here; immutability is what stops whoever
#     holds such a key from re-uploading archives and SHA256SUMS they signed;
#   * decdn's SHA256SUMS must carry a good signature, from a key in decdn's
#     KEYS at that commit that is neither revoked nor expired;
#   * each archive must have its own line in SHA256SUMS, and match it.
# The trust root is therefore the decdn commit a reviewed sponsord PR locked,
# the same source cargo builds the decdn crates from.
#
# Usage: .github/scripts/fetch-decdn.sh <dist-dir>
# Needs gh (authenticated, or GH_TOKEN), gpg, python3, sha256sum and tar.
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

[[ $# -eq 1 ]] || die "usage: $0 <dist-dir>"
DIST=$1

for tool in gh gpg python3 sha256sum tar; do
  command -v "$tool" >/dev/null || die "$tool not found on PATH"
done

SCRIPTS=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TAG=$("$SCRIPTS/bump-decdn.sh" pinned)
[[ -n "$TAG" ]] || die "the decdn aliases in Cargo.toml name no single release tag (a branch or rev pin has no release to fetch)"
[[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] ||
  die "the decdn pin ${TAG} is not a release tag (vX.Y.Z)"
VERSION=${TAG#v}
REPO=decdn/decdn
LOCKED=$("$SCRIPTS/bump-decdn.sh" locked-sha)

# The commit the tag names now, peeling an annotated tag to its commit.
read -r kind sha < <(gh api "repos/${REPO}/git/ref/tags/${TAG}" --jq '.object.type + " " + .object.sha') ||
  die "cannot resolve ${REPO} tag ${TAG}"
if [[ "$kind" == tag ]]; then
  read -r kind sha < <(gh api "repos/${REPO}/git/tags/${sha}" --jq '.object.type + " " + .object.sha') ||
    die "cannot peel ${REPO} tag ${TAG}"
fi
[[ "$kind" == commit && "$sha" == "$LOCKED" ]] ||
  die "${REPO} tag ${TAG} names ${kind} ${sha}, not the commit Cargo.lock pins (${LOCKED})"

immutable=$(gh api "repos/${REPO}/releases/tags/${TAG}" --jq '.immutable') ||
  die "cannot read ${REPO} release ${TAG}"
[[ "$immutable" == true ]] ||
  die "${REPO} release ${TAG} is not immutable, so its signed assets could be replaced after the pin was reviewed"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -m 700 "$WORK/gnupg"

# arch as buildx's TARGETARCH names it (the Dockerfile's dist/ layout) -> Rust
# target triple (decdn's archive names).
declare -A TRIPLES=(
  [amd64]=x86_64-unknown-linux-gnu
  [arm64]=aarch64-unknown-linux-gnu
)

archives=()
for arch in "${!TRIPLES[@]}"; do
  archives+=("decdn-${VERSION}-${TRIPLES[$arch]}.tar.gz")
done

patterns=(-p SHA256SUMS -p SHA256SUMS.asc)
for a in "${archives[@]}"; do patterns+=(-p "$a"); done
gh release download "$TAG" --repo "$REPO" --dir "$WORK" "${patterns[@]}"
# gh fails only when no pattern matches; a partial match downloads silently.
for f in SHA256SUMS SHA256SUMS.asc "${archives[@]}"; do
  [[ -s "$WORK/$f" ]] || die "decdn's ${TAG} release has no asset ${f}"
done

# KEYS at the locked commit, not at decdn's main: a key added or revoked
# later must not change whether this release verifies.
gh api -H "Accept: application/vnd.github.raw" \
  "repos/${REPO}/contents/KEYS?ref=${LOCKED}" > "$WORK/KEYS" ||
  die "cannot fetch KEYS from ${REPO} at ${LOCKED}"
gpg --homedir "$WORK/gnupg" --batch --import "$WORK/KEYS" >/dev/null 2>"$WORK/gpg.err" ||
  die "decdn's KEYS at ${LOCKED} is not a valid OpenPGP keyring: $(cat "$WORK/gpg.err")"
# A keyring holding only decdn's published keys, so a good signature means
# one of them made it, not merely some key this machine already trusts.
# gpg exits 0 for a good signature by a revoked or expired key, so its
# status lines decide, not its exit code alone.
status=$(gpg --homedir "$WORK/gnupg" --batch --status-fd 1 \
  --verify "$WORK/SHA256SUMS.asc" "$WORK/SHA256SUMS" 2>"$WORK/gpg.err") ||
  die "SHA256SUMS of decdn ${TAG} has no good signature from a key in decdn's KEYS: $(cat "$WORK/gpg.err")"
if grep -qE '^\[GNUPG:\] (REVKEYSIG|EXPKEYSIG|EXPSIG|BADSIG|ERRSIG) ' <<<"$status" ||
  ! grep -qE '^\[GNUPG:\] GOODSIG ' <<<"$status"; then
  die "SHA256SUMS of decdn ${TAG} is not signed by a valid key in decdn's KEYS:"$'\n'"$status"
fi

# Each archive must have a line: --ignore-missing alone would pass an archive
# SHA256SUMS does not list.
for a in "${archives[@]}"; do
  grep -qE "^[0-9a-f]{64} [ *]${a//./\\.}\$" "$WORK/SHA256SUMS" ||
    die "decdn's SHA256SUMS at ${TAG} does not list ${a}"
done
(cd "$WORK" && sha256sum --check --ignore-missing --quiet SHA256SUMS) ||
  die "decdn ${TAG} archives do not match its SHA256SUMS"

for arch in "${!TRIPLES[@]}"; do
  mkdir -p "$DIST/$arch"
  tar xzf "$WORK/decdn-${VERSION}-${TRIPLES[$arch]}.tar.gz" --no-same-owner -C "$DIST/$arch" decdn
  bin="$DIST/$arch/decdn"
  # -x follows symlinks; the image must get the file itself.
  [[ -f "$bin" && ! -L "$bin" && -x "$bin" ]] || die "$bin missing, not a regular file, or not executable"
done

echo "decdn ${TAG} (${LOCKED}) verified against its signed SHA256SUMS:"
sha256sum "$DIST"/*/decdn
