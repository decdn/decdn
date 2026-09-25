#!/bin/sh
# decdn-sponsored installer for macOS/Linux - served by sponsord at
# GET /decdn.sh, with the placeholders below substituted server-side (see
# crates/server/src/http/installer.rs) from ServerConfig, so nothing here
# needs an environment variable to run. The Windows twin is assets/decdn.ps1;
# the two write the same profile.
#
# Arguments, when given, are passed to decdn-sponsored after installing, so
# one line installs and downloads:
#   curl -fsSL <gateway>/decdn.sh | sh -s -- pull b3:<hash>
set -eu

GATEWAY="{{GATEWAY_BASE}}"
RPC_URL="{{RPC_URL}}"
PAYMENT_POOL="{{PAYMENT_POOL}}"
CAPACITY_BOND="{{CAPACITY_BOND}}"
CHAIN_ID="{{CHAIN_ID}}"

# The binaries come from pinned GitHub Releases. Each release is pinned by tag
# and by the SHA-256 of its SHA256SUMS file.
RELEASES="https://github.com/decdn"
DECDN_RELEASE="{{DECDN_RELEASE}}"
DECDN_SUMS_SHA256="{{DECDN_SUMS_SHA256}}"
WRAPPER_RELEASE="{{WRAPPER_RELEASE}}"
WRAPPER_SUMS_SHA256="{{WRAPPER_SUMS_SHA256}}"

BINDIR="${HOME}/.local/bin"
DECDN_DIR="${HOME}/.decdn"

die() {
  echo "decdn: $*" >&2
  exit 1
}

case "$(uname -s)" in
  Linux) OS_TRIPLE=unknown-linux-gnu ;;
  Darwin) OS_TRIPLE=apple-darwin ;;
  *) die "unsupported OS: $(uname -s) (on Windows, use decdn.ps1 in PowerShell)" ;;
esac
case "$(uname -m)" in
  x86_64 | amd64) ARCH=x86_64 ;;
  arm64 | aarch64) ARCH=aarch64 ;;
  *) die "unsupported architecture: $(uname -m)" ;;
esac
TARGET="${ARCH}-${OS_TRIPLE}"

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  die "need sha256sum or shasum to verify downloads"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
trap 'exit 130' INT TERM
mkdir -p "$BINDIR" "$DECDN_DIR"

# fetch_bin <repo> <binary> <release tag> <SHA256SUMS digest>
#
# Downloads SHA256SUMS and checks it against the pinned digest, then
# downloads the archive for this platform and checks it against SHA256SUMS.
# Nothing lands in BINDIR unless both checks pass.
fetch_bin() {
  repo="$1" bin="$2" tag="$3" sums_sha256="$4"
  base="${RELEASES}/${repo}/releases/download/${tag}"
  archive="${bin}-${tag#v}-${TARGET}.tar.gz"
  dir="${WORK}/${bin}"
  mkdir -p "$dir"

  echo "Installing ${bin} ${tag} (${TARGET})..."
  curl -fsSL "${base}/SHA256SUMS" -o "${dir}/SHA256SUMS"
  [ "$(sha256 "${dir}/SHA256SUMS")" = "$sums_sha256" ] ||
    die "SHA256SUMS for ${repo} ${tag} does not match the pinned digest"

  expected="$(awk -v f="$archive" '$2 == f || $2 == "*" f { print $1 }' "${dir}/SHA256SUMS")"
  [ -n "$expected" ] || die "${repo} ${tag} has no ${archive} (unsupported platform?)"
  curl -fsSL "${base}/${archive}" -o "${dir}/${archive}"
  [ "$(sha256 "${dir}/${archive}")" = "$expected" ] ||
    die "${archive} does not match SHA256SUMS"

  tar -xzf "${dir}/${archive}" -C "$dir" "$bin"
  chmod +x "${dir}/${bin}"
  mv -f "${dir}/${bin}" "${BINDIR}/${bin}"
}

# 1. Install the decdn and decdn-sponsored binaries.
fetch_bin decdn decdn "$DECDN_RELEASE" "$DECDN_SUMS_SHA256"
fetch_bin sponsord decdn-sponsored "$WRAPPER_RELEASE" "$WRAPPER_SUMS_SHA256"

# 2. Write the wrapper's profile. Field names and shape MUST match
# crates/wrapper/src/config.rs's `Profile` struct exactly. Each download
# gets its own throwaway key under data_dir; there is no key to set up here.
cat > "${DECDN_DIR}/sponsor.toml" <<EOF
gateway_base = "${GATEWAY}"
decdn_bin = "${BINDIR}/decdn"
data_dir = "${DECDN_DIR}/sponsored"
rpc_url = "${RPC_URL}"
payment_pool = "${PAYMENT_POOL}"
capacity_bond = "${CAPACITY_BOND}"
chain_id = ${CHAIN_ID}
EOF

if [ "$#" -gt 0 ]; then
  # `exec` skips the EXIT trap, so clean up first.
  rm -rf "$WORK"
  trap - EXIT
  exec "${BINDIR}/decdn-sponsored" "$@"
fi

echo ""
echo "decdn-sponsored is ready. Download with:"
echo "  decdn-sponsored pull b3:<hash> [-o <dir>]"
case ":${PATH}:" in
  *":${BINDIR}:"*) ;;
  *) echo "(add ${BINDIR} to your PATH first)" ;;
esac
