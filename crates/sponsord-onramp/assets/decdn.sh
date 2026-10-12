#!/bin/sh
# decdn installer for macOS/Linux - served by sponsord-onramp at GET /decdn.sh,
# with the placeholders below substituted server-side (see
# crates/sponsord-onramp/src/installer.rs), so nothing here needs an
# environment variable to run. The Windows twin is assets/decdn.ps1; the two
# keep the same state.
#
# It installs decdn and, given `pull`, downloads a bundle paid for by this
# onramp's sponsor:
#   curl -fsSL <onramp>/decdn.sh | sh -s -- pull b3:<hash> [-o <dir>] [--namespace <id>]
#
# Each download gets a throwaway key under ~/.decdn/sponsored/downloads/<hash>/
# and a capability for it, issued once the person passes the gate in the
# browser. A failed download keeps both, so re-running the same command
# resumes without the gate; a finished one deletes them.
#
# Everything runs from `main` on the last line, so a script cut short in
# transit runs nothing.
set -eu

ONRAMP_URL="{{ONRAMP_URL}}"

# decdn comes from a pinned GitHub Release, pinned by tag and by the SHA-256
# of its SHA256SUMS file; the version names its archives.
RELEASES="{{RELEASES_BASE}}"
DECDN_RELEASE="{{DECDN_RELEASE}}"
DECDN_VERSION="{{DECDN_VERSION}}"
DECDN_SUMS_SHA256="{{DECDN_SUMS_SHA256}}"

# The chain and contracts a download runs against. SLASH_JUDGE is empty when
# the onramp names none.
CHAIN_ID="{{CHAIN_ID}}"
RPC_URL="{{RPC_URL}}"
PAYMENT_POOL="{{PAYMENT_POOL}}"
CAPACITY_BOND="{{CAPACITY_BOND}}"
SLASH_JUDGE="{{SLASH_JUDGE}}"

BINDIR="${HOME}/.local/bin"
DECDN_DIR="${HOME}/.decdn"
MARKERS="${DECDN_DIR}/installer"
SPONSORED="${DECDN_DIR}/sponsored"
DECDN="${BINDIR}/decdn"

# How often, and for how long, to ask the onramp for the capability while the
# person passes the gate.
POLL_EVERY=2
POLL_TIMEOUT=600

# A saved capability this close to expiry is replaced, together with its key
# (a key's terms are frozen on-chain at its first redemption): a default node
# refuses vouchers 420 s before expiry, and the download needs an hour on top.
EXPIRY_MARGIN=4020

# Held while this run lasts; removed on exit.
WORK=""
LOCKS=""

die() {
  echo "decdn: $*" >&2
  exit 1
}

usage() {
  echo "usage: sh decdn.sh [pull <hash> [-o <dir>] [--namespace <id>]]" >&2
  exit 2
}

cleanup() {
  [ -z "$WORK" ] || rm -rf "$WORK"
  for lock in $LOCKS; do
    rm -rf "$lock"
  done
}

# lock <dir>: take a lock by creating <dir> with this shell's PID inside.
# Fails when a live process holds it; a lock left by a dead one is taken over.
lock() {
  if ! mkdir "$1" 2>/dev/null; then
    pid="$(cat "$1/pid" 2>/dev/null || true)"
    case "$pid" in
      '' | *[!0-9]*) ;;
      *) ! kill -0 "$pid" 2>/dev/null || return 1 ;;
    esac
    rm -rf "$1"
    mkdir "$1" 2>/dev/null || return 1
  fi
  echo "$$" >"$1/pid"
  LOCKS="${LOCKS} $1"
}

# normalize_hash <raw>: a BLAKE3 hash as the website prints it (`b3:<hex>`,
# `0x<hex>` or bare hex) as 64 lowercase hex characters.
normalize_hash() {
  h="$1"
  case "$h" in
    b3:*) h="${h#b3:}" ;;
    0x*) h="${h#0x}" ;;
  esac
  h="$(printf '%s' "$h" | tr 'A-F' 'a-f')"
  case "$h" in
    '' | *[!0-9a-f]*) return 1 ;;
  esac
  [ "${#h}" -eq 64 ] || return 1
  printf '%s' "$h"
}

detect_target() {
  case "$(uname -s)" in
    Linux) os_triple=unknown-linux-gnu ;;
    Darwin) os_triple=apple-darwin ;;
    *) die "unsupported OS: $(uname -s) (on Windows, use decdn.ps1 in PowerShell)" ;;
  esac
  case "$(uname -m)" in
    x86_64 | amd64) arch=x86_64 ;;
    arm64 | aarch64) arch=aarch64 ;;
    *) die "unsupported architecture: $(uname -m)" ;;
  esac
  TARGET="${arch}-${os_triple}"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256() { sha256sum "$1" | cut -d' ' -f1; }
  elif command -v shasum >/dev/null 2>&1; then
    sha256() { shasum -a 256 "$1" | cut -d' ' -f1; }
  else
    die "need sha256sum or shasum to verify downloads"
  fi
}

# fetch_bin <repo> <binary> <release tag> <version> <SHA256SUMS digest>
#
# Downloads SHA256SUMS and checks it against the pinned digest, then
# downloads the archive for this platform and checks it against SHA256SUMS.
# Nothing lands in BINDIR unless both checks pass.
#
# A marker in MARKERS records the pinned digest and target of the last
# install, so a binary already installed from the same release is kept and
# nothing is downloaded. Deleting the marker forces a reinstall.
fetch_bin() {
  repo="$1" bin="$2" tag="$3" version="$4" sums_sha256="$5"
  base="${RELEASES}/${repo}/releases/download/${tag}"
  archive="${bin}-${version}-${TARGET}.tar.gz"
  dir="${WORK}/${bin}"
  marker="${MARKERS}/${bin}"
  stamp="${sums_sha256} ${TARGET}"

  if [ -x "${BINDIR}/${bin}" ] && [ -f "$marker" ] && [ "$(cat "$marker")" = "$stamp" ]; then
    echo "${bin} ${tag} is already installed."
    return 0
  fi
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
  echo "$stamp" >"$marker"
}

# open_browser <url>: best effort, and only with a person at the terminal.
# The link is printed too, so a failure here is silent.
open_browser() {
  [ -t 1 ] || return 0
  if [ "$(uname -s)" = Darwin ]; then
    open "$1" >/dev/null 2>&1 </dev/null &
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$1" >/dev/null 2>&1 </dev/null &
  fi
  return 0
}

# ensure_key: ADDRESS of this download's key, generating the key and its
# random password on first use. A key missing its password or address is
# replaced, together with the capability bound to it.
ensure_key() {
  if [ -f "${STATE}/keystore.json" ] && [ -s "${STATE}/password" ] && [ -s "${STATE}/address" ]; then
    ADDRESS="$(cat "${STATE}/address")"
    return 0
  fi
  rm -f "${STATE}/keystore.json" "${STATE}/node.secret" "${STATE}/address" \
    "${STATE}/capability" "${STATE}/expiry"
  od -An -tx1 -N32 /dev/urandom | tr -d ' \n' >"${STATE}/password"
  [ -s "${STATE}/password" ] || die "could not read /dev/urandom"
  out="$("$DECDN" key-gen --output-dir "$STATE" \
    --keystore-password-file "${STATE}/password" </dev/null)" ||
    die "decdn key-gen failed"
  ADDRESS="$(printf '%s\n' "$out" | sed -n 's/^eth address: *//p')"
  hex="${ADDRESS#0x}"
  case "$hex" in
    "$ADDRESS" | '' | *[!0-9a-fA-F]*) die "decdn key-gen printed no address" ;;
  esac
  [ "${#hex}" -eq 40 ] || die "decdn key-gen printed no address"
  printf '%s' "$ADDRESS" >"${STATE}/address"
}

# get_capability: ask the onramp for the capability issued to ADDRESS. Sets
# TOKEN and EXPIRY and returns 0 when there is one, returns 1 while there is
# none yet.
get_capability() {
  body="${WORK}/capability.json"
  code="$(curl -sS -o "$body" -w '%{http_code}' \
    "${ONRAMP_URL}/v1/capability?client=${ADDRESS}")" ||
    die "could not reach ${ONRAMP_URL}"
  case "$code" in
    200) ;;
    204) return 1 ;;
    *) die "${ONRAMP_URL}/v1/capability answered ${code}: $(cat "$body" 2>/dev/null || true)" ;;
  esac
  json="$(tr -d '\n' <"$body")"
  TOKEN="$(printf '%s' "$json" | sed -n 's/.*"token" *: *"\(dcap1:[A-Za-z0-9_-]*\)".*/\1/p')"
  EXPIRY="$(printf '%s' "$json" | sed -n 's/.*"expiry" *: *\([0-9][0-9]*\).*/\1/p')"
  [ -n "$TOKEN" ] && [ -n "$EXPIRY" ] || die "unexpected capability response: ${json}"
}

# ensure_capability: TOKEN for this download's key. A saved one is reused;
# otherwise the onramp's, after the person passes the gate if it has none.
ensure_capability() {
  if [ -s "${STATE}/capability" ]; then
    TOKEN="$(cat "${STATE}/capability")"
    return 0
  fi
  if ! get_capability; then
    url="${ONRAMP_URL}/fund?client=${ADDRESS}"
    echo "Open this link to start the download:"
    echo "  ${url}"
    open_browser "$url"
    deadline=$(($(date +%s) + POLL_TIMEOUT))
    until get_capability; do
      [ "$(date +%s)" -lt "$deadline" ] ||
        die "timed out after ${POLL_TIMEOUT} s waiting for the check in the browser"
      sleep "$POLL_EVERY"
    done
  fi
  printf '%s' "$EXPIRY" >"${STATE}/expiry"
  printf '%s' "$TOKEN" >"${STATE}/capability"
}

# pull <hash> <output> <namespace>
pull() {
  hash="$1" output="$2" namespace="$3"
  # Key material is private; the downloaded files keep the user's umask.
  user_umask="$(umask)"
  umask 077
  mkdir -p "${SPONSORED}/downloads"
  chmod 700 "$SPONSORED" "${SPONSORED}/downloads"

  # One run per download at a time, so two runs never make two keys.
  lock "${SPONSORED}/downloads/${hash}.lock" ||
    die "this download is already running in another terminal"
  STATE="${SPONSORED}/downloads/${hash}"
  mkdir -p "$STATE"
  chmod 700 "$STATE"

  expiry="$(cat "${STATE}/expiry" 2>/dev/null || true)"
  case "$expiry" in
    '' | *[!0-9]*) expiry=0 ;;
  esac
  if [ -s "${STATE}/capability" ] && [ "$expiry" -le $(($(date +%s) + EXPIRY_MARGIN)) ]; then
    rm -rf "$STATE"
    mkdir -p "$STATE"
  fi

  # decdn reads the password from this before the password file.
  unset DECDN_KEYSTORE_PASSWORD
  ensure_key
  ensure_capability

  # decdn's data dir holds its peer store and buyer-channel store, shared by
  # downloads so peers one finds serve the next. decdn locks it for a whole
  # pull, so a download started while another holds it uses its own.
  data_dir="${SPONSORED}/decdn"
  if lock "${SPONSORED}/decdn.lock"; then
    mkdir -p "$data_dir"
  else
    echo "decdn: another download is running; this one starts without the shared peer cache" >&2
    data_dir="$STATE"
  fi

  set -- bundle pull --hash "$hash" -o "$output" \
    --capability "$TOKEN" \
    --keystore "${STATE}/keystore.json" \
    --keystore-password-file "${STATE}/password" \
    --data-dir "$data_dir" \
    --rpc-url "$RPC_URL" \
    --payment-pool-address "$PAYMENT_POOL" \
    --capacity-bond-address "$CAPACITY_BOND" \
    --chain-id "$CHAIN_ID"
  [ -z "$namespace" ] || set -- "$@" --namespace "$namespace"
  [ -z "$SLASH_JUDGE" ] || set -- "$@" --slash-judge-address "$SLASH_JUDGE"

  umask "$user_umask"
  status=0
  "$DECDN" "$@" </dev/null || status=$?
  if [ "$status" -ne 0 ]; then
    echo "decdn: download did not finish. Re-run the same command to resume:" \
      "downloaded bytes are kept, and the allowance is reused while it is valid." >&2
    exit "$status"
  fi
  rm -rf "$STATE"
}

main() {
  hash="" output="." namespace=""
  if [ "$#" -gt 0 ]; then
    [ "$1" = pull ] || usage
    shift
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -o | --output)
          [ "$#" -ge 2 ] || usage
          output="$2"
          shift 2
          ;;
        --namespace)
          [ "$#" -ge 2 ] || usage
          namespace="$2"
          shift 2
          ;;
        -*) usage ;;
        *)
          [ -z "$hash" ] || usage
          hash="$(normalize_hash "$1")" ||
            die "not a BLAKE3 hash (expected 64 hex characters, optionally b3:-prefixed): $1"
          shift
          ;;
      esac
    done
    [ -n "$hash" ] || usage
    # Namespace 0 means "no namespace" to decdn, so it is refused here.
    case "$namespace" in
      '') ;;
      *[!0-9]*) die "--namespace must be a positive integer: ${namespace}" ;;
      *[!0]*) ;;
      *) die "--namespace must be a positive integer: ${namespace}" ;;
    esac
  fi

  detect_target
  WORK="$(mktemp -d)"
  trap cleanup EXIT
  trap 'exit 130' INT TERM
  mkdir -p "$BINDIR" "$DECDN_DIR" "$MARKERS"

  fetch_bin decdn decdn "$DECDN_RELEASE" "$DECDN_VERSION" "$DECDN_SUMS_SHA256"

  if [ -n "$hash" ]; then
    pull "$hash" "$output" "$namespace"
    return 0
  fi

  echo ""
  echo "decdn is ready. Download a bundle with:"
  echo "  curl -fsSL ${ONRAMP_URL}/decdn.sh | sh -s -- pull b3:<hash> [-o <dir>] [--namespace <id>]"
  case ":${PATH}:" in
    *":${BINDIR}:"*) ;;
    *) echo "(add ${BINDIR} to your PATH to run decdn directly)" ;;
  esac
}

main "$@"
