#!/bin/sh
# decdn-sponsored installer - served by sponsord at GET /decdn.sh, with the
# placeholders below substituted server-side (see
# crates/server/src/http/installer.rs) from ServerConfig, so nothing here
# needs an environment variable to run.
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

BINDIR="${HOME}/.local/bin"
DECDN_DIR="${HOME}/.decdn"

mkdir -p "$BINDIR" "$DECDN_DIR"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

# 1. Install the decdn and decdn-sponsored binaries.
#
# NOTE: release hosting (GET /dl/<bin>-<os>-<arch>) is not wired up on the
# gateway yet — this is the structure the installer will use once it is.
# Until then this step will fail with a 404; that's expected pre-launch.
for bin in decdn decdn-sponsored; do
  echo "Installing ${bin}..."
  curl -fsSL "${GATEWAY}/dl/${bin}-${OS}-${ARCH}" -o "${BINDIR}/${bin}"
  chmod +x "${BINDIR}/${bin}"
done

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
  exec "${BINDIR}/decdn-sponsored" "$@"
fi

echo ""
echo "decdn-sponsored is ready. Download with:"
echo "  decdn-sponsored pull b3:<hash> [-o <dir>]"
case ":${PATH}:" in
  *":${BINDIR}:"*) ;;
  *) echo "(add ${BINDIR} to your PATH first)" ;;
esac
