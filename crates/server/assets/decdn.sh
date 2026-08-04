#!/bin/sh
# decdn sponsor installer - served by sponsord at GET /decdn.sh, with the
# placeholders below substituted server-side (see
# crates/server/src/http/installer.rs) from ServerConfig, so nothing here
# needs an environment variable to run.
set -eu

GATEWAY="{{GATEWAY_BASE}}"
RPC_URL="{{RPC_URL}}"
PAYMENT_CHANNEL="{{PAYMENT_CHANNEL}}"
CAPACITY_BOND="{{CAPACITY_BOND}}"
CHAIN_ID="{{CHAIN_ID}}"

BINDIR="${HOME}/.local/bin"
DECDN_DIR="${HOME}/.decdn"
CLIENT_DIR="${DECDN_DIR}/client"
KEYSTORE="${CLIENT_DIR}/keystore.json"

mkdir -p "$BINDIR" "$CLIENT_DIR"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

# 1. Install the decdn and onramp binaries.
#
# NOTE: release hosting (GET /dl/<bin>-<os>-<arch>) is not wired up on the
# gateway yet — this is the structure the installer will use once it is.
# Until then this step will fail with a 404; that's expected pre-launch.
for bin in decdn onramp; do
  echo "Installing ${bin}..."
  curl -fsSL "${GATEWAY}/dl/${bin}-${OS}-${ARCH}" -o "${BINDIR}/${bin}"
  chmod +x "${BINDIR}/${bin}"
done

# 2. Write the wrapper's profile. Field names and shape MUST match
# crates/wrapper/src/config.rs's `Profile` struct exactly.
cat > "${DECDN_DIR}/sponsor.toml" <<EOF
gateway_base = "${GATEWAY}"
keystore_path = "${KEYSTORE}"
decdn_bin = "${BINDIR}/decdn"
data_dir = "${CLIENT_DIR}"
rpc_url = "${RPC_URL}"
payment_channel = "${PAYMENT_CHANNEL}"
capacity_bond = "${CAPACITY_BOND}"
chain_id = ${CHAIN_ID}
EOF

# 3. Generate a client keystore if one doesn't already exist (idempotent).
if [ ! -f "$KEYSTORE" ]; then
  echo "Generating a new client keystore at ${KEYSTORE}..."
  "${BINDIR}/decdn" key-gen --output-dir "$CLIENT_DIR"
fi

echo ""
echo "decdn sponsor is ready."
echo "Set DECDN_KEYSTORE_PASSWORD to the password you chose for the keystore above,"
echo "then try: onramp <hash> -o out.bin"
