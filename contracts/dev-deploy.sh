#!/usr/bin/env bash
# Stand up a complete local dev environment in one command: a fresh Anvil
# chain with the full deCDN protocol suite, a mock USDC, and a funded TOKEN
# faucet deployed against it. Starts Anvil, deploys, then stays in the
# foreground tailing the node — Ctrl-C tears everything down.
#
# Usage:  ./dev-deploy.sh            # default port 8545
#         RPC_PORT=9545 ./dev-deploy.sh
#
# Overridable via env: RPC_PORT, FUNDING_AMOUNT (faucet TOKEN, 18-dec wei).
set -euo pipefail

# Resolve paths relative to this script so it can be run from anywhere.
cd "$(dirname "${BASH_SOURCE[0]}")"

log() { echo "▸ $*" >&2; }
die() { echo "error: $*" >&2; exit 1; }

# --- Prerequisites -----------------------------------------------------------
for tool in anvil forge cast jq; do
  command -v "$tool" >/dev/null 2>&1 || die \
    "'$tool' not found on PATH. Install Foundry via foundryup: https://book.getfoundry.sh/getting-started/installation"
done

# --- Config ------------------------------------------------------------------
RPC_PORT="${RPC_PORT:-8545}"
RPC_URL="http://127.0.0.1:${RPC_PORT}"
CHAIN_ID=31337
MANIFEST="deployments/${CHAIN_ID}.json"

# Funded TOKEN balance for the faucet (1M TOKEN, 18 decimals).
FUNDING_AMOUNT="${FUNDING_AMOUNT:-1000000000000000000000000}"

# Anvil's deterministic dev accounts. LOCAL-ONLY — these private keys are
# public knowledge; never use them on a real chain.
#   #0 = deployer + INITIAL_TOKEN_HOLDER + faucet treasury/admin/gov/pauser.
#        It receives the 1B TOKEN at genesis, so it can fund the faucet, and
#        the faucet script requires TREASURY_ADDRESS == --sender.
ACC0_ADDR="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ACC0_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ACC1_ADDR="0x70997970C51812dc3A010C7d01b50e0d17dc79C8" # EMERGENCY_MULTISIG

# --- Clean stale manifest ----------------------------------------------------
# Anvil starts fresh each run, so a manifest from a prior run is meaningless
# and would trip DeployProtocol's ManifestAlreadyExists guard.
rm -f "$MANIFEST"

# --- Start Anvil -------------------------------------------------------------
# Refuse if something already answers on the port. Otherwise our own anvil
# fails to bind and exits, but the foreign node keeps serving — the readiness
# poll passes, we deploy against a node we don't control, and `wait` returns
# immediately instead of holding the chain open.
if cast block-number --rpc-url "$RPC_URL" >/dev/null 2>&1; then
  die "something is already listening on ${RPC_URL}; stop it or run with RPC_PORT=<other port>"
fi

ANVIL_LOG=$(mktemp)
log "starting anvil on ${RPC_URL} (log: ${ANVIL_LOG})"
# Pin --chain-id so the manifest path (deployments/<block.chainid>.json) is
# guaranteed to match the CHAIN_ID we read back, regardless of anvil's default.
anvil --port "$RPC_PORT" --chain-id "$CHAIN_ID" >"$ANVIL_LOG" 2>&1 &
ANVIL_PID=$!
# Bash runs the EXIT trap on SIGINT/SIGTERM death too, so EXIT alone covers
# Ctrl-C and preserves the 130 exit status.
trap 'kill "$ANVIL_PID" 2>/dev/null || true; rm -f "$ANVIL_LOG"' EXIT

# Wait for the RPC to accept requests (bounded ~15s). Bail early if anvil
# already exited — e.g. the port is in use — so we surface that immediately
# instead of spinning the full timeout against a dead endpoint.
for _ in $(seq 1 30); do
  kill -0 "$ANVIL_PID" 2>/dev/null \
    || die "anvil exited during startup (port ${RPC_PORT} in use?):"$'\n'"$(tail -n 20 "$ANVIL_LOG")"
  if cast block-number --rpc-url "$RPC_URL" >/dev/null 2>&1; then break; fi
  sleep 0.5
done
cast block-number --rpc-url "$RPC_URL" >/dev/null 2>&1 \
  || die "anvil did not become ready on ${RPC_URL}:"$'\n'"$(tail -n 20 "$ANVIL_LOG")"

# --- Deploy mock USDC --------------------------------------------------------
log "deploying MintableUSDC mock"
USDC=$(forge create test/mocks/MintableUSDC.sol:MintableUSDC \
  --rpc-url "$RPC_URL" --private-key "$ACC0_KEY" --broadcast --json \
  | jq -r .deployedTo)
[[ "$USDC" =~ ^0x[0-9a-fA-F]{40}$ ]] || die "failed to deploy MintableUSDC"
log "USDC = $USDC"

# --- Deploy protocol ---------------------------------------------------------
log "deploying protocol suite (DeployProtocol)"
# ADR 019 § Terms Acceptance — DeployProtocol requires a non-zero genesis
# operator-terms hash (CapacityBond rejects the zero sentinel). For local dev
# any non-zero placeholder is fine; production supplies keccak256(TERMS.md).
USDC_ADDRESS="$USDC" \
EMERGENCY_MULTISIG="$ACC1_ADDR" \
INITIAL_TOKEN_HOLDER="$ACC0_ADDR" \
CURRENT_TERMS_HASH="${CURRENT_TERMS_HASH:-0x0000000000000000000000000000000000000000000000000000000000000001}" \
forge script script/DeployProtocol.s.sol:DeployProtocol \
  --rpc-url "$RPC_URL" --sender "$ACC0_ADDR" --private-key "$ACC0_KEY" --broadcast

[[ -f "$MANIFEST" ]] || die "protocol deploy did not write $MANIFEST"
TOKEN=$(jq -r '.contracts.Token // empty' "$MANIFEST")
[[ "$TOKEN" =~ ^0x[0-9a-fA-F]{40}$ ]] \
  || die "manifest $MANIFEST missing/invalid .contracts.Token (got: '${TOKEN:-<empty>}')"

# --- Deploy faucet -----------------------------------------------------------
log "deploying TestnetFaucet (funding ${FUNDING_AMOUNT} TOKEN from account #0)"
TOKEN_ADDRESS="$TOKEN" \
TREASURY_ADDRESS="$ACC0_ADDR" \
FUNDING_AMOUNT="$FUNDING_AMOUNT" \
ADMIN_ADDRESS="$ACC0_ADDR" \
GOVERNANCE_ADDRESS="$ACC0_ADDR" \
PAUSER_ADDRESS="$ACC0_ADDR" \
forge script script/TestnetFaucet.s.sol:DeployTestnetFaucet \
  --rpc-url "$RPC_URL" --sender "$ACC0_ADDR" --private-key "$ACC0_KEY" --broadcast

# --- Summary + keep-alive ----------------------------------------------------
echo
echo "Local deCDN dev environment is up:"
echo "  RPC URL : ${RPC_URL}  (chain id ${CHAIN_ID})"
echo "  Deployer: ${ACC0_ADDR}  (holds 1B TOKEN)"
echo "  USDC    : ${USDC}"
jq -r '.contracts | to_entries[] | "  \(.key): \(.value)"' "$MANIFEST"
echo
echo "Manifest: contracts/${MANIFEST}"
echo "Press Ctrl-C to stop Anvil and tear down."

wait "$ANVIL_PID"
