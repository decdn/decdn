#!/usr/bin/env bash
# Deploy the deCDN contract surface to the local dockerized anvil.
#
# Mirrors the exact, tested recipe from the anvil settlement e2e
# (crates/node/tests/anvil_settlement_e2e.rs): deploy the test-only MintableUSDC
# stand-in, then run the PRODUCTION DeployProtocol.s.sol against it. No
# throwaway Solidity — the on-chain surface is identical to Sepolia/mainnet.
#
# Requires foundry (forge + cast) on PATH: https://getfoundry.sh
# Run anytime after `docker compose up -d` reports anvil healthy.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
contracts_dir="$(cd "$here/.." && pwd)"

# Load config from .env (next to this script), falling back to the defaults
# below. `set -a` exports everything sourced so child `forge` sees it.
if [[ -f "$here/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "$here/.env"
  set +a
fi

# Local-only rig: Docker publishes anvil on 127.0.0.1. An explicit RPC_URL in
# .env still wins.
RPC_PORT="${RPC_PORT:-8545}"
RPC_URL="${RPC_URL:-http://127.0.0.1:${RPC_PORT}}"
CHAIN_ID="${CHAIN_ID:-31337699}"
TIMELOCK_DELAY="${TIMELOCK_DELAY:-3600}"
# Anvil dev account #0 (mnemonic "test test ... junk"), funded at genesis.
# THROWAWAY key — this entire rig is disposable. Never a real key.
DEPLOYER_KEY="${DEPLOYER_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
DEPLOYER_ADDR="${DEPLOYER_ADDR:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
INITIAL_TOKEN_HOLDER="${INITIAL_TOKEN_HOLDER:-$DEPLOYER_ADDR}"

command -v forge >/dev/null 2>&1 || {
  echo "error: foundry (forge) not on PATH — install via https://getfoundry.sh" >&2
  exit 1
}

echo "==> waiting for anvil at $RPC_URL"
for ((i = 1; i <= 30; i++)); do
  if cast chain-id --rpc-url "$RPC_URL" >/dev/null 2>&1; then break; fi
  if [[ "$i" -eq 30 ]]; then
    echo "error: anvil never came up at $RPC_URL (is \`docker compose up -d\` running?)" >&2
    exit 1
  fi
  sleep 1
done

got_chain="$(cast chain-id --rpc-url "$RPC_URL")"
if [[ "$got_chain" != "$CHAIN_ID" ]]; then
  echo "warning: anvil reports chain-id=$got_chain but CHAIN_ID=$CHAIN_ID;" \
       "manifest will be deployments/$got_chain.json" >&2
fi

cd "$contracts_dir"

echo "==> building contracts"
forge build >/dev/null

echo "==> deploying MintableUSDC (test-only 6-decimal USDC stand-in)"
usdc_json="$(forge create test/mocks/MintableUSDC.sol:MintableUSDC \
  --rpc-url "$RPC_URL" --private-key "$DEPLOYER_KEY" --broadcast --json)"
usdc="$(printf '%s' "$usdc_json" | { jq -r '.deployedTo' 2>/dev/null || true; })"
if [[ -z "$usdc" || "$usdc" == "null" ]]; then
  usdc="$(printf '%s' "$usdc_json" | sed -n 's/.*"deployedTo":[[:space:]]*"\([^"]*\)".*/\1/p')"
fi
[[ -n "$usdc" ]] || { echo "error: MintableUSDC deploy produced no address" >&2; exit 1; }
echo "    USDC = $usdc"

echo "==> deploying protocol via production DeployProtocol.s.sol (timelock ${TIMELOCK_DELAY}s)"
USDC_ADDRESS="$usdc" \
INITIAL_TOKEN_HOLDER="$INITIAL_TOKEN_HOLDER" \
EMERGENCY_MULTISIG="$DEPLOYER_ADDR" \
CHALLENGER_INCENTIVE_POOL="$DEPLOYER_ADDR" \
TIMELOCK_DELAY="$TIMELOCK_DELAY" \
FORCE_OVERWRITE_MANIFEST=true \
  forge script script/DeployProtocol.s.sol:DeployProtocol \
    --rpc-url "$RPC_URL" --broadcast \
    --private-key "$DEPLOYER_KEY" --sender "$DEPLOYER_ADDR"

manifest="deployments/${got_chain}.json"
echo
echo "==> deploy complete."
echo "    USDC (mock)   = $usdc"
echo "    manifest      = contracts/$manifest"
if command -v jq >/dev/null 2>&1 && [[ -f "$manifest" ]]; then
  echo "    addresses:"
  jq -r '.contracts | to_entries[] | "      \(.key) = \(.value)"' "$manifest"
fi
echo
echo "Next: mint test USDC/TOKEN to your accounts and point a node at the RPC —"
echo "see contracts/local/README.md § Fund test accounts and § Connect a node."
