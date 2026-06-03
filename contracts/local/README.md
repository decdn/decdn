# Local internal-testnet rig (anvil + Docker)

A **disposable, local-only** rig for standing up the full deCDN contract surface
on your machine. anvil runs in Docker (persistent, published to `127.0.0.1`) and
the contracts are deployed with the **production** `DeployProtocol.s.sol` — no
throwaway Solidity, so what you test matches Sepolia/mainnet shape.

> This rig is intentionally local-only. Standing up a **shared** test RPC for the
> team (and hardening/exposing it) is the devops test-RPC setup's job, not this
> rig's.

> ⚠️ **Not for mainnet or Arbitrum Sepolia.** This uses anvil, well-known dev
> keys, and a test-only mock USDC. It is deliberately isolated under
> `contracts/local/` so the whole thing can be deleted in one move — see
> [Scrap it](#scrap-it). For the real deploy path use `DeployProtocol.s.sol`
> against a real RPC with real env vars (see that script's header).

## What it does

1. Runs `anvil` in Docker with a persistent state volume, published to
   `127.0.0.1` only.
2. `deploy.sh` deploys the test-only `MintableUSDC` (6-decimal USDC stand-in),
   then runs the production `DeployProtocol.s.sol` against it — deploying the
   real `Token`, `CapacityBond`, `FeeRouter`, `PaymentChannel`, `SlashJudge`,
   `SlashAppeal`, `ContentBlacklist`, `PublisherRegistry`, `OriginAssignment`,
   `DecdnGovernor`, `TimelockController`, and the real `Ed25519Verifier`.
3. Addresses land in `contracts/deployments/<chainId>.json` (gitignored).

This is the same recipe the `anvil-e2e` integration test uses
(`crates/node/tests/anvil_settlement_e2e.rs`), so it stays in lockstep with how
the node actually talks to the chain.

## Prerequisites

- **Docker + Docker Compose.**
- **Foundry** (`forge` + `cast`) on the box you run `deploy.sh` from
  (<https://getfoundry.sh>). The deploy needs the contracts source + git
  submodules under `contracts/lib/`, which already live in this repo.
- `jq` (optional, for prettier address output).

## Quick start

```bash
cd contracts/local
cp .env.example .env            # defaults are fine for a local run

docker compose up -d            # start anvil (persistent, 127.0.0.1 only)
docker compose ps               # wait for the anvil service to be "healthy"

./deploy.sh                     # deploy mock USDC + full protocol surface
```

`deploy.sh` prints every deployed address and the manifest path on success.

## A note on safety

anvil's JSON-RPC is **unauthenticated** and exposes the `anvil_*` cheat methods
(`anvil_setBalance`, `anvil_impersonateAccount`, …) plus well-known dev private
keys — anyone who can reach the port owns the chain. That's why this rig
publishes the port to `127.0.0.1` only. **Don't re-point it at a public
interface.** If the team needs a shared test RPC, that belongs in the devops
test-RPC setup, which owns exposure and hardening. (Note: Docker publishes ports
via its own iptables chain and bypasses `ufw`/`firewalld`, so a bind address is
the real control — another reason to leave it on loopback here.)

## Fund test accounts

`MintableUSDC.mint` is permissionless — anyone with RPC access can top up. TOKEN
is fixed supply, held by `INITIAL_TOKEN_HOLDER` (the deployer by default);
distribute it with `cast send`.

```bash
RPC=http://127.0.0.1:8545     # http://127.0.0.1:$RPC_PORT
USDC=0x...                    # mock USDC, printed by deploy.sh
TOKEN=0x...                   # Token address from the manifest
DEPLOYER_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

# Mint 1,000 USDC (6 decimals) to an operator/client:
cast send "$USDC" "mint(address,uint256)" YOUR_ADDRESS 1000000000 \
  --rpc-url "$RPC" --private-key "$DEPLOYER_KEY"

# Send 100,000 TOKEN (18 decimals) from the genesis holder to an operator
# (needs ≥ MIN_BOND = 50,000 TOKEN to stake):
cast send "$TOKEN" "transfer(address,uint256)" OPERATOR_ADDRESS 100000000000000000000000 \
  --rpc-url "$RPC" --private-key "$DEPLOYER_KEY"

# Gas for a fresh EOA (anvil cheat method):
cast rpc anvil_setBalance YOUR_ADDRESS 0xde0b6b3a7640000 --rpc-url "$RPC"
```

## Connect a node

Point a `decdn-node` at the anvil RPC and the deployed addresses. The contract
addresses come from `contracts/deployments/<chainId>.json`; the RPC is the local
URL (`http://127.0.0.1:$RPC_PORT`). The canonical, working example of wiring a
node + client against this exact anvil deploy (stake → registerNode → open
channel → settle) is `crates/node/tests/anvil_settlement_e2e.rs` — read it as the
reference for the on-chain config a node needs.

In the node config, set `[blockchain] chain_id` to match this rig's `CHAIN_ID`
(it defaults to Arbitrum Sepolia otherwise — the node binds this chain id on its
signer and `slash_sig` EIP-712 domain) and `[blockchain] rpc_url` to the local
RPC. To route iroh traffic through the self-hosted relay, set `[network]
relay_url` (env `DECDN_RELAY_URL`); see § iroh relay.

## iroh relay

The node honors `[network] relay_url` (env `DECDN_RELAY_URL`): when set, it
replaces the public n0 relay map with your self-hosted relay
(`RelayMode::Custom`) while keeping n0 DNS address-lookup for NodeId→address
discovery. On bring-up the node runs a quick TCP reachability probe (a few
retries with backoff) and logs a **warning** if the relay is unreachable, but
**starts anyway** — iroh keeps retrying the relay in the background, so a
slow-starting or transiently-down relay won't block node startup. The probe is a
diagnostic to catch a down/mistyped URL early, not a hard gate.

Do you need it?

- **Host has outbound internet:** skip the relay entirely — nodes use the public
  n0 relays. Simplest path to first light.
- **Same host / loopback:** nodes can often dial each other directly once their
  `NodeId` + direct address are known.
- **Isolated / self-hosted:** run the bundled relay and point nodes at it via
  `relay_url`.

Start the relay (opt-in profile) and wire nodes to it:

```bash
# Set IROH_RELAY_IMAGE in .env to a relay image/build first, then:
docker compose --profile relay up -d
# Node config: [network] relay_url = "http://127.0.0.1:3340"
```

NodeId discovery still uses n0 DNS (pkarr), so full isolation from public n0
infrastructure (custom discovery) and multi-relay failover are tracked as
follow-ups — see issue #795.

## Persistence & restarts

`--state /state/anvil-state.json` on a named volume persists the chain across
container restarts and host reboots (`restart: unless-stopped`). After a restart,
the deployed addresses are still valid — no redeploy needed. To start clean,
`docker compose down -v` wipes the volume; then redeploy.

## Time control

Governance (`TimelockController`), unbonding, dispute windows, and FeeRouter
epochs are time-based. `BLOCK_TIME=2` advances wall-clock with each block, but
for long waits (the 1h+ timelock, 14d unbonding, 7d epochs) fast-forward:

```bash
cast rpc evm_increaseTime 3600 --rpc-url "$RPC"   # +1h
cast rpc evm_mine --rpc-url "$RPC"                 # mine so the bump takes effect
```

## Scrap it

```bash
cd contracts/local
docker compose down -v          # stop anvil + delete the state volume
rm -f ../deployments/YOUR_CHAIN_ID.json   # if it lingers (gitignored anyway)
```

Then delete `contracts/local/`. Nothing else in the repo references it — no
production Solidity, no CI wiring — so removal is total.
