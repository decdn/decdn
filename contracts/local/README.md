# Local internal-testnet rig (anvil + Docker + WireGuard)

A **disposable** rig for standing up the full deCDN contract surface on a VPS so
the team can test internally. anvil runs in Docker (persistent + WireGuard-bound)
and the contracts are deployed with the **production** `DeployProtocol.s.sol` —
no throwaway Solidity, so what you test matches Sepolia/mainnet shape.

> ⚠️ **Not for mainnet or Arbitrum Sepolia.** This uses anvil, well-known dev
> keys, and a test-only mock USDC. It is deliberately isolated under
> `contracts/local/` so the whole thing can be deleted in one move — see
> [Scrap it](#scrap-it). For the real deploy path use `DeployProtocol.s.sol`
> against a real RPC with real env vars (see that script's header).

## What it does

1. Runs `anvil` in Docker with a persistent state volume, bound to your
   WireGuard interface only.
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

- **Docker + Docker Compose** on the VPS.
- **Foundry** (`forge` + `cast`) on the box you run `deploy.sh` from
  (<https://getfoundry.sh>). The deploy needs the contracts source + git
  submodules under `contracts/lib/`, which already live in this repo.
- **WireGuard** between you and the VPS (anvil must never be exposed publicly).
- `jq` (optional, for prettier address output).

## Quick start

```bash
cd contracts/local
cp .env.example .env
# Edit .env: set WG_BIND_IP to the VPS's WireGuard IP (`ip -4 addr show wg0`).

docker compose up -d            # start anvil (persistent, wg-bound)
docker compose ps               # wait for the anvil service to be "healthy"

./deploy.sh                     # deploy mock USDC + full protocol surface
```

`deploy.sh` prints every deployed address and the manifest path on success.

## Security (read this)

anvil's JSON-RPC is **unauthenticated** and exposes the `anvil_*` cheat methods
(`anvil_setBalance`, `anvil_impersonateAccount`, …) plus well-known dev private
keys. Anyone who can reach the port owns the chain. Two rules:

1. **Bind to WireGuard, not `0.0.0.0`.** `WG_BIND_IP` in `.env` pins Docker's
   port publish to the VPN interface. It defaults to `127.0.0.1`. **Never set it
   to `0.0.0.0` on a VPS.**
2. **Don't rely on `ufw`/`firewalld` alone.** Docker publishes ports via its own
   iptables chain and routinely bypasses host firewalls. Binding to the wg IP
   (rule 1) is the actual control.

There's no real value at stake, but an exposed rig invites griefing of your
shared test state.

## Fund test accounts

`MintableUSDC.mint` is permissionless — any wg client can top up. TOKEN is fixed
supply, held by `INITIAL_TOKEN_HOLDER` (the deployer by default); distribute it
with `cast send`.

```bash
RPC=http://<WG_BIND_IP>:8545
USDC=<mock-usdc-from-deploy>                 # printed by deploy.sh
TOKEN=<Token-from-manifest>
DEPLOYER_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

# Mint 1,000 USDC (6 decimals) to an operator/client:
cast send "$USDC" "mint(address,uint256)" <addr> 1000000000 \
  --rpc-url "$RPC" --private-key "$DEPLOYER_KEY"

# Send 100,000 TOKEN (18 decimals) from the genesis holder to an operator
# (needs ≥ MIN_BOND = 50,000 TOKEN to stake):
cast send "$TOKEN" "transfer(address,uint256)" <operator> 100000000000000000000000 \
  --rpc-url "$RPC" --private-key "$DEPLOYER_KEY"

# Gas for a fresh EOA (anvil cheat method):
cast rpc anvil_setBalance <addr> 0xde0b6b3a7640000 --rpc-url "$RPC"
```

## Connect a node

Point a `decdn-node` at the anvil RPC and the deployed addresses. The contract
addresses come from `contracts/deployments/<chainId>.json`; the RPC is your wg
URL. The canonical, working example of wiring a node + client against this exact
anvil deploy (stake → registerNode → open channel → settle) is
`crates/node/tests/anvil_settlement_e2e.rs` — read it as the reference for the
on-chain config a node needs.

> Note: the node's `chain id` is currently bound to Arbitrum Sepolia in
> `load_eth_signer` (see the comment in `crates/node/src/runtime/mod.rs`). Using
> a different `CHAIN_ID` here may require threading the chain id through
> `ResolvedBlockchain` first. Either set `CHAIN_ID` to the value the node
> expects, or make that change before wiring real nodes.

## iroh relay — do we need one?

Short version: **probably not for a first internal test, and it isn't
plug-and-play yet.**

The node builds its iroh endpoint with `presets::N0` (public number0 relays +
DNS discovery) and does **not** currently consume `network.relay_url` (see
`build_endpoint` in `crates/node/src/runtime/mod.rs`; the field is `warn_ignored`
on reload). So:

- **If the VPS has outbound internet:** nodes reach the public n0 relays and
  work with **no relay container at all**. Simplest path to first light.
- **On a flat WireGuard subnet:** nodes can dial each other **directly** once
  their `NodeId` + direct address are known — relays exist for NAT hole-punching
  and NodeId→addr discovery, neither of which a routable wg mesh needs.
- **If you want full isolation (no public n0 dependency):** you'd run a
  self-hosted `iroh-relay` **and** make a small endpoint-builder change to honor
  `relay_url` (+ a discovery mechanism). That code change is out of scope for
  this deploy tooling and tracked as a follow-up.

A commented `relay` service (opt-in `--profile relay`) is stubbed in
`docker-compose.yml` so the scaffolding is ready the moment that endpoint change
lands. Until then it's intentionally inert.

## Persistence & restarts

`--state /state/anvil-state.json` on a named volume persists the chain across
container restarts and VPS reboots (`restart: unless-stopped`). After a restart,
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
rm -f contracts/deployments/<chainId>.json   # if it lingers (gitignored anyway)
```

Then delete `contracts/local/`. Nothing else in the repo references it — no
production Solidity, no CI wiring — so removal is total.
