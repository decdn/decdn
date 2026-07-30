# deCDN Contracts

The on-chain surface of deCDN: operator bonding, per-megabyte USDC settlement, served-bytes-weighted governance, and the slashing/appeals machinery. Built with [Foundry](https://book.getfoundry.sh). Dual-currency by design — **USDC** for payments, **TOKEN** for staking and governance.

This is a standalone Foundry project at the repo root, **excluded from the Cargo workspace**. The ADRs in [`../adr/`](../adr/) are the source of truth for every protocol and economic claim; [ADR 016](../adr/016-contract-interactions.md) is the contract-interaction overview and inventory.

## Contracts

Deployable contracts in [`src/`](src/):

| Contract | Purpose | ADR |
|----------|---------|-----|
| `Token` | Fixed-supply (1B) ERC20 with permit + burn; no mint post-genesis | [026](../adr/026-tokenomics.md) |
| `CapacityBond` | Operator registry; custodies the TOKEN bond, executes escrow-on-slash, reports settlements to `FeeRouter` | [026](../adr/026-tokenomics.md), [036](../adr/036-served-bytes-voting-weight.md) |
| `FeeRouter` | 60/30/10 settlement split (operator / buyback / treasury) and the canonical served-bytes accountant for vote weight | [026](../adr/026-tokenomics.md), [036](../adr/036-served-bytes-voting-weight.md) |
| `PaymentChannel` | Unidirectional off-chain USDC channels with on-chain settlement; forwards fees to `FeeRouter` rather than skimming inline | [003](../adr/003-payments.md) |
| `SlashJudge` | On-chain adjudicator for signature-dependent slashable offenses; verifies the EIP-712 slash signature | [028](../adr/028-slashing-appeals.md) |
| `SlashAppeal` | Slash-appeal state machine (open → ratify/reverse) holding a per-appeal TOKEN bond | [028](../adr/028-slashing-appeals.md) |
| `OriginAssignment` | The DAO's positive origin authority — namespace-based origin allow-lists | [011](../adr/011-content-takedown.md) |
| `BuybackBurner` | Swaps the 30% USDC buyback bucket for TOKEN and burns the proceeds; venue-neutral (concrete Balancer V3 or Uniswap V3 subclass, selected at deploy) | [018](../adr/018-liquidity-strategy.md), [026](../adr/026-tokenomics.md) |
| `ContentBlacklist` | Global + regional content-hash blacklist, operator- and origin-level blacklists | [011](../adr/011-content-takedown.md) |
| `PublisherRegistry` | Permissionless namespace creation and append-only content claims | [002](../adr/002-content-addressing.md) |
| `DecdnGovernor` | Served-bytes-weighted on-chain governor with a Timelock executor | [036](../adr/036-served-bytes-voting-weight.md) |
| `Ed25519Verifier` | RFC 8032 PureEdDSA verifier (crypto-lib EIP-6565) with strict canonicalization guards | — |

Libraries:

- `BondMath` — pure arithmetic for the ADR 026 capacity-bond curve and tier-based slash reduction.
- `SlashEscrowLib` — shared slash-escrow lifecycle types (status enum + record struct).

## Layout

```
contracts/
  src/            — 12 contracts + BondMath/SlashEscrowLib libraries
    interfaces/   — 14 frozen external surfaces (I*.sol)
  test/           — Foundry suite (19 *.t.sol + mocks/ + ed25519-vectors/)
  script/         — DeployProtocol, BaseProtocolDeploy, ActivateBuyback,
                    TransitionToGovernor, TestnetFaucet
    lib/          — BuybackVenueLib (shared venue dispatch + burner construction)
  lib/            — submodules: openzeppelin-contracts, forge-std, solady, crypto-lib
```

`ActivateBuyback` and `TransitionToGovernor` broadcast nothing — they print the
Timelock calldata for a governance action to schedule. `TransitionToGovernor`
ends the [ADR 009](../adr/009-governance.md#bootstrap-multisig-phase)
bootstrap-multisig phase (only relevant if the deploy set `BOOTSTRAP_MULTISIG`):

```bash
GOVERNANCE_TIMELOCK=<timelock> DECDN_GOVERNOR=<governor> BOOTSTRAP_MULTISIG=<multisig> \
  forge script script/TransitionToGovernor.s.sol:TransitionToGovernor --rpc-url "$RPC_URL"
```

It reverts `NotInBootstrapPhase` rather than print a batch if the chain is not
mid-bootstrap. Executing the batch is **one-way** — it strips the multisig's
`PROPOSER_ROLE`, so it can never schedule again.

Compiled with solc `0.8.28`, EVM `cancun`, optimizer at 200 runs. CI pins Foundry to `v1.7.1`.

## Build & test

Run from `contracts/`:

```bash
forge fmt --check                       # formatting check (CI gate)
forge build --sizes --deny warnings     # build + report sizes; fail on warnings (CI gate)
forge test                              # default profile: 256 fuzz, 32-depth invariants
FOUNDRY_PROFILE=ci forge test           # 1024 fuzz, 256/50-depth (matches CI)
FOUNDRY_PROFILE=fuzz forge test         # 10k fuzz, 1024/100-depth (nightly/manual)
forge snapshot --diff .gas-snapshot     # current gas vs committed baseline
```

For a local chain with the full suite deployed in one command, run [`./dev-deploy.sh`](dev-deploy.sh). See [`../CONTRIBUTING.md#solidity-development`](../CONTRIBUTING.md#solidity-development) for the full workflow (coverage, gas-snapshot updates, install pins) and the [local-deployment guide](../CONTRIBUTING.md#local-deployment-anvil) for what that script does step by step. `--deny warnings` under `FOUNDRY_PROFILE=ci` is stricter than local `forge test` — the common traps are documented in [`../CLAUDE.md`](../CLAUDE.md) under "Solidity CI gotchas".

## Static analysis

| Tool | Gate | Config |
|------|------|--------|
| Slither | fail on medium+ | `slither.config.json` |
| Aderyn | fail on high | `aderyn.toml` |
| Solhint | lints `src/` + `testnet/` + `script/` (tests excluded) | `.solhint.json` |

`.gas-snapshot` is a committed baseline; regenerate it in the same PR when a change legitimately moves gas. See CONTRIBUTING for how to interpret and suppress findings.

## Dependencies

Git submodules under `lib/`:

- **openzeppelin-contracts** (v5.1.0) — AccessControl, ERC20, Governor, Timelock, SafeERC20.
- **forge-std** — Foundry test/script standard library.
- **solady** — `FixedPointMathLib`, used by `BondMath`.
- **crypto-lib** — Smoo.th Ed25519 (EIP-6565) and SHA-512 primitives backing `Ed25519Verifier`.
