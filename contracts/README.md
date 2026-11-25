# deCDN Contracts

The on-chain surface of deCDN: operator bonding, per-megabyte USDC settlement, served-bytes-weighted governance, and the content blacklist. Built with [Foundry](https://book.getfoundry.sh). Dual-currency by design — **USDC** for payments, **TOKEN** for operator bonds. Vote weight comes from served bytes, not from TOKEN balances ([ADR 036](../adr/036-served-bytes-voting-weight.md)).

This is a standalone Foundry project at the repo root, **excluded from the Cargo workspace**. The ADRs in [`../adr/`](../adr/) are the source of truth for every protocol and economic claim; [ADR 016](../adr/016-contract-interactions.md) is the contract-interaction overview and inventory.

## Contracts

Deployable contracts in [`src/`](src/). `PaymentPool` runs on each payment chain; every other contract runs on the governance chain, Ethereum ([ADR 016 § Deployment Tiers](../adr/016-contract-interactions.md#deployment-tiers)):

| Contract | Purpose | ADR |
|----------|---------|-----|
| `Token` | Fixed-supply (1B) ERC20 with permit, minted once at genesis | [026](../adr/026-tokenomics.md) |
| `CapacityBond` | Operator registry; custodies the TOKEN bond (released in one `unbond` call), supplies the active flag and declared capacity `FeeRouter` credits against | [026](../adr/026-tokenomics.md), [036](../adr/036-served-bytes-voting-weight.md) |
| `FeeRouter` | Permissionless `deposit` of each sweep: rebate / treasury split and the canonical served-bytes accountant for vote weight | [026](../adr/026-tokenomics.md), [036](../adr/036-served-bytes-voting-weight.md) |
| `CctpSweepReceiver` | Stateless CCTP V2 bridge adapter: receives a sweep and deposits each operator's share into `FeeRouter` | [016](../adr/016-contract-interactions.md#bridge-adapters) |
| `PaymentPool` | Payment-chain USDC pool with off-chain vouchers and on-chain redemption; pays `operatorBps` at redemption and sweeps the rest over CCTP, up to 32 operators per transfer. Built on the bridge-neutral `PaymentPoolBase` | [003](../adr/003-payments.md) |
| `OriginAssignment` | Namespace origin sets: a vetted publisher seats and unseats bonded operators as origins for its namespaces | [011](../adr/011-content-takedown.md) |
| `ManualVettingPolicy` | Genesis vetting policy: a `VETTER_ROLE` holder approves which publishers may seat origins | [011](../adr/011-content-takedown.md#vetting-policies) |
| `OpenVettingPolicy` | Vetting policy that vets every publisher; for local and test deployments only | [011](../adr/011-content-takedown.md#vetting-policies) |
| `ContentBlacklist` | Global + regional content-hash blacklist, operator- and origin-level blacklists | [011](../adr/011-content-takedown.md) |
| `PublisherRegistry` | Permissionless namespace creation and two-step namespace ownership transfer | [002](../adr/002-content-addressing.md) |
| `DecdnGovernor` | Served-bytes-weighted on-chain governor with a Timelock executor | [036](../adr/036-served-bytes-voting-weight.md) |
| `Ed25519Verifier` | RFC 8032 PureEdDSA verifier (crypto-lib EIP-6565) with strict canonicalization guards | — |

Libraries:

- `BondMath` — pure arithmetic for the ADR 026 capacity-bond curve.
- `RegionScopeLib` — region string packing and the ADR 030 region-stability scope test,
  inlined into `ContentBlacklist`.
- `DeclaredMbpsHistoryLib` — per-operator `declaredMbps` checkpoint history for
  `CapacityBond`, read back per epoch for the ADR 036 vote-weight cap.

## Layout

```
contracts/
  src/            — contracts (PaymentPoolBase abstract) + 3 libraries
    interfaces/   — frozen external surfaces (I*.sol), including the CCTP V2 calls
  test/           — Foundry suite (*.t.sol + mocks/ + ed25519-vectors/)
  testnet/        — TestnetFaucet and LoopbackCctp (same-chain CCTP for local deploys)
  script/         — DeployProtocol (governance chain), DeployPaymentPool (each
                    payment chain), BaseProtocolDeploy,
                    TransitionToGovernor, TestnetFaucet
  lib/            — submodules: openzeppelin-contracts, forge-std, solady, crypto-lib
```

`TransitionToGovernor` broadcasts nothing. It prints the Timelock calldata for a
governance action to schedule, so it cannot transition anything itself. It ends the [ADR 009](../adr/009-governance.md#bootstrap-multisig-phase)
bootstrap-multisig phase (only relevant if the deploy set `BOOTSTRAP_MULTISIG`):

```bash
GOVERNANCE_TIMELOCK=<timelock> DECDN_GOVERNOR=<governor> BOOTSTRAP_MULTISIG=<multisig> \
  forge script script/TransitionToGovernor.s.sol:TransitionToGovernor --rpc-url "$RPC_URL"
```

It reverts `NotInBootstrapPhase` rather than print a batch if the chain is not
mid-bootstrap. Executing the batch is **one-way** — it strips the multisig's `PROPOSER_ROLE`, so
it can never schedule again, including its own reinstatement. Only the inheriting
DAO could restore it, by vote.

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

For a local chain with the full suite deployed in one command, run [`./dev-deploy.sh`](dev-deploy.sh). See [`../CONTRIBUTING.md#solidity-development`](../CONTRIBUTING.md#solidity-development) for the full workflow (coverage, gas-snapshot updates, install pins) and the [local-deployment guide](../CONTRIBUTING.md#local-deployment-anvil) for what that script does step by step. `--deny warnings` under `FOUNDRY_PROFILE=ci` is stricter than local `forge test` — the common traps are listed under "CI gotchas" in [`../CONTRIBUTING.md#solidity-development`](../CONTRIBUTING.md#solidity-development).

## Static analysis

| Tool | Gate | Config |
|------|------|--------|
| Slither | fail on medium+ | `slither.config.json` |
| Solhint | lints `src/` + `testnet/` + `script/` (tests excluded) | `.solhint.json` |

`.gas-snapshot` is a committed baseline; regenerate it in the same PR when a change legitimately moves gas. See CONTRIBUTING for how to interpret and suppress findings.

## Dependencies

Git submodules under `lib/`:

- **openzeppelin-contracts** (v5.1.0) — AccessControl, ERC20, Governor, Timelock, SafeERC20.
- **forge-std** — Foundry test/script standard library.
- **solady** — `FixedPointMathLib`, used by `BondMath`.
- **crypto-lib** — Smoo.th Ed25519 (EIP-6565) and SHA-512 primitives backing `Ed25519Verifier`.
