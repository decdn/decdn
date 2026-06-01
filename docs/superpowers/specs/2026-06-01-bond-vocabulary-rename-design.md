# Design: Unify operator-collateral vocabulary on "bond" across the contract surface

**Date:** 2026-06-01
**Status:** Approved (pending spec review)
**Scope:** `contracts/src/CapacityBond.sol`, `contracts/src/StakeMath.sol`, their tests, the `incentive` crate's `sol!` mirror, and residual "stake" wording in ADRs 026/028/036.

## Problem

The operator-collateral concept is **half-migrated** and internally contradictory. The contract was renamed `StakingRegistry → CapacityBond` (ADR 026 v2.2) and the ADRs standardized on "bond" as the design term, but the live ABI still exposes `stake()` / `unstake()` actions while the *withdrawal* half already uses bond vocabulary (`unbondingOf`, `unbondingPeriod`, `UnbondingRequested`). An operator therefore **stakes** but **unbonds** the same single collateral pool — the worst outcome, reading like two concepts when it is one.

This is not a semantic change to mechanics. It is a surface vocabulary realignment to match the ADRs (the workspace source of truth).

## Why now

- **Zero bytecode impact.** Identifier renames don't change contract size — relevant given `CapacityBond` sits near the EIP-170 ceiling. Only event/error *selectors* change (ABI-level, not size).
- **Pre-mainnet, no pinned integrators.** Testnet deployment; no external callers are pinned to `Staked`/`stake()` selectors yet. Post-launch this becomes a breaking-ABI migration.
- The Rust `incentive` binding is a hand-maintained `alloy::sol!` mirror (no codegen step), so it is updated in lockstep in the same PR.

## Rename map — `CapacityBond.sol` (public surface)

| Now | After |
|---|---|
| `stake(uint256 amount)` | `bond(uint256 amount)` |
| `requestUnstake(uint256 amount)` | `requestUnbond(uint256 amount)` |
| `unstake()` | `unbond()` |
| `stakeOf(address)` view | `bondOf(address)` |
| `getStakeMultiple(address)` view | `getBondMultiple(address)` |
| `setMinStake(uint256)` | `setMinBond(uint256)` |
| `activeStake` mapping (public state) | `activeBond` |
| `minStake` (public state) | `minBond` |
| event `Staked(operator, amount, newActiveStake)` | `Bonded(operator, amount, newActiveBond)` |
| event `Unstaked(operator, amount)` | `Unbonded(operator, amount)` |
| event `MinStakeUpdated(old, new)` | `MinBondUpdated(old, new)` |
| event param `remainingStake` (in `AutoEjected`, `NodeAutoEjected`) | `remainingBond` |
| error `InsufficientStake(requested, available)` | `InsufficientBond(requested, available)` |
| error `StakeBelowMinimum(stake, required)` | `BondBelowMinimum(bond, required)` |
| internal `_reduceStakeAtTier` | `_reduceBondAtTier` |
| internal `_enforceMinStakeBounds` | `_enforceMinBondBounds` |
| ctor param `minStake_`, locals (`oldMinStake`, `newMinStake`, `stakeSlash`, `stakePortion`) | `minBond_`, `oldMinBond`, `newMinBond`, `bondSlash`, `bondPortion` |
| NatSpec / comments using "stake"/"staking" | "bond"/"bonding" |

## `StakeMath.sol → BondMath.sol`

- Rename file, library, and the `import` in `CapacityBond.sol`.
- `reduceAtTier(uint256 active, uint256 unbonding, uint256 tierBps)` — signature and param names **unchanged** (already generic / bond-vocab). Only the library name changes.
- Rename the corresponding test file (`contracts/test/CapacityBond.t.sol` references it via `CapacityBond`; check for any direct `StakeMath` import in tests).

## Already correct — left untouched

`unbondingOf`, `unbondingPeriod`, `UnbondingRequested`, `UnbondingPeriodUpdated`, `firstBondedAt`, `_firstBondedAt` — already bond vocabulary.

## Explicitly out of scope

- **`SlashAppeal` / `SlashJudge` / `ContentBlacklist` "appeal bond" / "challenge bond"** — distinct deposits, already correctly named. No change.
- **`StakerSet` / `AllStaked` in `crates/node/tests/dht_loopback.rs`** — a node-membership/staker-filter test abstraction in the DHT layer, not the `CapacityBond` ABI. Renaming the node-side "staker" vocabulary is a separate concern; not touched here.
- No change to mechanics, bounds, time-locks, slash arithmetic, or storage layout (storage slot *names* change; slot *order/types* do not).

## Rust `incentive` crate

`crates/incentive/src/capacity_bond.rs` is an inline `alloy::sol!` mirror of the Solidity surface. Apply the same rename map to the macro body (function/event/error/state names) and to the doc-comments referencing `stake`/`minStake`/`activeStake`. Update any callers of the renamed Rust binding accessors elsewhere in the crate.

## ADR sweep (same PR)

Sweep residual "stake"/"staking" wording in **ADR 026, 028, 036** to "bond"/"bonding" where it refers to the operator-collateral pool (chiefly slash-arithmetic mentions; most prose already says "bond"). Keep "appeal bond"/"challenge bond" usage as-is. Rationale: per workspace CLAUDE.md the ADRs are source of truth — landing the wording with the code avoids a window where SoT and ABI disagree.

## Testing & verification

- Foundry: `forge fmt --check`, `FOUNDRY_PROFILE=ci forge build --sizes --deny warnings`, `forge test` (rename-only — all existing assertions must still pass against the renamed surface).
- Static analysis: `aderyn` (fail-on high), `slither` (fail-on medium) — rename should be neutral.
- Rust: `cargo build && cargo clippy`, `cargo nextest run -p decdn-incentive` (and `-p decdn-node` for the DHT tests, to confirm the out-of-scope `StakerSet` mock still compiles untouched).
- `--sizes` gate confirms the rename did not grow `CapacityBond` bytecode.

## Risks

- **Selector churn:** event/error/function selectors change. Acceptable pre-mainnet; no deployed integrators. Any deploy scripts / off-chain indexers referencing old selectors or event names must be updated (audit `contracts/script/` and any indexer config).
- **Missed occurrence:** a stray `stake` reference left behind reintroduces the inconsistency. Mitigation: post-rename `grep -riE "stak(e|ing|ed)"` over `contracts/src/CapacityBond.sol`, `contracts/src/BondMath.sol`, and `crates/incentive/` should return only intentional matches (none expected on the collateral path).
